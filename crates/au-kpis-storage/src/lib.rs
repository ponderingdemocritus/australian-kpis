//! Blob storage abstraction over `object_store`.
//!
//! In production we back this with Cloudflare R2 via the S3-compatible
//! API (`object_store::aws::AmazonS3Builder`); locally and in integration
//! tests we point the same builder at a MinIO container. The public
//! surface hides the backend — callers see a [`BlobStore`] and an
//! opaque, content-addressed [`Sha256Key`].
//!
//! # Content-addressed writes
//!
//! [`BlobStore::put_artifact`] hashes the payload with SHA-256 and
//! writes it under a deterministic key (`artifacts/<hex-digest>`). A
//! second call with identical content returns the same [`Sha256Key`]
//! **and does not re-write the backend**, satisfying the idempotency
//! contract in `Spec.md § Ingestion pipeline` (fetch is the one step
//! we re-run on every retry).
//!
//! # Streaming reads
//!
//! [`BlobStore::get_artifact`] returns a [`ByteStream`] so large
//! artifacts (multi-GB ABS dumps) never need to be buffered in memory.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{fmt, sync::Arc};

use au_kpis_error::{Classify, CoreError, ErrorClass};
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use object_store::{Error as ObjectStoreError, ObjectStore, path::Path as ObjectPath};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Prefix applied to every content-addressed key.
///
/// The ingestion spec (`Spec.md § Database schema — artifacts`) places
/// raw source files under this namespace; keeping it as a constant means
/// loader queries and storage writes agree by construction.
pub const ARTIFACTS_PREFIX: &str = "artifacts";

/// Content-addressed key for an artifact.
///
/// Wraps the lower-case hex encoding of the payload's SHA-256 digest.
/// The underlying string is always 64 characters long and matches
/// `[0-9a-f]{64}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Sha256Key(String);

impl Sha256Key {
    /// Compute the sha256 of `content` and wrap it as a key.
    #[must_use]
    pub fn from_content(content: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(content);
        Self(hex_encode(hasher.finalize().as_slice()))
    }

    /// Lower-case hex encoding of the digest (no prefix).
    #[must_use]
    pub fn as_hex(&self) -> &str {
        &self.0
    }

    /// Full object-store path, including the `artifacts/` prefix.
    #[must_use]
    pub fn to_object_path(&self) -> ObjectPath {
        ObjectPath::from(format!("{ARTIFACTS_PREFIX}/{}", self.0))
    }
}

impl fmt::Display for Sha256Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Errors returned by the storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Shared I/O, serde, or validation failure.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// Upstream object-store backend returned an unexpected failure.
    #[error("object store backend: {0}")]
    Backend(#[source] ObjectStoreError),

    /// The requested key does not exist in the backend.
    #[error("object not found: {0}")]
    NotFound(String),
}

impl Classify for StorageError {
    fn class(&self) -> ErrorClass {
        match self {
            StorageError::Core(e) => e.class(),
            // Network blips, 5xx, or transient rate-limits from the
            // backend all land here; retry is the right default.
            StorageError::Backend(_) => ErrorClass::Transient,
            // Content-addressed keys don't "appear later" — a NotFound
            // means the producer never wrote the artifact, so retrying
            // the fetch will keep failing in the same way.
            StorageError::NotFound(_) => ErrorClass::Permanent,
        }
    }
}

impl StorageError {
    fn from_object_store(err: ObjectStoreError) -> Self {
        match err {
            ObjectStoreError::NotFound { path, .. } => StorageError::NotFound(path),
            other => StorageError::Backend(other),
        }
    }
}

/// Streamed body returned by [`BlobStore::get_artifact`].
pub type ByteStream = BoxStream<'static, Result<Bytes, StorageError>>;

/// Content-addressed blob store.
#[derive(Clone)]
pub struct BlobStore {
    inner: Arc<dyn ObjectStore>,
}

impl fmt::Debug for BlobStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobStore").finish_non_exhaustive()
    }
}

impl BlobStore {
    /// Wrap an `ObjectStore` implementation. Key layout is owned by this
    /// crate, so the passed-in store should be scoped to a bucket/root
    /// without an extra prefix.
    #[must_use]
    pub fn new<S: ObjectStore + 'static>(inner: S) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Wrap a pre-shared `Arc<dyn ObjectStore>`. Useful in tests that
    /// also want to inspect the backend directly (e.g. comparing
    /// `ObjectMeta` across `put_artifact` calls).
    #[must_use]
    pub fn from_arc(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    /// Write `content` under its content-addressed key.
    ///
    /// If a blob with the same hash is already present the call is a
    /// no-op: the backend is **not** re-written. Either way the caller
    /// receives the canonical [`Sha256Key`].
    #[tracing::instrument(skip(self, content), fields(len = content.len()))]
    pub async fn put_artifact(&self, content: Bytes) -> Result<Sha256Key, StorageError> {
        let key = Sha256Key::from_content(&content);
        let path = key.to_object_path();

        // Content-addressed keys collapse "same content" to "same key",
        // so a successful head means the bytes are already present and
        // a fresh write would just be a duplicate PUT. Skipping it
        // delivers the "no rewrite" half of the idempotency contract
        // and stays portable across S3/R2/MinIO — conditional writes
        // (`PutMode::Create`) aren't universally supported.
        match self.inner.head(&path).await {
            Ok(_) => return Ok(key),
            Err(ObjectStoreError::NotFound { .. }) => {}
            Err(err) => return Err(StorageError::from_object_store(err)),
        }

        self.inner
            .put(&path, content.into())
            .await
            .map_err(StorageError::from_object_store)?;
        Ok(key)
    }

    /// Return a streaming reader of the artifact at `key`.
    ///
    /// Yields chunks of [`Bytes`] as the backend delivers them; no
    /// in-memory buffer of the full body is materialised.
    #[tracing::instrument(skip(self))]
    pub async fn get_artifact(&self, key: &Sha256Key) -> Result<ByteStream, StorageError> {
        let path = key.to_object_path();
        let result = self
            .inner
            .get(&path)
            .await
            .map_err(StorageError::from_object_store)?;
        Ok(result
            .into_stream()
            .map(|chunk| chunk.map_err(StorageError::from_object_store))
            .boxed())
    }

    /// Return `true` when the artifact at `key` is present.
    #[tracing::instrument(skip(self))]
    pub async fn contains(&self, key: &Sha256Key) -> Result<bool, StorageError> {
        match self.inner.head(&key.to_object_path()).await {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::NotFound { .. }) => Ok(false),
            Err(err) => Err(StorageError::Backend(err)),
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_key_matches_known_vector() {
        // SHA-256 of the empty string is a fixed reference value; if
        // the hashing glue ever regresses (wrong encoding, double-hash,
        // truncation) this test is the first to notice.
        let key = Sha256Key::from_content(b"");
        assert_eq!(
            key.as_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            key.to_object_path().as_ref(),
            "artifacts/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_key_is_deterministic() {
        let a = Sha256Key::from_content(b"hello world");
        let b = Sha256Key::from_content(b"hello world");
        assert_eq!(a, b);
        assert_eq!(a.as_hex().len(), 64);
        assert!(a.as_hex().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn distinct_content_yields_distinct_keys() {
        let a = Sha256Key::from_content(b"hello");
        let b = Sha256Key::from_content(b"hell0");
        assert_ne!(a, b);
    }

    #[test]
    fn hex_encode_round_trips_known_bytes() {
        assert_eq!(hex_encode(&[0x00, 0x0f, 0xff]), "000fff");
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn core_error_flows_through_from() {
        let io: std::io::Error = std::io::Error::other("down");
        let err: StorageError = CoreError::from(io).into();
        assert_eq!(err.class(), ErrorClass::Transient);
    }

    #[test]
    fn backend_is_transient() {
        let err = StorageError::Backend(ObjectStoreError::Generic {
            store: "test",
            source: "boom".into(),
        });
        assert_eq!(err.class(), ErrorClass::Transient);
    }

    #[test]
    fn not_found_is_permanent() {
        assert_eq!(
            StorageError::NotFound("artifacts/abc".into()).class(),
            ErrorClass::Permanent,
        );
    }

    #[test]
    fn from_object_store_maps_not_found() {
        let raw = ObjectStoreError::NotFound {
            path: "artifacts/abc".into(),
            source: "missing".into(),
        };
        let mapped = StorageError::from_object_store(raw);
        match mapped {
            StorageError::NotFound(p) => assert_eq!(p, "artifacts/abc"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }
}
