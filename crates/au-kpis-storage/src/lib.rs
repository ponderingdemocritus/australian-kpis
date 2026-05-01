//! Blob storage abstraction over `object_store`.
//!
//! In production this is backed by Cloudflare R2 via the S3-compatible
//! API (`object_store::aws::AmazonS3Builder`); locally and in integration
//! tests we point the same builder at a MinIO container. The public
//! surface hides the backend — callers see a [`BlobStore`], a
//! [`StorageKey`] read handle, and [`au_kpis_domain::ids::ArtifactId`]
//! for content-addressed writes.
//!
//! # Content-addressed writes
//!
//! Two write paths share the same canonical key layout
//! (`artifacts/<hex-digest>`) and return the same [`ArtifactId`]:
//!
//! * [`BlobStore::put_artifact`] — takes a `Bytes` value already in
//!   memory. Cheapest path for small/medium artifacts that a caller
//!   has already buffered.
//! * [`BlobStore::put_artifact_stream`] — takes a `Stream<Item =
//!   Result<Bytes, _>>`, hashes each chunk as it flows, and uploads
//!   via `put_multipart`. **No in-memory buffer of the full body is
//!   materialised**, which is what the adapter fetch stage needs for
//!   multi-GB upstream files (`Spec.md § Ingestion pipeline`).
//!
//! Both paths are idempotent: a second call with identical content
//! returns the same [`ArtifactId`] and does not re-write the backend.
//!
//! # Reads go via the persisted `storage_key`
//!
//! [`BlobStore::get`] / [`BlobStore::exists`] take a [`StorageKey`]
//! rather than an [`ArtifactId`]. `Artifact.storage_key` is persisted
//! explicitly in the schema so retention policies can move an artifact
//! to cold tier (rewriting the key) without changing its id; deriving
//! the path from the id would 404 after any such move. Callers that
//! just wrote (or never persist the key) can reconstruct the default
//! with [`StorageKey::canonical_for`].
//!
//! # Staging
//!
//! Streaming writes land first in `artifacts-staging/<uuid>` and are
//! server-side copied to the canonical key once the hash is known. The
//! staging delete on the happy path is retried with backoff before
//! being logged — see [`STAGING_PREFIX`] — so the deployment **must**
//! configure a bucket lifecycle rule that sweeps this prefix (7d is
//! generous) to catch the rare case where delete fails for longer
//! than the retry window can cover.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{fmt, sync::Arc, time::Duration};

use au_kpis_domain::ids::{ArtifactId, Sha256Digest};
use au_kpis_error::{Classify, CoreError, ErrorClass};
use bytes::Bytes;
use futures::{Stream, StreamExt, stream::BoxStream};
use object_store::{
    Error as ObjectStoreError, ObjectStore, WriteMultipart, path::Path as ObjectPath,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Prefix applied to every canonical content-addressed key.
///
/// The ingestion spec (`Spec.md § Database schema — artifacts`) places
/// raw source files under this namespace; keeping it as a constant means
/// loader queries and storage writes agree by construction.
pub const ARTIFACTS_PREFIX: &str = "artifacts";

/// Prefix used for in-flight streaming uploads.
///
/// A streaming `put` cannot know the sha256 until the last byte has
/// been hashed, so it writes to `artifacts-staging/<uuid>` first, then
/// server-side copies to `artifacts/<hex>`. A dead staging object is
/// inert — a bucket lifecycle rule on this prefix can sweep it later.
pub const STAGING_PREFIX: &str = "artifacts-staging";

/// Location at which a blob lives inside the backend.
///
/// Two constructors cover both call sites: callers that have just
/// written go through [`StorageKey::canonical_for`], callers reading
/// back from a persisted row use [`StorageKey::from_persisted`]. Both
/// produce the same shape — opaque string — but the split keeps the
/// intent visible at every call site.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StorageKey(String);

impl StorageKey {
    /// The default `artifacts/<hex>` placement written by this crate.
    #[must_use]
    pub fn canonical_for(id: &ArtifactId) -> Self {
        Self(format!("{ARTIFACTS_PREFIX}/{}", id.to_hex()))
    }

    /// Wrap a key that was previously persisted (e.g. `artifacts.storage_key`
    /// loaded from Postgres). The crate stores `storage_key` explicitly
    /// so retention moves can rewrite it, and reads must follow the
    /// persisted value rather than rederiving it from the digest.
    #[must_use]
    pub fn from_persisted(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// Underlying string representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn as_object_path(&self) -> ObjectPath {
        ObjectPath::from(self.0.as_str())
    }
}

impl fmt::Display for StorageKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for StorageKey {
    fn as_ref(&self) -> &str {
        &self.0
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

    /// A caller-supplied stream yielded an error mid-upload.
    ///
    /// Stored as a string so the variant stays `Send + Sync + 'static`
    /// and round-trips through `thiserror` cleanly; the original error
    /// is displayed via `{}` before being captured.
    #[error("source stream: {0}")]
    Source(String),
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
            // A failed upstream stream usually means the adapter's
            // fetch hit a transport error partway through; let the
            // queue retry with backoff.
            StorageError::Source(_) => ErrorClass::Transient,
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

/// A completed streaming upload that is still parked under the staging prefix.
#[derive(Debug)]
pub struct StagedArtifact {
    id: ArtifactId,
    size_bytes: u64,
    staging_path: Option<ObjectPath>,
}

impl StagedArtifact {
    /// Content-addressed id computed while streaming to staging.
    #[must_use]
    pub fn id(&self) -> ArtifactId {
        self.id
    }

    /// Number of bytes streamed from the source.
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
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
    /// receives the canonical [`ArtifactId`].
    #[tracing::instrument(skip(self, content), fields(len = content.len()))]
    pub async fn put_artifact(&self, content: Bytes) -> Result<ArtifactId, StorageError> {
        let id = ArtifactId::of_content(&content);
        let path = canonical_object_path(&id);

        // Content-addressed keys collapse "same content" to "same key",
        // so a successful head means the bytes are already present and
        // a fresh write would just be a duplicate PUT. Skipping it
        // delivers the "no rewrite" half of the idempotency contract
        // and stays portable across S3/R2/MinIO — conditional writes
        // (`PutMode::Create`) aren't universally supported.
        match self.inner.head(&path).await {
            Ok(_) => return Ok(id),
            Err(ObjectStoreError::NotFound { .. }) => {}
            Err(err) => return Err(StorageError::from_object_store(err)),
        }

        self.inner
            .put(&path, content.into())
            .await
            .map_err(StorageError::from_object_store)?;
        Ok(id)
    }

    /// Stream `chunks` to backend staging while hashing on the fly.
    ///
    /// Writes are performed via `put_multipart` to a staging key so the
    /// backend never has to see the whole body at once. Callers that
    /// need to consult durable provenance before creating a canonical
    /// hot copy can inspect the returned id and then either
    /// [`commit_staged_artifact`](Self::commit_staged_artifact) or
    /// [`discard_staged_artifact`](Self::discard_staged_artifact).
    #[tracing::instrument(skip(self, chunks))]
    pub async fn stage_artifact_stream<S, E>(
        &self,
        mut chunks: S,
    ) -> Result<StagedArtifact, StorageError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin + Send,
        E: fmt::Display,
    {
        let staging_path = ObjectPath::from(format!("{STAGING_PREFIX}/{}", uuid::Uuid::new_v4()));

        let upload = self
            .inner
            .put_multipart(&staging_path)
            .await
            .map_err(StorageError::from_object_store)?;
        // `WriteMultipart` batches small input chunks into 5 MB parts
        // before dispatching them — S3/R2/MinIO reject intermediate
        // parts below 5 MB (`EntityTooSmall`), so a naive per-chunk
        // `put_part` breaks the moment a caller streams ~KB-sized
        // reads from a slow HTTP response.
        let mut write = WriteMultipart::new(upload);
        let mut hasher = Sha256::new();
        let mut wrote_any = false;
        let mut size_bytes = 0_u64;

        while let Some(next) = chunks.next().await {
            let chunk = match next {
                Ok(chunk) => chunk,
                Err(err) => {
                    // Best-effort abort; the staging key is disposable
                    // either way. We swallow abort errors so the
                    // original source-stream failure reaches the caller
                    // instead of being masked.
                    let _ = write.abort().await;
                    return Err(StorageError::Source(err.to_string()));
                }
            };
            if chunk.is_empty() {
                // Empty chunks don't affect the sha256 and would just
                // waste a round-trip through `WriteMultipart::put`;
                // drop them and keep draining the stream.
                continue;
            }
            size_bytes = size_bytes
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| CoreError::Validation("artifact exceeds u64 bytes".into()))?;
            hasher.update(&chunk);
            wrote_any = true;
            // Cap in-flight part uploads so a fast producer can't run
            // the process out of memory; 8 concurrent 5 MB parts ≈ 40
            // MB of buffer, which is a reasonable ceiling for the
            // ingestion binary.
            if let Err(err) = write.wait_for_capacity(8).await {
                let _ = write.abort().await;
                return Err(StorageError::from_object_store(err));
            }
            write.put(chunk);
        }

        // Zero-byte streams are legal upstream payloads (an API that
        // returns an empty body, a 0-byte XLSX) but S3 rejects
        // `CompleteMultipartUpload` with zero parts. Abort the empty
        // multipart and return a stage-less handle that commit can
        // materialise through the single-shot path if needed.
        if !wrote_any {
            let _ = write.abort().await;
            return Ok(StagedArtifact {
                id: ArtifactId::of_content(&[]),
                size_bytes,
                staging_path: None,
            });
        }

        if let Err(err) = write.finish().await {
            // `WriteMultipart::finish` already attempts an abort when
            // `complete()` fails, so we just surface the error.
            return Err(StorageError::from_object_store(err));
        }

        let id = ArtifactId::from_digest(Sha256Digest::from_bytes(hasher.finalize().into()));
        Ok(StagedArtifact {
            id,
            size_bytes,
            staging_path: Some(staging_path),
        })
    }

    /// Commit a staged artifact to its canonical content-addressed key.
    ///
    /// Returns the canonical [`ArtifactId`] regardless of whether the
    /// canonical key already existed — matching [`put_artifact`]'s
    /// idempotency contract. The copy is server-side; bytes do not
    /// travel back through this process.
    #[tracing::instrument(skip(self, staged), fields(id = %staged.id))]
    pub async fn commit_staged_artifact(
        &self,
        staged: &StagedArtifact,
    ) -> Result<ArtifactId, StorageError> {
        let Some(staging_path) = &staged.staging_path else {
            return self.put_artifact(Bytes::new()).await;
        };
        let canonical = canonical_object_path(&staged.id);

        // Canonical key already present → discard the stage; caller
        // still gets the deterministic id.
        match self.inner.head(&canonical).await {
            Ok(_) => {
                self.best_effort_delete_staging(staging_path).await;
                return Ok(staged.id);
            }
            Err(ObjectStoreError::NotFound { .. }) => {}
            Err(err) => {
                self.best_effort_delete_staging(staging_path).await;
                return Err(StorageError::from_object_store(err));
            }
        }

        // Server-side copy; the bytes never travel back through this
        // process. `copy` overwrites on S3/R2/MinIO, which is fine:
        // content-addressed means "same bytes" even if a concurrent
        // writer got there first.
        if let Err(err) = self.inner.copy(staging_path, &canonical).await {
            self.best_effort_delete_staging(staging_path).await;
            return Err(StorageError::from_object_store(err));
        }
        self.best_effort_delete_staging(staging_path).await;
        Ok(staged.id)
    }

    /// Replace the canonical key with a staged artifact.
    ///
    /// This is reserved for repair paths that have already proven the
    /// existing durable object is missing or does not hash to the artifact id.
    #[tracing::instrument(skip(self, staged), fields(id = %staged.id))]
    pub async fn replace_staged_artifact(
        &self,
        staged: &StagedArtifact,
    ) -> Result<ArtifactId, StorageError> {
        let canonical = canonical_object_path(&staged.id);
        let Some(staging_path) = &staged.staging_path else {
            self.inner
                .put(&canonical, Bytes::new().into())
                .await
                .map_err(StorageError::from_object_store)?;
            return Ok(staged.id);
        };

        if let Err(err) = self.inner.copy(staging_path, &canonical).await {
            self.best_effort_delete_staging(staging_path).await;
            return Err(StorageError::from_object_store(err));
        }
        self.best_effort_delete_staging(staging_path).await;
        Ok(staged.id)
    }

    /// Discard a staged artifact without creating a canonical hot copy.
    #[tracing::instrument(skip(self, staged), fields(id = %staged.id))]
    pub async fn discard_staged_artifact(
        &self,
        staged: &StagedArtifact,
    ) -> Result<(), StorageError> {
        if let Some(staging_path) = &staged.staging_path {
            self.best_effort_delete_staging(staging_path).await;
        }
        Ok(())
    }

    /// Stream `chunks` to the backend while hashing on the fly, then
    /// land the content under the canonical `artifacts/<hex>` key.
    ///
    /// Writes are staged first because the sha256 is only known after
    /// the stream drains. Once known, the staged object is server-side
    /// copied to the canonical key and deleted.
    #[tracing::instrument(skip(self, chunks))]
    pub async fn put_artifact_stream<S, E>(&self, chunks: S) -> Result<ArtifactId, StorageError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin + Send,
        E: fmt::Display,
    {
        let staged = self.stage_artifact_stream(chunks).await?;
        self.commit_staged_artifact(&staged).await
    }

    /// Return a streaming reader of the artifact at `key`.
    ///
    /// Takes the persisted [`StorageKey`] rather than an
    /// [`ArtifactId`] so retention-tier moves that rewrite the row's
    /// `storage_key` continue to resolve after the move. Yields chunks
    /// of [`Bytes`] as the backend delivers them; no in-memory buffer
    /// of the full body is materialised.
    #[tracing::instrument(skip(self))]
    pub async fn get(&self, key: &StorageKey) -> Result<ByteStream, StorageError> {
        let result = self
            .inner
            .get(&key.as_object_path())
            .await
            .map_err(StorageError::from_object_store)?;
        Ok(result
            .into_stream()
            .map(|chunk| chunk.map_err(StorageError::from_object_store))
            .boxed())
    }

    /// Return `true` when an object exists at `key`.
    #[tracing::instrument(skip(self))]
    pub async fn exists(&self, key: &StorageKey) -> Result<bool, StorageError> {
        match self.inner.head(&key.as_object_path()).await {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::NotFound { .. }) => Ok(false),
            Err(err) => Err(StorageError::Backend(err)),
        }
    }

    /// Return `true` when an object exists at `key` with the expected size.
    #[tracing::instrument(skip(self))]
    pub async fn exists_with_size(
        &self,
        key: &StorageKey,
        size_bytes: u64,
    ) -> Result<bool, StorageError> {
        match self.inner.head(&key.as_object_path()).await {
            Ok(meta) => {
                Ok(u64::try_from(meta.size).is_ok_and(|actual_size| actual_size == size_bytes))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(false),
            Err(err) => Err(StorageError::Backend(err)),
        }
    }

    /// Return `true` when `key` exists and streams back to `id`.
    #[tracing::instrument(skip(self))]
    pub async fn matches_artifact_id(
        &self,
        key: &StorageKey,
        id: ArtifactId,
    ) -> Result<bool, StorageError> {
        let mut stream = match self.get(key).await {
            Ok(stream) => stream,
            Err(StorageError::NotFound(_)) => return Ok(false),
            Err(err) => return Err(err),
        };
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            hasher.update(&chunk?);
        }
        let actual = ArtifactId::from_digest(Sha256Digest::from_bytes(hasher.finalize().into()));
        Ok(actual == id)
    }

    /// Delete the object at `key`.
    ///
    /// Missing objects are treated as success so orphan cleanup can be retried
    /// safely.
    #[tracing::instrument(skip(self))]
    pub async fn delete(&self, key: &StorageKey) -> Result<(), StorageError> {
        match self.inner.delete(&key.as_object_path()).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(err) => Err(StorageError::from_object_store(err)),
        }
    }

    /// Retrying staging delete.
    ///
    /// A failed staging delete after a successful canonical write is
    /// non-fatal for the caller — the bytes they wanted are in place —
    /// but silently swallowing the error lets staging grow without
    /// bound. Retry a few times with exponential backoff, then log at
    /// `warn` level with the key so the bucket lifecycle rule (see
    /// [`STAGING_PREFIX`] docs) and oncall both have a record. We
    /// never surface this as an error: turning a benign staging leak
    /// into a put failure would be a worse outcome.
    async fn best_effort_delete_staging(&self, path: &ObjectPath) {
        const ATTEMPTS: u32 = 3;
        let mut backoff = Duration::from_millis(100);
        let mut last_err: Option<ObjectStoreError> = None;
        for _ in 0..ATTEMPTS {
            match self.inner.delete(path).await {
                Ok(()) => return,
                // A concurrent cleanup (or a lifecycle sweep) may have
                // already removed the object; nothing to do.
                Err(ObjectStoreError::NotFound { .. }) => return,
                Err(err) => {
                    last_err = Some(err);
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }
        tracing::warn!(
            staging_key = %path,
            error = ?last_err,
            "failed to delete staging object after {ATTEMPTS} retries; \
             the bucket lifecycle rule on `{STAGING_PREFIX}/` will sweep it eventually",
        );
    }
}

/// Internal helper: object-store path for the canonical write target.
/// Kept private so callers cannot accidentally bypass `StorageKey`
/// when reading.
fn canonical_object_path(id: &ArtifactId) -> ObjectPath {
    ObjectPath::from(format!("{ARTIFACTS_PREFIX}/{}", id.to_hex()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_storage_key_matches_known_empty_vector() {
        // SHA-256 of the empty string is a fixed reference value; if
        // the hashing glue ever regresses (wrong encoding, double-hash,
        // truncation) this test is the first to notice.
        let id = ArtifactId::of_content(b"");
        assert_eq!(
            id.to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            StorageKey::canonical_for(&id).as_str(),
            "artifacts/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn storage_key_from_persisted_preserves_arbitrary_path() {
        // The domain model allows retention moves to rewrite
        // `storage_key` to something outside `artifacts/...` — the
        // read API must treat the string as opaque.
        let moved = StorageKey::from_persisted("cold/2024/abc123");
        assert_eq!(moved.as_str(), "cold/2024/abc123");
    }

    #[test]
    fn artifact_id_is_deterministic_and_reversible_via_hex() {
        // The reviewer's P2 on PR #74 was that downstream stages
        // persist the digest and need to reconstruct an id from it.
        // Guard that invariant by round-tripping through hex.
        let a = ArtifactId::of_content(b"hello world");
        let b = ArtifactId::of_content(b"hello world");
        assert_eq!(a, b);

        let restored = ArtifactId::from_hex(&a.to_hex()).expect("hex round-trip");
        assert_eq!(restored, a);
    }

    #[test]
    fn distinct_content_yields_distinct_ids() {
        let a = ArtifactId::of_content(b"hello");
        let b = ArtifactId::of_content(b"hell0");
        assert_ne!(a, b);
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
    fn source_stream_is_transient() {
        assert_eq!(
            StorageError::Source("broken pipe".into()).class(),
            ErrorClass::Transient,
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

    #[tokio::test]
    async fn replace_staged_artifact_overwrites_canonical_mismatch() {
        let store = BlobStore::new(object_store::memory::InMemory::new());
        let id = ArtifactId::of_content(b"correct bytes");
        let key = StorageKey::canonical_for(&id);
        store
            .inner
            .put(&key.as_object_path(), Bytes::from_static(b"corrupt").into())
            .await
            .expect("seed corrupt canonical object");

        let staged = store
            .stage_artifact_stream(futures::stream::iter([Ok::<_, std::io::Error>(
                Bytes::from_static(b"correct bytes"),
            )]))
            .await
            .expect("stage replacement");
        store
            .replace_staged_artifact(&staged)
            .await
            .expect("replace canonical object");

        assert!(
            store
                .matches_artifact_id(&key, id)
                .await
                .expect("hash replacement")
        );
    }

    #[tokio::test]
    async fn exists_with_size_checks_head_metadata_without_streaming() {
        let store = BlobStore::new(object_store::memory::InMemory::new());
        let id = store
            .put_artifact(Bytes::from_static(b"sized payload"))
            .await
            .expect("put artifact");
        let key = StorageKey::canonical_for(&id);

        assert!(
            store
                .exists_with_size(&key, 13)
                .await
                .expect("matching head")
        );
        assert!(
            !store
                .exists_with_size(&key, 12)
                .await
                .expect("mismatching head")
        );
    }
}
