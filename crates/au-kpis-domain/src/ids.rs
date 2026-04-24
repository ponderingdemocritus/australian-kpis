//! Newtype identifiers. No raw `String`/`[u8]` IDs leak across public boundaries.
//!
//! Two flavours live here:
//!
//! * String-backed slugs (`SourceId`, `DataflowId`, `DimensionId`, `CodelistId`,
//!   `CodeId`, `MeasureId`) — human-readable, URL-safe, assigned by adapters.
//! * Hash-backed binary IDs (`SeriesKey`, `ArtifactId`) — 32-byte SHA-256 digests
//!   serialised as lowercase hex.
//!
//! `ObservationId` is a composite `(series_key, time, revision_no)` triple — the
//! natural primary key of the `observations` hypertable (see `Spec.md §
//! Database schema`).

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

/// Errors produced while constructing an identifier.
#[derive(Debug, thiserror::Error, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum IdError {
    #[error("identifier must not be empty")]
    Empty,
    #[error("identifier exceeds {max} bytes")]
    TooLong { max: usize },
    #[error("hash must be {expected} hex characters, got {actual}")]
    HashLength { expected: usize, actual: usize },
    #[error("hash contains non-hex characters")]
    HashEncoding,
}

/// Maximum length (in bytes) for slug-style identifiers. Matches the DB column
/// width chosen in migration 0001.
const MAX_ID_LEN: usize = 128;

/// Length in bytes of a SHA-256 digest.
pub const SHA256_BYTES: usize = 32;

fn validate_slug(s: &str) -> Result<(), IdError> {
    if s.is_empty() {
        return Err(IdError::Empty);
    }
    if s.len() > MAX_ID_LEN {
        return Err(IdError::TooLong { max: MAX_ID_LEN });
    }
    Ok(())
}

macro_rules! slug_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, ToSchema)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Construct a new identifier, validating non-empty + length bounds.
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let v = value.into();
                validate_slug(&v)?;
                Ok(Self(v))
            }

            /// Borrow the inner string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consume the newtype and return the inner `String`.
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct SlugIdVisitor;

                impl<'de> serde::de::Visitor<'de> for SlugIdVisitor {
                    type Value = $name;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("a non-empty identifier string")
                    }

                    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                        $name::new(value).map_err(E::custom)
                    }

                    fn visit_string<E: serde::de::Error>(
                        self,
                        value: String,
                    ) -> Result<Self::Value, E> {
                        $name::new(value).map_err(E::custom)
                    }
                }

                deserializer.deserialize_string(SlugIdVisitor)
            }
        }
    };
}

slug_id!(
    /// Identifier for an upstream data source (e.g. `abs`, `rba`, `apra`).
    SourceId
);
slug_id!(
    /// Identifier for a dataflow (e.g. `abs.cpi`).
    DataflowId
);
slug_id!(
    /// Identifier for a dimension within a dataflow (e.g. `region`).
    DimensionId
);
slug_id!(
    /// Identifier for a codelist (e.g. `CL_STATE_AU`).
    CodelistId
);
slug_id!(
    /// Identifier for a code within a codelist (e.g. `VIC`).
    CodeId
);
slug_id!(
    /// Identifier for a measure (e.g. `unemployment_rate`).
    MeasureId
);

/// 32-byte SHA-256 digest. Serialises as lowercase hex.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Sha256Digest([u8; SHA256_BYTES]);

impl Sha256Digest {
    pub const fn from_bytes(bytes: [u8; SHA256_BYTES]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; SHA256_BYTES] {
        &self.0
    }

    /// Hash arbitrary bytes into a digest.
    pub fn hash(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Self(hasher.finalize().into())
    }

    /// Lowercase hex representation (64 chars).
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse from a 64-char lowercase-or-mixed-case hex string.
    pub fn from_hex(s: &str) -> Result<Self, IdError> {
        if s.len() != SHA256_BYTES * 2 {
            return Err(IdError::HashLength {
                expected: SHA256_BYTES * 2,
                actual: s.len(),
            });
        }
        let mut out = [0u8; SHA256_BYTES];
        hex::decode_to_slice(s, &mut out).map_err(|_| IdError::HashEncoding)?;
        Ok(Self(out))
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl FromStr for Sha256Digest {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        Self::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

impl utoipa::PartialSchema for Sha256Digest {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{ObjectBuilder, Type};
        ObjectBuilder::new()
            .schema_type(Type::String)
            .format(Some(utoipa::openapi::SchemaFormat::Custom(
                "sha256-hex".to_string(),
            )))
            .description(Some("Lowercase hex-encoded SHA-256 digest (64 chars)."))
            .min_length(Some(64))
            .max_length(Some(64))
            .into()
    }
}

impl utoipa::ToSchema for Sha256Digest {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Sha256Digest")
    }
}

/// Deterministic series key: `sha256(dataflow_id || '\0' || k1=v1 || '\0' || ...)`
/// over the dataflow id followed by dimensions sorted by key. Newtype guarantees
/// callers cannot silently feed a raw hash where a series key is expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SeriesKey(Sha256Digest);

impl SeriesKey {
    pub const fn from_digest(digest: Sha256Digest) -> Self {
        Self(digest)
    }

    pub fn digest(&self) -> &Sha256Digest {
        &self.0
    }

    /// Canonical derivation: `dataflow_id` then each `(key, value)` sorted by
    /// key, null-separated. Used by adapters and the loader; both must agree
    /// byte-for-byte or the series table fragments.
    pub fn derive<'a, I>(dataflow: &DataflowId, dimensions: I) -> Self
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut pairs: Vec<(&str, &str)> = dimensions.into_iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));

        let mut hasher = Sha256::new();
        hasher.update(dataflow.as_str().as_bytes());
        for (k, v) in pairs {
            hasher.update(b"\0");
            hasher.update(k.as_bytes());
            hasher.update(b"=");
            hasher.update(v.as_bytes());
        }
        Self(Sha256Digest(hasher.finalize().into()))
    }

    pub fn to_hex(&self) -> String {
        self.0.to_hex()
    }

    pub fn from_hex(s: &str) -> Result<Self, IdError> {
        Sha256Digest::from_hex(s).map(Self)
    }
}

impl fmt::Display for SeriesKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for SeriesKey {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl utoipa::PartialSchema for SeriesKey {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        Sha256Digest::schema()
    }
}

impl utoipa::ToSchema for SeriesKey {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("SeriesKey")
    }
}

/// Content-addressed identifier for a raw upstream artifact. SHA-256 of the
/// byte contents, as persisted in R2 under `artifacts/<hex>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactId(Sha256Digest);

impl ArtifactId {
    pub const fn from_digest(digest: Sha256Digest) -> Self {
        Self(digest)
    }

    pub fn of_content(bytes: &[u8]) -> Self {
        Self(Sha256Digest::hash(bytes))
    }

    pub fn digest(&self) -> &Sha256Digest {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        self.0.to_hex()
    }

    pub fn from_hex(s: &str) -> Result<Self, IdError> {
        Sha256Digest::from_hex(s).map(Self)
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for ArtifactId {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl utoipa::PartialSchema for ArtifactId {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        Sha256Digest::schema()
    }
}

impl utoipa::ToSchema for ArtifactId {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("ArtifactId")
    }
}

/// Composite natural key for an observation: `(series_key, time, revision_no)`.
/// Mirrors the primary key of the `observations` hypertable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct ObservationId {
    pub series_key: SeriesKey,
    pub time: DateTime<Utc>,
    pub revision_no: u32,
}

impl ObservationId {
    pub const fn new(series_key: SeriesKey, time: DateTime<Utc>, revision_no: u32) -> Self {
        Self {
            series_key,
            time,
            revision_no,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::{PartialSchema, ToSchema};

    #[test]
    fn slug_id_rejects_empty() {
        assert_eq!(SourceId::new(""), Err(IdError::Empty));
    }

    #[test]
    fn slug_id_rejects_oversized() {
        let too_long = "x".repeat(MAX_ID_LEN + 1);
        assert_eq!(
            SourceId::new(too_long),
            Err(IdError::TooLong { max: MAX_ID_LEN })
        );
    }

    #[test]
    fn slug_id_roundtrips_as_transparent_string() {
        let id = DataflowId::new("abs.cpi").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"abs.cpi\"");
        let back: DataflowId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn sha256_digest_roundtrips() {
        let d = Sha256Digest::hash(b"hello");
        let hex = d.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(Sha256Digest::from_hex(&hex).unwrap(), d);
    }

    #[test]
    fn series_key_is_deterministic_and_order_independent() {
        let df = DataflowId::new("abs.cpi").unwrap();
        let a = SeriesKey::derive(&df, [("region", "AUS"), ("measure", "index")]);
        let b = SeriesKey::derive(&df, [("measure", "index"), ("region", "AUS")]);
        assert_eq!(a, b);

        let different = SeriesKey::derive(&df, [("region", "VIC"), ("measure", "index")]);
        assert_ne!(a, different);
    }

    #[test]
    fn series_key_serialises_as_transparent_hex() {
        let df = DataflowId::new("abs.cpi").unwrap();
        let key = SeriesKey::derive(&df, [("region", "AUS")]);
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, format!("\"{}\"", key.to_hex()));
        let back: SeriesKey = serde_json::from_str(&json).unwrap();
        assert_eq!(key, back);
    }

    #[test]
    fn artifact_id_of_content_is_stable() {
        let a = ArtifactId::of_content(b"payload");
        let b = ArtifactId::of_content(b"payload");
        assert_eq!(a, b);
        assert_ne!(a, ArtifactId::of_content(b"other"));
    }

    #[test]
    fn artifact_id_roundtrips_and_has_sha256_openapi_shape() {
        let id = ArtifactId::of_content(b"payload");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{}\"", id.to_hex()));
        let back: ArtifactId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);

        let schema = serde_json::to_value(ArtifactId::schema()).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], 64);
        assert_eq!(schema["maxLength"], 64);
    }

    #[test]
    fn observation_id_roundtrips() {
        let df = DataflowId::new("abs.cpi").unwrap();
        let oid = ObservationId::new(
            SeriesKey::derive(&df, [("region", "AUS")]),
            DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            0,
        );
        let json = serde_json::to_string(&oid).unwrap();
        let back: ObservationId = serde_json::from_str(&json).unwrap();
        assert_eq!(oid, back);
    }

    #[test]
    fn sha256_digest_rejects_wrong_length() {
        assert!(matches!(
            Sha256Digest::from_hex("deadbeef"),
            Err(IdError::HashLength { .. })
        ));
    }

    #[test]
    fn sha256_digest_rejects_non_hex() {
        let s = "z".repeat(64);
        assert_eq!(Sha256Digest::from_hex(&s), Err(IdError::HashEncoding));
    }

    #[test]
    fn slug_id_json_deserialization_validates_input() {
        let err = serde_json::from_str::<DimensionId>("\"\"").unwrap_err();
        assert!(
            err.to_string().contains("identifier must not be empty"),
            "unexpected error: {err}"
        );

        let too_long = "x".repeat(MAX_ID_LEN + 1);
        let err = serde_json::from_str::<CodeId>(&format!("\"{too_long}\"")).unwrap_err();
        assert!(
            err.to_string().contains("identifier exceeds"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn id_error_roundtrips_and_has_openapi_schema() {
        let err = IdError::TooLong { max: MAX_ID_LEN };
        let json = serde_json::to_string(&err).unwrap();
        let back: IdError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, back);
        assert_eq!(IdError::name(), "IdError");
    }
}
