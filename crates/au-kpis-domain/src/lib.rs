//! Pure domain types for australian-kpis (no async/tokio).
//!
//! This crate is the single source of truth for the SDMX-inspired data model
//! (see `Spec.md § Data model`). Everything public here derives `Serialize`,
//! `Deserialize`, and `utoipa::ToSchema` so it flows unchanged from the DB
//! layer through the HTTP API into the generated TypeScript SDK.
//!
//! No async, no I/O — depend on this crate freely.

#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]

pub mod artifact;
pub mod codelist;
pub mod dataflow;
pub mod dimension;
pub mod ids;
pub mod measure;
pub mod observation;
pub mod series;
pub mod source;

pub use artifact::{Artifact, ContentType};
pub use codelist::{Code, Codelist};
pub use dataflow::{Dataflow, Frequency, License};
pub use dimension::Dimension;
pub use ids::{
    ArtifactId, CodeId, CodelistId, DataflowId, DimensionId, IdError, MeasureId, ObservationId,
    SHA256_BYTES, SeriesKey, Sha256Digest, SourceId,
};
pub use measure::Measure;
pub use observation::{Observation, ObservationStatus, TimePrecision};
pub use series::{Series, SeriesDescriptor};
pub use source::Source;
