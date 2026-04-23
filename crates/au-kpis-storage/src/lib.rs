//! object_store wrapper for S3/R2.
//!
//! Only the error type is fleshed out at this stage — it demonstrates the
//! composition pattern from `au-kpis-error` that all library crates follow:
//! cross-cutting variants come in via `#[from] CoreError`, backend-specific
//! variants are added as explicit members, and `Classify` bubbles retry
//! policy up to the queue.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use au_kpis_error::{Classify, CoreError, ErrorClass};
use thiserror::Error;

/// Errors returned by the storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Shared I/O, serde, or validation failure.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// Upstream object-store backend returned an unexpected failure.
    #[error("object store backend: {0}")]
    Backend(String),

    /// The requested key does not exist in the backend.
    #[error("object not found: {0}")]
    NotFound(String),
}

impl Classify for StorageError {
    fn class(&self) -> ErrorClass {
        match self {
            StorageError::Core(e) => e.class(),
            StorageError::Backend(_) => ErrorClass::Transient,
            StorageError::NotFound(_) => ErrorClass::Permanent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_error_flows_through_from() {
        let io: std::io::Error = std::io::Error::other("down");
        let err: StorageError = CoreError::from(io).into();
        assert_eq!(err.class(), ErrorClass::Transient);
    }

    #[test]
    fn backend_is_transient() {
        assert_eq!(
            StorageError::Backend("timeout".into()).class(),
            ErrorClass::Transient
        );
    }

    #[test]
    fn not_found_is_permanent() {
        assert_eq!(
            StorageError::NotFound("artifacts/abc".into()).class(),
            ErrorClass::Permanent
        );
    }
}
