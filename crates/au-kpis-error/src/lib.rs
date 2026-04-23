//! Shared error primitives for australian-kpis library crates.
//!
//! # What this crate exposes
//!
//! * [`ErrorClass`] — how callers should react (retry, DLQ, return 400).
//! * [`Classify`] — trait implemented by error types that want to broadcast
//!   their class without forcing callers to pattern-match every variant.
//! * [`CoreError`] — a compact enum of cross-cutting error variants (I/O,
//!   JSON, UTF-8, validation, not-found) that several library crates
//!   compose via `#[from]`.
//!
//! Library crates are expected to define their **own** `thiserror` enums and
//! use `#[from]` to absorb `CoreError` (or its sub-variants directly). See
//! `Spec.md § Error handling across crates` — `anyhow::Error` never appears
//! in public library APIs.
//!
//! # Example
//!
//! ```
//! use au_kpis_error::{Classify, CoreError, ErrorClass};
//!
//! #[derive(Debug, thiserror::Error)]
//! enum MyError {
//!     #[error(transparent)]
//!     Core(#[from] CoreError),
//!     #[error("downstream service unavailable")]
//!     Downstream,
//! }
//!
//! impl Classify for MyError {
//!     fn class(&self) -> ErrorClass {
//!         match self {
//!             MyError::Core(e) => e.class(),
//!             MyError::Downstream => ErrorClass::Transient,
//!         }
//!     }
//! }
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::time::Duration;

use thiserror::Error;

/// Classification of an error used by retry-aware callers.
///
/// The ingestion queue uses this to decide between retry-with-backoff and
/// immediate DLQ; the HTTP layer uses it to map internal errors to the right
/// status code (5xx vs 4xx vs 422).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    /// Transient — retry with backoff is safe and expected to eventually succeed
    /// (network blip, upstream 5xx, DB lock contention).
    Transient,
    /// Permanent — retrying will never succeed. Push to DLQ / surface to the
    /// caller (schema drift, missing artifact, bad credentials).
    Permanent,
    /// Validation — input failed a precondition. Callers render these as
    /// user-visible 400/422 responses without alerting on-call.
    Validation,
}

impl ErrorClass {
    /// `true` when the queue should retry this job with backoff.
    pub const fn is_retryable(self) -> bool {
        matches!(self, ErrorClass::Transient)
    }
}

/// Implemented by error types that can report their retry classification.
///
/// Keep the blanket implementation on the crate boundary — don't add it to
/// foreign types (orphan rule aside, it prevents cross-crate policy drift).
pub trait Classify {
    /// Return the retry class of this error.
    fn class(&self) -> ErrorClass;

    /// Suggested delay before retry. Only meaningful for [`ErrorClass::Transient`];
    /// the default returns `None` so callers fall back to the queue's backoff
    /// policy. Override when the underlying error carries an explicit hint
    /// (e.g. a `Retry-After` header, a `429` from an upstream API).
    fn retry_after(&self) -> Option<Duration> {
        None
    }
}

/// Cross-cutting error variants shared by multiple library crates.
///
/// Library crates that need richer errors should define their own
/// `thiserror` enum and embed `CoreError` as a source via `#[from]`.
#[derive(Debug, Error)]
pub enum CoreError {
    /// Generic I/O failure.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialisation or deserialisation failure.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    /// UTF-8 decoding failure over a borrowed byte slice.
    #[error("invalid utf-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    /// UTF-8 decoding failure over an owned `Vec<u8>`.
    #[error("invalid utf-8: {0}")]
    FromUtf8(#[from] std::string::FromUtf8Error),

    /// Precondition failed on user input (400/422 material).
    #[error("validation: {0}")]
    Validation(String),

    /// Target resource not found.
    #[error("not found: {0}")]
    NotFound(String),
}

impl Classify for CoreError {
    fn class(&self) -> ErrorClass {
        match self {
            // I/O is usually transient (disk, socket, temp network); callers
            // that know otherwise wrap it in their own Permanent variant.
            CoreError::Io(_) => ErrorClass::Transient,
            // Parse/decoding errors are permanent — the artifact won't decode
            // differently on the next attempt.
            CoreError::Json(_) | CoreError::Utf8(_) | CoreError::FromUtf8(_) => {
                ErrorClass::Permanent
            }
            CoreError::Validation(_) => ErrorClass::Validation,
            CoreError::NotFound(_) => ErrorClass::Permanent,
        }
    }
}

/// Convenience: result alias using [`CoreError`].
pub type CoreResult<T> = std::result::Result<T, CoreError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_class_retryable_flag() {
        assert!(ErrorClass::Transient.is_retryable());
        assert!(!ErrorClass::Permanent.is_retryable());
        assert!(!ErrorClass::Validation.is_retryable());
    }

    #[test]
    fn core_error_io_is_transient() {
        let io: std::io::Error = std::io::Error::other("unreachable");
        let err: CoreError = io.into();
        assert_eq!(err.class(), ErrorClass::Transient);
    }

    #[test]
    fn core_error_json_is_permanent() {
        let json_err = serde_json::from_str::<i32>("not-a-number").unwrap_err();
        let err: CoreError = json_err.into();
        assert_eq!(err.class(), ErrorClass::Permanent);
    }

    #[test]
    fn core_error_validation_class() {
        let err = CoreError::Validation("missing field `name`".into());
        assert_eq!(err.class(), ErrorClass::Validation);
    }

    #[test]
    fn downstream_enum_can_compose_via_from() {
        #[derive(Debug, Error)]
        enum Wrapped {
            #[error(transparent)]
            Core(#[from] CoreError),
            #[error("gone")]
            Gone,
        }

        impl Classify for Wrapped {
            fn class(&self) -> ErrorClass {
                match self {
                    Wrapped::Core(c) => c.class(),
                    Wrapped::Gone => ErrorClass::Permanent,
                }
            }
        }

        let io: std::io::Error = std::io::Error::other("flaky");
        let w: Wrapped = CoreError::from(io).into();
        assert_eq!(w.class(), ErrorClass::Transient);
        assert_eq!(Wrapped::Gone.class(), ErrorClass::Permanent);
    }

    #[test]
    fn default_retry_after_is_none() {
        let err = CoreError::Validation("x".into());
        assert!(err.retry_after().is_none());
    }
}
