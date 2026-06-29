//! Shared scheduler library surfaces.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

/// Data-quality rule catalog, report generation, and PagerDuty paging.
pub mod data_quality;
/// Source-location audit rule catalog and report generation.
pub mod source_location_audit;
