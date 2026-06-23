//! Layered config loader for australian-kpis binaries.
//!
//! Precedence is **defaults → TOML file → environment variables** (later
//! layers override earlier ones). See `Spec.md § Tech stack` — `figment` was
//! chosen for the layered, type-safe model.
//!
//! # Environment variables
//!
//! Variables are read with a fixed prefix ([`ENV_PREFIX`]) and nested field
//! paths joined with [`ENV_NESTED_SEPARATOR`]. For example
//! `AU_KPIS_HTTP__BIND=0.0.0.0:9000` maps to `http.bind`.
//!
//! # Errors
//!
//! A missing required field (notably `database.url`, which has no default)
//! makes [`load`] fail fast with a [`ConfigError::Figment`] whose message
//! names the missing path — callers do not have to wait for a runtime
//! connection attempt to notice the misconfiguration.
//!
//! # Example
//!
//! ```no_run
//! use std::path::Path;
//! use au_kpis_config::{load, AppConfig};
//!
//! let cfg: AppConfig = load(Some(Path::new("au-kpis.toml")))
//!     .expect("config");
//! let _ = cfg.http.bind;
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{collections::BTreeMap, env, path::Path};

use au_kpis_error::{Classify, CoreError, ErrorClass};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

/// Prefix applied to every environment variable consumed by [`load`].
pub const ENV_PREFIX: &str = "AU_KPIS_";

/// Separator between nested fields in environment variables.
///
/// `AU_KPIS_HTTP__BIND` overrides `http.bind`.
pub const ENV_NESTED_SEPARATOR: &str = "__";

/// Errors returned by the config loader.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Shared I/O, serde, or validation failure (see
    /// [`au_kpis_error::CoreError`]).
    #[error(transparent)]
    Core(#[from] CoreError),

    /// `figment` layer failure — missing required field, TOML parse error,
    /// or malformed env var.
    #[error("config load: {0}")]
    Figment(#[from] figment::Error),
}

impl Classify for ConfigError {
    fn class(&self) -> ErrorClass {
        match self {
            ConfigError::Core(e) => e.class(),
            // Config errors surface at startup; retrying the same inputs
            // produces the same failure.
            ConfigError::Figment(_) => ErrorClass::Permanent,
        }
    }
}

/// Top-level application config.
///
/// Sections exist only for fields that already have a downstream consumer;
/// new sections are added alongside the dependent crate. Required fields
/// without a [`Default`] (currently `database.url` and `cache.url`) force [`load`] to fail
/// at startup if unset.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    /// HTTP server configuration — consumed by `au-kpis-api`.
    pub http: HttpConfig,
    /// Database connection configuration — consumed by `au-kpis-db`.
    pub database: DatabaseConfig,
    /// Redis cache configuration — consumed by `au-kpis-cache`.
    pub cache: CacheConfig,
    /// Telemetry configuration — consumed by `au-kpis-telemetry`.
    pub telemetry: TelemetryConfig,
    /// API rate-limit tiers and anonymous request quota.
    pub rate_limits: RateLimitConfig,
}

/// Config consumed by the ingestion worker binary.
///
/// The ingestion worker shares HTTP, database, and telemetry config with the
/// rest of the monorepo, but it does not currently require Redis/cache wiring.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IngestionConfig {
    /// HTTP server configuration for the metrics endpoint.
    pub http: HttpConfig,
    /// Database connection configuration.
    pub database: DatabaseConfig,
    /// Optional cache configuration. Present when a future ingestion feature
    /// needs it, absent for today's one-shot and queue-worker modes.
    pub cache: Option<CacheConfig>,
    /// Telemetry configuration.
    pub telemetry: TelemetryConfig,
}

/// HTTP server configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpConfig {
    /// Socket address the API binary binds to.
    pub bind: String,
    /// Explicit browser origin allowlist for API routes.
    pub cors_allowed_origins: Vec<String>,
    /// Graceful shutdown drain window, in seconds.
    pub shutdown_grace_period_secs: u64,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:3000".into(),
            cors_allowed_origins: Vec::new(),
            shutdown_grace_period_secs: 30,
        }
    }
}

/// Database connection configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatabaseConfig {
    /// Postgres (Timescale) connection URL. Required — no default.
    pub url: String,
}

/// Redis cache configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CacheConfig {
    /// Redis connection URL.
    pub url: String,
}

/// Telemetry and logging configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelemetryConfig {
    /// Service name emitted on traces and logs.
    pub service_name: String,
    /// Log output format.
    pub log_format: LogFormat,
    /// Log filter, interpreted by `tracing-subscriber`'s `EnvFilter`.
    pub log_level: String,
    /// OTLP endpoint for span export. `None` disables OTel export.
    pub otlp_endpoint: Option<String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: "au-kpis".into(),
            log_format: LogFormat::Json,
            log_level: "info".into(),
            otlp_endpoint: None,
        }
    }
}

/// API rate-limit configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RateLimitConfig {
    /// Tier name used when an API key references an unknown tier.
    pub default_tier: String,
    /// Named API-key rate-limit tiers.
    pub tiers: BTreeMap<String, RateLimitTierConfig>,
    /// Anonymous no-key per-IP quota.
    pub anonymous: RateLimitQuotaConfig,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        let free = RateLimitTierConfig {
            per_key: RateLimitQuotaConfig {
                per_second: 60,
                per_hour: 1_000,
                burst_multiplier: 2,
            },
            per_ip: RateLimitQuotaConfig {
                per_second: 10,
                per_hour: 100,
                burst_multiplier: 2,
            },
        };

        Self {
            default_tier: "free".into(),
            tiers: BTreeMap::from([("free".into(), free)]),
            anonymous: RateLimitQuotaConfig {
                per_second: 10,
                per_hour: 100,
                burst_multiplier: 2,
            },
        }
    }
}

/// API-key tier quota, enforced by both key and client IP.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RateLimitTierConfig {
    /// Per-key quota.
    pub per_key: RateLimitQuotaConfig,
    /// Per-IP quota for requests carrying a key in this tier.
    pub per_ip: RateLimitQuotaConfig,
}

/// Token-bucket quota shape.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct RateLimitQuotaConfig {
    /// Refill rate for the one-second bucket.
    pub per_second: u32,
    /// Refill rate for the one-hour bucket.
    pub per_hour: u32,
    /// Bucket capacity multiplier over the refill rate.
    pub burst_multiplier: u32,
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LogFormat {
    /// JSON-per-line, production default.
    Json,
    /// Human-readable, recommended for local dev.
    Pretty,
}

/// Default values serialised as the first figment layer.
///
/// Sections without a default (e.g. `database`) are intentionally omitted
/// so figment surfaces a clear "missing field" error when the caller
/// forgets to supply them.
#[derive(Debug, Clone, Default, Serialize)]
struct Defaults {
    http: HttpConfig,
    telemetry: TelemetryConfig,
    rate_limits: RateLimitConfig,
}

impl Defaults {
    fn with_runtime_env() -> Self {
        let mut defaults = Self::default();
        if env::var_os("AU_KPIS_HTTP__BIND").is_none() {
            if let Ok(port) = env::var("PORT") {
                if port.parse::<u16>().is_ok() {
                    defaults.http.bind = format!("0.0.0.0:{port}");
                }
            }
        }
        defaults
    }
}

/// Load [`AppConfig`] using the standard **defaults → TOML → env** precedence.
///
/// - Defaults come from each field's [`Default`] impl.
/// - `toml_path`, when `Some`, is merged in; a missing file or parse error
///   fails fast.
/// - Environment variables prefixed with [`ENV_PREFIX`] override everything
///   else; nested fields use [`ENV_NESTED_SEPARATOR`]
///   (`AU_KPIS_HTTP__BIND=0.0.0.0:9000`).
pub fn load(toml_path: Option<&Path>) -> Result<AppConfig, ConfigError> {
    load_from_figment(toml_path)
}

/// Load [`IngestionConfig`] using the standard **defaults → TOML → env**
/// precedence.
pub fn load_ingestion(toml_path: Option<&Path>) -> Result<IngestionConfig, ConfigError> {
    load_from_figment(toml_path)
}

fn load_from_figment<T>(toml_path: Option<&Path>) -> Result<T, ConfigError>
where
    T: DeserializeOwned,
{
    Ok(config_figment(toml_path).extract()?)
}

fn config_figment(toml_path: Option<&Path>) -> Figment {
    let mut fig = Figment::from(Serialized::defaults(Defaults::with_runtime_env()));

    if let Some(path) = toml_path {
        fig = fig.merge(Toml::file_exact(path));
    }

    fig.merge(Env::prefixed(ENV_PREFIX).split(ENV_NESTED_SEPARATOR))
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Jail;

    #[test]
    fn missing_database_url_fails_fast() {
        Jail::expect_with(|_jail| {
            let err = load(None).expect_err("database.url has no default");
            let msg = err.to_string();
            assert!(
                msg.contains("database"),
                "expected error to reference the missing path, got: {msg}"
            );
            match err {
                ConfigError::Figment(_) => {}
                other => panic!("expected Figment variant, got {other:?}"),
            }
            Ok(())
        });
    }

    #[test]
    fn missing_cache_url_fails_fast() {
        Jail::expect_with(|jail| {
            jail.set_env("AU_KPIS_DATABASE__URL", "postgres://env/db");
            let err = load(None).expect_err("cache.url has no default");
            let msg = err.to_string();
            assert!(
                msg.contains("cache"),
                "expected error to reference the missing path, got: {msg}"
            );
            match err {
                ConfigError::Figment(_) => {}
                other => panic!("expected Figment variant, got {other:?}"),
            }
            Ok(())
        });
    }

    #[test]
    fn env_alone_supplies_required_fields() {
        Jail::expect_with(|jail| {
            jail.set_env("PORT", "not-a-port");
            jail.set_env("AU_KPIS_DATABASE__URL", "postgres://env/db");
            jail.set_env("AU_KPIS_CACHE__URL", "redis://env:6379");
            let cfg = load(None).expect("env supplies required fields");
            assert_eq!(cfg.database.url, "postgres://env/db");
            assert_eq!(cfg.cache.url, "redis://env:6379");
            assert_eq!(cfg.http.bind, "0.0.0.0:3000");
            assert!(cfg.http.cors_allowed_origins.is_empty());
            assert_eq!(cfg.http.shutdown_grace_period_secs, 30);
            assert_eq!(cfg.telemetry.service_name, "au-kpis");
            assert_eq!(cfg.telemetry.log_format, LogFormat::Json);
            assert!(cfg.telemetry.otlp_endpoint.is_none());
            Ok(())
        });
    }

    #[test]
    fn railway_port_supplies_default_bind_when_explicit_bind_is_absent() {
        Jail::expect_with(|jail| {
            jail.set_env("PORT", "9123");
            jail.set_env("AU_KPIS_DATABASE__URL", "postgres://env/db");
            jail.set_env("AU_KPIS_CACHE__URL", "redis://env:6379");

            let cfg = load(None).expect("env supplies required fields");

            assert_eq!(cfg.http.bind, "0.0.0.0:9123");
            Ok(())
        });
    }

    #[test]
    fn explicit_bind_overrides_railway_port_default() {
        Jail::expect_with(|jail| {
            jail.set_env("PORT", "9123");
            jail.set_env("AU_KPIS_HTTP__BIND", "0.0.0.0:8080");
            jail.set_env("AU_KPIS_DATABASE__URL", "postgres://env/db");
            jail.set_env("AU_KPIS_CACHE__URL", "redis://env:6379");

            let cfg = load(None).expect("env supplies required fields");

            assert_eq!(cfg.http.bind, "0.0.0.0:8080");
            Ok(())
        });
    }

    #[test]
    fn env_parses_cors_allowed_origins_list() {
        Jail::expect_with(|jail| {
            jail.set_env("PORT", "not-a-port");
            jail.set_env("AU_KPIS_DATABASE__URL", "postgres://env/db");
            jail.set_env("AU_KPIS_CACHE__URL", "redis://env:6379");
            jail.set_env(
                "AU_KPIS_HTTP__CORS_ALLOWED_ORIGINS",
                r#"["http://127.0.0.1:4173","http://localhost:4173"]"#,
            );

            let cfg = load(None).expect("env supplies cors origins");

            assert_eq!(
                cfg.http.cors_allowed_origins,
                vec![
                    "http://127.0.0.1:4173".to_string(),
                    "http://localhost:4173".to_string(),
                ]
            );
            Ok(())
        });
    }

    #[test]
    fn default_rate_limits_match_free_tier_spec() {
        Jail::expect_with(|jail| {
            jail.set_env("PORT", "not-a-port");
            jail.set_env("AU_KPIS_DATABASE__URL", "postgres://env/db");
            jail.set_env("AU_KPIS_CACHE__URL", "redis://env:6379");

            let cfg = load(None).expect("env supplies required fields");
            let free = cfg
                .rate_limits
                .tiers
                .get("free")
                .expect("free tier configured");

            assert_eq!(free.per_key.per_second, 60);
            assert_eq!(free.per_key.per_hour, 1_000);
            assert_eq!(free.per_key.burst_multiplier, 2);
            assert_eq!(cfg.rate_limits.anonymous.per_second, 10);
            assert_eq!(cfg.rate_limits.anonymous.per_hour, 100);
            assert_eq!(cfg.rate_limits.anonymous.burst_multiplier, 2);
            Ok(())
        });
    }

    #[test]
    fn env_can_override_rate_limit_tiers() {
        Jail::expect_with(|jail| {
            jail.set_env("PORT", "not-a-port");
            jail.set_env("AU_KPIS_DATABASE__URL", "postgres://env/db");
            jail.set_env("AU_KPIS_CACHE__URL", "redis://env:6379");
            jail.set_env("AU_KPIS_RATE_LIMITS__DEFAULT_TIER", "internal");
            jail.set_env(
                "AU_KPIS_RATE_LIMITS__TIERS__internal__PER_KEY__PER_SECOND",
                "120",
            );
            jail.set_env(
                "AU_KPIS_RATE_LIMITS__TIERS__internal__PER_KEY__PER_HOUR",
                "5000",
            );
            jail.set_env(
                "AU_KPIS_RATE_LIMITS__TIERS__internal__PER_KEY__BURST_MULTIPLIER",
                "3",
            );
            jail.set_env(
                "AU_KPIS_RATE_LIMITS__TIERS__internal__PER_IP__PER_SECOND",
                "20",
            );
            jail.set_env(
                "AU_KPIS_RATE_LIMITS__TIERS__internal__PER_IP__PER_HOUR",
                "500",
            );
            jail.set_env(
                "AU_KPIS_RATE_LIMITS__TIERS__internal__PER_IP__BURST_MULTIPLIER",
                "2",
            );

            let cfg = load(None).expect("env supplies rate-limit tiers");
            let internal = cfg
                .rate_limits
                .tiers
                .get("internal")
                .expect("internal tier configured");

            assert_eq!(cfg.rate_limits.default_tier, "internal");
            assert_eq!(internal.per_key.per_second, 120);
            assert_eq!(internal.per_key.per_hour, 5_000);
            assert_eq!(internal.per_key.burst_multiplier, 3);
            assert_eq!(internal.per_ip.per_second, 20);
            assert_eq!(internal.per_ip.per_hour, 500);
            Ok(())
        });
    }

    #[test]
    fn ingestion_env_allows_missing_cache_url() {
        Jail::expect_with(|jail| {
            jail.set_env("PORT", "not-a-port");
            jail.set_env("AU_KPIS_DATABASE__URL", "postgres://env/db");
            let cfg = load_ingestion(None).expect("ingestion config without cache url");
            assert_eq!(cfg.database.url, "postgres://env/db");
            assert!(cfg.cache.is_none());
            assert_eq!(cfg.http.bind, "0.0.0.0:3000");
            Ok(())
        });
    }

    #[test]
    fn toml_overrides_defaults() {
        Jail::expect_with(|jail| {
            jail.set_env("PORT", "not-a-port");
            jail.create_file(
                "au-kpis.toml",
                r#"
                    [http]
                    bind = "127.0.0.1:3000"
                    cors_allowed_origins = ["https://app.au-kpis.example"]
                    shutdown_grace_period_secs = 45

                    [database]
                    url = "postgres://from-toml/db"

                    [cache]
                    url = "redis://cache.internal:6379"

                    [telemetry]
                    service_name = "from-toml"
                    log_format = "pretty"
                    log_level = "debug"
                "#,
            )?;
            let cfg = load(Some(Path::new("au-kpis.toml"))).unwrap();
            assert_eq!(cfg.http.bind, "127.0.0.1:3000");
            assert_eq!(
                cfg.http.cors_allowed_origins,
                vec!["https://app.au-kpis.example".to_string()]
            );
            assert_eq!(cfg.http.shutdown_grace_period_secs, 45);
            assert_eq!(cfg.database.url, "postgres://from-toml/db");
            assert_eq!(cfg.cache.url, "redis://cache.internal:6379");
            assert_eq!(cfg.telemetry.service_name, "from-toml");
            assert_eq!(cfg.telemetry.log_format, LogFormat::Pretty);
            assert_eq!(cfg.telemetry.log_level, "debug");
            Ok(())
        });
    }

    #[test]
    fn env_overrides_toml() {
        Jail::expect_with(|jail| {
            jail.set_env("PORT", "not-a-port");
            jail.create_file(
                "au-kpis.toml",
                r#"
                    [http]
                    bind = "127.0.0.1:3000"

                    [database]
                    url = "postgres://from-toml/db"
                "#,
            )?;
            jail.set_env("AU_KPIS_HTTP__BIND", "0.0.0.0:9999");
            jail.set_env("AU_KPIS_DATABASE__URL", "postgres://from-env/db");
            jail.set_env("AU_KPIS_CACHE__URL", "redis://from-env:6379");
            jail.set_env("AU_KPIS_HTTP__SHUTDOWN_GRACE_PERIOD_SECS", "12");
            jail.set_env("AU_KPIS_TELEMETRY__OTLP_ENDPOINT", "http://otel:4317");
            let cfg = load(Some(Path::new("au-kpis.toml"))).unwrap();
            assert_eq!(cfg.http.bind, "0.0.0.0:9999");
            assert_eq!(cfg.http.shutdown_grace_period_secs, 12);
            assert_eq!(cfg.database.url, "postgres://from-env/db");
            assert_eq!(cfg.cache.url, "redis://from-env:6379");
            assert_eq!(
                cfg.telemetry.otlp_endpoint.as_deref(),
                Some("http://otel:4317")
            );
            Ok(())
        });
    }

    #[test]
    fn missing_toml_file_fails_fast() {
        Jail::expect_with(|jail| {
            jail.set_env("AU_KPIS_DATABASE__URL", "postgres://env/db");
            jail.set_env("AU_KPIS_CACHE__URL", "redis://env:6379");
            let err =
                load(Some(Path::new("does-not-exist.toml"))).expect_err("missing TOML should fail");
            assert!(matches!(err, ConfigError::Figment(_)));
            Ok(())
        });
    }

    #[test]
    fn config_error_is_permanent() {
        Jail::expect_with(|_jail| {
            let err = load(None).expect_err("no required connection URLs");
            assert_eq!(err.class(), ErrorClass::Permanent);
            Ok(())
        });
    }

    #[test]
    fn core_error_composes_via_from() {
        let io: std::io::Error = std::io::Error::other("unreachable");
        let err: ConfigError = CoreError::from(io).into();
        assert_eq!(err.class(), ErrorClass::Transient);
    }
}
