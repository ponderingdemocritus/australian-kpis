//! tracing + OTel setup.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::env;

use au_kpis_config::{LogFormat, TelemetryConfig};
use opentelemetry::{KeyValue, trace::TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, runtime::Tokio, trace::TracerProvider as SdkTracerProvider};
use thiserror::Error;
use tracing::Subscriber;
use tracing_subscriber::{
    EnvFilter,
    fmt::{self, writer::BoxMakeWriter},
    layer::SubscriberExt,
    registry::LookupSpan,
};

const OTEL_EXPORTER_OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTEL_SERVICE_NAME_ENV: &str = "OTEL_SERVICE_NAME";
const RUST_LOG_ENV: &str = "RUST_LOG";
#[cfg(feature = "tokio-console")]
const TOKIO_CONSOLE_ENABLED_ENV: &str = "AU_KPIS_TOKIO_CONSOLE";

/// Errors returned while initialising tracing and OpenTelemetry.
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// `RUST_LOG` / configured log filter could not be parsed.
    #[error("invalid log filter: {0}")]
    EnvFilter(#[from] tracing_subscriber::filter::ParseError),

    /// The OTLP exporter could not be constructed.
    #[error("otlp exporter: {0}")]
    OtlpExporter(#[from] opentelemetry::trace::TraceError),

    /// The process-wide tracing subscriber was already installed.
    #[error("global telemetry subscriber already installed")]
    SetGlobalDefault(#[from] tracing::dispatcher::SetGlobalDefaultError),
}

/// Handle returned by [`init`].
///
/// Keeping this value alive preserves the non-blocking log worker and allows
/// `Drop` to flush/shutdown the tracer provider cleanly on process exit.
#[derive(Debug)]
pub struct Telemetry {
    tracer_provider: Option<SdkTracerProvider>,
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown();
        }
    }
}

/// Install the process-wide tracing subscriber and return a shutdown handle.
pub fn init(config: &TelemetryConfig) -> Result<Telemetry, TelemetryError> {
    let (dispatch, telemetry) =
        build_dispatch(resolve_config(config), BoxMakeWriter::new(std::io::stderr))?;
    tracing::dispatcher::set_global_default(dispatch)?;
    Ok(telemetry)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedTelemetryConfig {
    service_name: String,
    log_format: LogFormat,
    log_level: String,
    otlp_endpoint: Option<String>,
    #[cfg(feature = "tokio-console")]
    tokio_console_enabled: bool,
}

fn resolve_config(config: &TelemetryConfig) -> ResolvedTelemetryConfig {
    ResolvedTelemetryConfig {
        service_name: env::var(OTEL_SERVICE_NAME_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| config.service_name.clone()),
        log_format: config.log_format,
        log_level: env::var(RUST_LOG_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| config.log_level.clone()),
        otlp_endpoint: env::var(OTEL_EXPORTER_OTLP_ENDPOINT_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| config.otlp_endpoint.clone()),
        #[cfg(feature = "tokio-console")]
        tokio_console_enabled: env_flag(TOKIO_CONSOLE_ENABLED_ENV),
    }
}

#[cfg(feature = "tokio-console")]
fn env_flag(name: &str) -> bool {
    env::var(name).ok().is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn build_dispatch(
    config: ResolvedTelemetryConfig,
    make_writer: BoxMakeWriter,
) -> Result<(tracing::Dispatch, Telemetry), TelemetryError> {
    let tracer_provider = build_tracer_provider(&config)?;
    let subscriber = match config.log_format {
        LogFormat::Json => {
            let base = tracing_subscriber::registry()
                .with(EnvFilter::try_new(config.log_level.clone())?)
                .with(
                    fmt::layer()
                        .json()
                        .with_current_span(true)
                        .with_span_list(false)
                        .flatten_event(false)
                        .with_writer(make_writer)
                        .with_ansi(false),
                );
            attach_optional_layers(base, &config, tracer_provider.as_ref())
        }
        LogFormat::Pretty => {
            let base = tracing_subscriber::registry()
                .with(EnvFilter::try_new(config.log_level.clone())?)
                .with(fmt::layer().pretty().with_writer(make_writer));
            attach_optional_layers(base, &config, tracer_provider.as_ref())
        }
    };

    Ok((
        tracing::Dispatch::new(subscriber),
        Telemetry { tracer_provider },
    ))
}

fn build_tracer_provider(
    config: &ResolvedTelemetryConfig,
) -> Result<Option<SdkTracerProvider>, TelemetryError> {
    let Some(endpoint) = config.otlp_endpoint.as_deref() else {
        return Ok(None);
    };

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter, Tokio)
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            config.service_name.clone(),
        )]))
        .build();

    Ok(Some(tracer_provider))
}

fn attach_optional_layers<S>(
    subscriber: S,
    config: &ResolvedTelemetryConfig,
    tracer_provider: Option<&SdkTracerProvider>,
) -> Box<dyn Subscriber + Send + Sync>
where
    S: Subscriber + Send + Sync + 'static + for<'span> LookupSpan<'span>,
{
    #[cfg(feature = "tokio-console")]
    {
        match tracer_provider {
            Some(provider) => Box::new(
                subscriber
                    .with(
                        tracing_opentelemetry::layer()
                            .with_tracer(provider.tracer(config.service_name.clone())),
                    )
                    .with(config.tokio_console_enabled.then(console_subscriber::spawn)),
            ),
            None => Box::new(
                subscriber.with(config.tokio_console_enabled.then(console_subscriber::spawn)),
            ),
        }
    }

    #[cfg(not(feature = "tokio-console"))]
    {
        match tracer_provider {
            Some(provider) => Box::new(
                subscriber.with(
                    tracing_opentelemetry::layer()
                        .with_tracer(provider.tracer(config.service_name.clone())),
                ),
            ),
            None => Box::new(subscriber),
        }
    }
}

#[cfg(test)]
fn build_subscriber<W>(
    config: &TelemetryConfig,
    make_writer: W,
) -> Result<(tracing::Dispatch, Telemetry), TelemetryError>
where
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    build_dispatch(resolve_config(config), BoxMakeWriter::new(make_writer))
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use au_kpis_config::{LogFormat, TelemetryConfig};
    use figment::Jail;
    use serde_json::Value;
    use tracing_subscriber::fmt::MakeWriter;

    use super::{build_subscriber, resolve_config};

    #[test]
    fn otlp_endpoint_can_come_from_env() {
        Jail::expect_with(|jail| {
            jail.set_env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318");

            let resolved = resolve_config(&base_config());

            assert_eq!(
                resolved.otlp_endpoint.as_deref(),
                Some("http://127.0.0.1:4318")
            );

            Ok(())
        });
    }

    #[test]
    fn json_logs_include_span_fields() {
        Jail::expect_with(|_jail| {
            let sink = SharedBuffer::default();
            let (subscriber, _telemetry) =
                build_subscriber(&base_config(), sink.make_writer()).expect("subscriber");

            tracing::dispatcher::with_default(&subscriber, || {
                let span = tracing::info_span!("load_batch", job_id = 7_u64);
                let _entered = span.enter();
                tracing::info!(request_id = "req-1", rows = 42_u64, "span emitted");
            });

            let line = sink.as_string();
            let json: Value = serde_json::from_str(line.trim()).expect("valid json log line");

            assert_eq!(json["level"], "INFO");
            assert_eq!(json["fields"]["message"], "span emitted");
            assert_eq!(json["fields"]["request_id"], "req-1");
            assert_eq!(json["fields"]["rows"], 42);
            assert_eq!(json["span"]["job_id"], 7);
            assert_eq!(json["span"]["name"], "load_batch");

            Ok(())
        });
    }

    fn base_config() -> TelemetryConfig {
        TelemetryConfig {
            service_name: "au-kpis-test".into(),
            log_format: LogFormat::Json,
            log_level: "info".into(),
            otlp_endpoint: None,
        }
    }

    #[derive(Clone, Debug, Default)]
    struct SharedBuffer {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedBuffer {
        fn make_writer(&self) -> SharedWriter {
            SharedWriter {
                bytes: Arc::clone(&self.bytes),
            }
        }

        fn as_string(&self) -> String {
            String::from_utf8(self.bytes.lock().expect("lock").clone()).expect("utf8")
        }
    }

    #[derive(Clone, Debug)]
    struct SharedWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl<'a> MakeWriter<'a> for SharedWriter {
        type Writer = SharedGuard;

        fn make_writer(&'a self) -> Self::Writer {
            SharedGuard {
                bytes: Arc::clone(&self.bytes),
            }
        }
    }

    #[derive(Debug)]
    struct SharedGuard {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for SharedGuard {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes.lock().expect("lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
