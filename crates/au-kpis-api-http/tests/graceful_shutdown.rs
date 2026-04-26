use std::{
    process::{Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use au_kpis_api_http::{AppState, router};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::{AppConfig, DatabaseConfig, HttpConfig, LogFormat, TelemetryConfig};
use au_kpis_telemetry::Telemetry;
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct NoopCacheBackend;

#[async_trait::async_trait]
impl CacheBackend for NoopCacheBackend {
    async fn get(&self, _key: &str) -> Result<Option<String>, CacheError> {
        Ok(None)
    }

    async fn set(&self, _key: &str, _value: String, _ttl: Duration) -> Result<(), CacheError> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<bool, CacheError> {
        Ok(false)
    }

    async fn take_token_bucket(
        &self,
        _key: &str,
        _config: TokenBucketConfig,
        _requested: u32,
        _now_ms: u64,
    ) -> Result<RateLimitDecision, CacheError> {
        Ok(RateLimitDecision {
            allowed: true,
            remaining: 0,
            retry_after: Duration::ZERO,
        })
    }
}

fn test_state(token: CancellationToken) -> AppState {
    let db = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://postgres:postgres@localhost/au_kpis")
        .expect("lazy postgres pool");

    let config = AppConfig {
        http: HttpConfig {
            bind: "127.0.0.1:0".into(),
            cors_allowed_origins: Vec::new(),
        },
        database: DatabaseConfig {
            url: "postgres://postgres:postgres@localhost/au_kpis".into(),
        },
        telemetry: TelemetryConfig {
            service_name: "au-kpis-test".into(),
            log_format: LogFormat::Json,
            log_level: "info".into(),
            otlp_endpoint: None,
        },
    };

    AppState::new(
        db,
        Arc::new(CacheClient::from_backend(NoopCacheBackend)),
        Arc::new(config),
        Arc::new(Telemetry::disabled()),
        token,
    )
}

#[tokio::test]
async fn server_exits_promptly_when_shutdown_token_is_cancelled() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let token = CancellationToken::new();
    let state = test_state(token.clone());
    let shutdown = token.clone();

    let router = router(state).expect("router");
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    });

    token.cancel();

    let join = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server should stop within timeout")
        .expect("server task should join");

    join.expect("server should exit cleanly");
}

#[test]
fn api_binary_honors_sigterm() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let build = Command::new("cargo")
        .args(["build", "-p", "au-kpis-api"])
        .current_dir(manifest_dir)
        .status()
        .expect("build au-kpis-api binary");
    assert!(build.success(), "binary build failed: {build:?}");

    let binary = std::path::Path::new(manifest_dir).join("../../target/debug/au-kpis-api");
    let addr = reserve_local_addr();

    let mut child = Command::new(binary)
        .env("AU_KPIS_HTTP__BIND", &addr)
        .env(
            "AU_KPIS_DATABASE__URL",
            "postgres://postgres:postgres@localhost/au_kpis",
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn au-kpis-api");

    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        if std::net::TcpStream::connect(&addr).is_ok() {
            break;
        }
        if child
            .try_wait()
            .expect("poll child during startup")
            .is_some()
        {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    assert!(
        std::net::TcpStream::connect(&addr).is_ok(),
        "au-kpis-api never became ready on {addr}"
    );

    let kill = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(kill.success(), "SIGTERM failed: {kill:?}");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            assert!(
                status.success(),
                "contract server exited unsuccessfully: {status}"
            );
            break;
        }

        assert!(
            Instant::now() < deadline,
            "contract server did not exit within 5s of SIGTERM"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn reserve_local_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr.to_string()
}
