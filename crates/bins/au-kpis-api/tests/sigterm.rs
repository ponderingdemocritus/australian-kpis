use std::{
    io::{Read, Write},
    net::TcpStream,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use assert_cmd::cargo::cargo_bin;
use au_kpis_testing::{
    redis::{RedisHarness, start_redis},
    timescale::{TimescaleHarness, start_timescale},
};

#[tokio::test(flavor = "multi_thread")]
async fn api_binary_honors_sigterm() {
    let mut harness = ApiProcess::start("au_kpis_api_sigterm", None).await;
    let addr = harness.addr.clone();

    assert!(
        TcpStream::connect(&addr).is_ok(),
        "au-kpis-api never became ready on {addr}"
    );

    harness.send_sigterm();
    harness.wait_for_exit(Duration::from_secs(5));
}

#[tokio::test(flavor = "multi_thread")]
async fn api_binary_serves_health_route() {
    let _harness = ApiProcess::start("au_kpis_api_health", None).await;
    let response = http_get(&(_harness.addr), "/v1/health");

    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "unexpected health response: {response}"
    );
    assert!(
        response.contains(r#""status":"ok""#),
        "health body did not include ok status: {response}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn api_binary_honors_configured_shutdown_grace_period() {
    let mut harness = ApiProcess::start("au_kpis_api_sigterm_drain", Some(1)).await;
    let mut stream = TcpStream::connect(&harness.addr).expect("open in-flight request connection");
    stream
        .write_all(b"GET /v1/health HTTP/1.1\r\nHost: localhost\r\n")
        .expect("write partial request");

    let started = Instant::now();
    harness.send_sigterm();

    thread::sleep(Duration::from_millis(400));
    assert!(
        harness.child.try_wait().expect("poll child").is_none(),
        "server exited before the configured drain window elapsed"
    );

    harness.wait_for_exit(Duration::from_secs(5));
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "server did not honor configured shutdown grace period"
    );
}

struct ApiProcess {
    child: Child,
    addr: String,
    startup_file: PathBuf,
    _timescale: TimescaleHarness,
    _redis: RedisHarness,
}

impl ApiProcess {
    async fn start(database: &str, shutdown_grace_period_secs: Option<u64>) -> Self {
        let binary = cargo_bin("au-kpis-api");
        let startup_file = unique_startup_file();
        let timescale = start_timescale(database)
            .await
            .expect("start timescale test container");
        let redis = start_redis().await.expect("start redis test container");

        let mut command = Command::new(binary);
        command
            .env("AU_KPIS_HTTP__BIND", "127.0.0.1:0")
            .env("AU_KPIS_DATABASE__URL", timescale.url())
            .env("AU_KPIS_CACHE__URL", redis.url())
            .env("AU_KPIS_STARTUP_NOTIFY_FILE", &startup_file)
            // The child is a different instrumented binary. Inheriting the
            // nextest process profile path makes llvm-cov merge incompatible
            // counters and can crash report generation.
            .env("LLVM_PROFILE_FILE", ignored_child_coverage_profile())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        if let Some(seconds) = shutdown_grace_period_secs {
            command.env(
                "AU_KPIS_HTTP__SHUTDOWN_GRACE_PERIOD_SECS",
                seconds.to_string(),
            );
        }

        let mut child = command.spawn().expect("spawn au-kpis-api");
        let addr = wait_for_startup_file(&startup_file, &mut child);

        Self {
            child,
            addr,
            startup_file,
            _timescale: timescale,
            _redis: redis,
        }
    }

    fn send_sigterm(&self) {
        let kill = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("send SIGTERM");
        assert!(kill.success(), "SIGTERM failed: {kill:?}");
    }

    fn wait_for_exit(&mut self, within: Duration) {
        let deadline = Instant::now() + within;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child") {
                assert!(
                    status.success(),
                    "au-kpis-api exited unsuccessfully: {status}"
                );
                break;
            }

            assert!(
                Instant::now() < deadline,
                "au-kpis-api did not exit within {}s of SIGTERM",
                within.as_secs()
            );
            thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for ApiProcess {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.startup_file);
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn unique_startup_file() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "au-kpis-api-startup-{}-{}.txt",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    path
}

fn ignored_child_coverage_profile() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "au-kpis-api-child-coverage-{}-%p-%m.profraw",
        std::process::id()
    ));
    path
}

fn wait_for_startup_file(path: &PathBuf, child: &mut std::process::Child) -> String {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        if let Ok(addr) = std::fs::read_to_string(path) {
            let addr = addr.trim().to_string();
            if !addr.is_empty() {
                let _ = std::fs::remove_file(path);
                return addr;
            }
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

    panic!("au-kpis-api never reported its bound address");
}

fn http_get(addr: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect to API");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("write HTTP request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read HTTP response");
    response
}
