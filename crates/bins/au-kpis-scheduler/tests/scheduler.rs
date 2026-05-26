use std::{
    io::{Read, Write},
    net::TcpStream,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::LazyLock,
    thread,
    time::{Duration, Instant},
};

use assert_cmd::cargo::cargo_bin;
use au_kpis_testing::timescale::{TimescaleHarness, start_timescale};

static SCHEDULER_PROCESS_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_schedulers_singleton_and_failover_emits_discovery_jobs() {
    let _guard = SCHEDULER_PROCESS_TEST_LOCK.lock().await;
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = SchedulerDb::start("au_kpis_scheduler_failover").await;
    let mut first = SchedulerProcess::start(db.url(), "scheduler-a").await;
    let mut second = SchedulerProcess::start(db.url(), "scheduler-b").await;

    let leader = wait_for_single_leader(&mut first, &mut second);
    wait_for_discovery_job_count(db.pool(), 1).await;
    let before_failover = discovery_job_count(db.pool()).await;

    match leader {
        Leader::First => {
            first.send_sigterm();
            first.wait_for_exit(Duration::from_secs(5));
            wait_until_leader(&mut second);
        }
        Leader::Second => {
            second.send_sigterm();
            second.wait_for_exit(Duration::from_secs(5));
            wait_until_leader(&mut first);
        }
    }

    wait_for_discovery_job_count(db.pool(), before_failover + 1).await;
}

struct SchedulerDb {
    pool: sqlx::PgPool,
    url: String,
    _timescale: TimescaleHarness,
}

impl SchedulerDb {
    async fn start(database: &str) -> Self {
        let timescale = start_timescale(database)
            .await
            .expect("start timescale test container");
        let pool = connect_with_retry(timescale.url()).await;
        Self {
            pool,
            url: timescale.url().to_string(),
            _timescale: timescale,
        }
    }

    fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    fn url(&self) -> &str {
        &self.url
    }
}

struct SchedulerProcess {
    addr: String,
    startup_file: PathBuf,
    child: Child,
}

impl SchedulerProcess {
    async fn start(database_url: &str, worker_id: &str) -> Self {
        let startup_file = unique_startup_file(worker_id);
        let mut command = Command::new(cargo_bin("au-kpis-scheduler"));
        command
            .args([
                "--worker-id",
                worker_id,
                "--tick-ms",
                "50",
                "--abs-interval-ms",
                "200",
                "run",
            ])
            .env("AU_KPIS_HTTP__BIND", "127.0.0.1:0")
            .env("AU_KPIS_DATABASE__URL", database_url)
            .env("AU_KPIS_STARTUP_NOTIFY_FILE", &startup_file)
            .env(
                "LLVM_PROFILE_FILE",
                ignored_child_coverage_profile(worker_id),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        let mut child = command.spawn().expect("spawn au-kpis-scheduler");
        let addr = wait_for_startup_file(&startup_file, &mut child);
        Self {
            addr,
            startup_file,
            child,
        }
    }

    fn is_leader(&mut self) -> bool {
        self.assert_running();
        http_get(&self.addr, "/metrics").contains("au_kpis_scheduler_leader_active 1")
    }

    fn send_sigterm(&self) {
        let kill = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("send SIGTERM");
        assert!(kill.success(), "SIGTERM failed: {kill:?}");
    }

    fn assert_running(&mut self) {
        if let Some(status) = self.child.try_wait().expect("poll child") {
            panic!("au-kpis-scheduler exited unexpectedly: {status}");
        }
    }

    fn wait_for_exit(&mut self, within: Duration) {
        let deadline = Instant::now() + within;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child") {
                assert!(
                    status.success(),
                    "au-kpis-scheduler exited unsuccessfully: {status}"
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "au-kpis-scheduler did not exit within {}s of SIGTERM",
                within.as_secs()
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for SchedulerProcess {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.startup_file);
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leader {
    First,
    Second,
}

fn wait_for_single_leader(first: &mut SchedulerProcess, second: &mut SchedulerProcess) -> Leader {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let first_leads = first.is_leader();
        let second_leads = second.is_leader();
        match (first_leads, second_leads) {
            (true, false) => return Leader::First,
            (false, true) => return Leader::Second,
            (true, true) => panic!("both schedulers reported leader_active=1"),
            (false, false) => {}
        }
        assert!(
            Instant::now() < deadline,
            "no singleton scheduler leader became active"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn wait_until_leader(process: &mut SchedulerProcess) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if process.is_leader() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "standby scheduler did not acquire leadership"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

async fn connect_with_retry(database_url: &str) -> sqlx::PgPool {
    let cfg = au_kpis_config::DatabaseConfig {
        url: database_url.to_string(),
    };
    let mut last_err = None;
    for _ in 0..10 {
        match au_kpis_db::connect(&cfg).await {
            Ok(pool) => {
                au_kpis_db::migrate(&pool).await.expect("apply migrations");
                return pool;
            }
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    panic!("timescaledb did not accept connections: {last_err:?}");
}

async fn wait_for_discovery_job_count(pool: &sqlx::PgPool, minimum: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let count = discovery_job_count(pool).await;
        if count >= minimum {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "scheduler emitted {count} discovery jobs, expected at least {minimum}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn discovery_job_count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM queue_jobs WHERE stage = 'discover'")
        .fetch_one(pool)
        .await
        .expect("count discovery jobs")
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}

fn unique_startup_file(worker_id: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "au-kpis-scheduler-startup-{worker_id}-{}-{}.txt",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    path
}

fn ignored_child_coverage_profile(worker_id: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "au-kpis-scheduler-child-coverage-{worker_id}-{}-%p-%m.profraw",
        std::process::id()
    ));
    path
}

fn wait_for_startup_file(path: &PathBuf, child: &mut Child) -> String {
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

    panic!("au-kpis-scheduler never reported its bound address");
}

fn http_get(addr: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect to scheduler metrics server");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set metrics read timeout");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("write HTTP request");

    read_http_response(&mut stream)
}

fn read_http_response(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut content_length = None;
    loop {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).expect("read HTTP response");
        assert!(read > 0, "metrics server closed before response completed");
        bytes.extend_from_slice(&chunk[..read]);

        if content_length.is_none() {
            if let Some((headers, _)) = split_headers_body(&bytes) {
                content_length = parse_content_length(headers);
            }
        }
        if let Some(len) = content_length {
            if let Some((_, body)) = split_headers_body(&bytes) {
                if body.len() >= len {
                    return String::from_utf8(bytes).expect("metrics response is UTF-8");
                }
            }
        }
    }
}

fn split_headers_body(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| bytes.split_at(index + 4))
}

fn parse_content_length(headers: &[u8]) -> Option<usize> {
    std::str::from_utf8(headers)
        .expect("HTTP headers are UTF-8")
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().expect("valid content-length"))
        })
}
