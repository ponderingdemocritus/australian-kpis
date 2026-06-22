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
use au_kpis_domain::SourceId;
use au_kpis_queue::{ApalisPgQueue, Job, Queue};
use au_kpis_testing::{
    minio::{MinioHarness, start_minio},
    redis::{RedisHarness, start_redis},
    timescale::{TimescaleHarness, start_timescale},
};
use object_store::{Error as ObjectStoreError, ObjectStore, aws::AmazonS3Builder, path::Path};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

static INGESTION_PROCESS_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

const DATAFLOW_LISTING: &str = r#"{
  "data": {
    "dataflows": [{
      "id": "CPI",
      "agencyID": "ABS",
      "version": "2.0.0",
      "name": "Consumer Price Index",
      "updated": "2026-04-28T00:00:00Z",
      "links": [
        { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0", "rel": "self" }
      ]
    }]
  }
}"#;

const CPI_FIXTURE: &[u8] = include_bytes!("../../../adapters/abs/tests/fixtures/cpi_sdmx.json");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn once_mode_loads_abs_cpi_fixture_end_to_end() {
    let _guard = INGESTION_PROCESS_TEST_LOCK.lock().await;
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }
    let fixture_base_url = serve_abs_cpi_once().await;
    let harness = IngestionProcess::for_once("au_kpis_ingestion_once", &fixture_base_url).await;

    let status = harness
        .command()
        .args(["--once", "--source", "abs", "--dataflow", "cpi"])
        .status()
        .expect("run au-kpis-ingestion once");

    assert!(
        status.success(),
        "once mode exited unsuccessfully: {status}"
    );
    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(harness.pool())
        .await
        .expect("count observations");
    assert_eq!(observation_count, 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn once_mode_runs_without_cache_config() {
    let _guard = INGESTION_PROCESS_TEST_LOCK.lock().await;
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }
    let fixture_base_url = serve_abs_cpi_once().await;
    let harness = IngestionProcess::for_once_without_cache(
        "au_kpis_ingestion_once_no_cache",
        &fixture_base_url,
    )
    .await;

    let status = harness
        .command()
        .args(["--once", "--source", "abs", "--dataflow", "cpi"])
        .status()
        .expect("run au-kpis-ingestion once without cache config");

    assert!(
        status.success(),
        "once mode should not require cache config: {status}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn once_mode_requires_object_store_config() {
    let _guard = INGESTION_PROCESS_TEST_LOCK.lock().await;
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }
    let fixture_base_url = serve_abs_cpi_once().await;
    let harness = IngestionProcess::for_once_without_object_store(
        "au_kpis_ingestion_once_requires_object_store",
        &fixture_base_url,
    )
    .await;

    let output = harness
        .command()
        .args(["--once", "--source", "abs", "--dataflow", "cpi"])
        .output()
        .expect("run au-kpis-ingestion once without object store config");

    assert!(
        !output.status.success(),
        "once mode should fail fast without durable object store config"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("durable object store config"),
        "stderr should explain the missing durable object store config, got: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_mode_requires_object_store_config() {
    let _guard = INGESTION_PROCESS_TEST_LOCK.lock().await;
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }
    let timescale = start_timescale("au_kpis_ingestion_run_requires_object_store")
        .await
        .expect("start timescale test container");
    let redis = start_redis().await.expect("start redis test container");
    let pool = connect_with_retry(timescale.url()).await;
    seed_cpi_reference_data(&pool).await;
    let startup_file = unique_startup_file();
    let object_store = object_store_env();
    let mut command = base_command(
        timescale.url(),
        Some(redis.url()),
        &startup_file,
        "http://127.0.0.1:1/rest",
    );
    clear_object_store_env(&mut command);
    let mut child = command
        .arg("run")
        .spawn()
        .expect("spawn au-kpis-ingestion run without object store config");

    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            assert!(
                !status.success(),
                "run mode should fail fast without durable object store config"
            );
            assert!(
                !startup_file.exists(),
                "startup notification should not be written before startup validation succeeds"
            );
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "run mode should exit quickly when object store config is missing"
        );
        thread::sleep(Duration::from_millis(50));
    }

    let mut success_command = base_command(
        timescale.url(),
        Some(redis.url()),
        &startup_file,
        "http://127.0.0.1:1/rest",
    );
    apply_object_store_env(&mut success_command, &object_store);
    let mut success_child = success_command
        .arg("run")
        .spawn()
        .expect("spawn au-kpis-ingestion run with object store config");
    wait_for_startup_file(&startup_file, &mut success_child);
    let _ = Command::new("kill")
        .args(["-TERM", &success_child.id().to_string()])
        .status();
    let _ = success_child.wait();
}

#[test]
fn once_mode_missing_cache_config_fails_after_config_loading() {
    let output = Command::new(cargo_bin("au-kpis-ingestion"))
        .env("AU_KPIS_DATABASE__URL", "postgres://127.0.0.1:1/au_kpis")
        .args(["--once", "--source", "abs", "--dataflow", "cpi"])
        .output()
        .expect("run au-kpis-ingestion once without cache config");

    assert!(
        !output.status.success(),
        "command should still fail without a real database"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("missing field `cache`"),
        "once mode should progress past config loading without cache config, got: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_mode_serves_metrics_until_sigterm_then_exits() {
    let _guard = INGESTION_PROCESS_TEST_LOCK.lock().await;
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }
    let mut harness = IngestionProcess::start_run("au_kpis_ingestion_run").await;

    let metrics = http_get(&harness.addr, "/metrics");
    assert!(
        metrics.starts_with("HTTP/1.1 200 OK"),
        "unexpected metrics response: {metrics}"
    );
    assert!(
        metrics.contains("au_kpis_ingestion_worker_loops_total"),
        "metrics body missing worker loop counter: {metrics}"
    );

    harness.send_sigterm();
    harness.wait_for_exit(Duration::from_secs(5));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_mode_syncs_adapter_catalog_before_worker_loop() {
    let _guard = INGESTION_PROCESS_TEST_LOCK.lock().await;
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }
    let mut harness = IngestionProcess::start_run("au_kpis_ingestion_catalog_sync").await;

    let apra_source_homepage: String =
        sqlx::query_scalar("SELECT homepage FROM sources WHERE id = 'apra'")
            .fetch_one(harness.pool())
            .await
            .expect("APRA source should be synced");
    assert_eq!(apra_source_homepage, "https://www.apra.gov.au");

    let (name, dimensions, measures, frequency, license): (
        String,
        Vec<String>,
        Vec<String>,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT name, dimensions, measures, frequency, license
         FROM dataflows
         WHERE id = 'apra.quarterly_statistics'",
    )
    .fetch_one(harness.pool())
    .await
    .expect("APRA dataflow should be synced");
    assert_eq!(name, "APRA quarterly statistics");
    assert_eq!(
        dimensions,
        vec!["publication", "table", "series", "entity", "sector"]
    );
    assert_eq!(measures, vec!["value"]);
    assert_eq!(frequency, "quarterly");
    assert_eq!(
        license,
        "Creative Commons Attribution 3.0 Australia Licence"
    );

    let apra_dimensions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM dimensions WHERE dataflow_id = 'apra.quarterly_statistics'",
    )
    .fetch_one(harness.pool())
    .await
    .expect("count APRA dimensions");
    assert_eq!(apra_dimensions, 5);

    let synced_static_dataflows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM dataflows
         WHERE id IN (
             'apra.quarterly_statistics',
             'rba.statistical_tables',
             'treasury.budget_papers',
             'state_budgets.nsw_budget',
             'state_budgets.vic_budget',
             'state_budgets.qld_budget'
         )",
    )
    .fetch_one(harness.pool())
    .await
    .expect("count synced static dataflows");
    assert_eq!(synced_static_dataflows, 6);

    harness.send_sigterm();
    harness.wait_for_exit(Duration::from_secs(5));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_mode_dead_letters_invalid_jobs_without_exiting() {
    let _guard = INGESTION_PROCESS_TEST_LOCK.lock().await;
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }
    let mut harness = IngestionProcess::start_run("au_kpis_ingestion_poison_job").await;
    let queue = ApalisPgQueue::new(harness.pool().clone());
    let job_id = queue
        .push(
            Job::discover(SourceId::new("unsupported").expect("valid source id"))
                .with_max_attempts(1),
        )
        .await
        .expect("push unsupported queue job");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if queue.dead_lettered(job_id).await.is_ok() {
            break;
        }
        harness.assert_running();
        assert!(
            Instant::now() < deadline,
            "worker did not dead-letter invalid job before timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    harness.assert_running();
    harness.send_sigterm();
    harness.wait_for_exit(Duration::from_secs(5));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_mode_exits_within_configured_shutdown_grace_period() {
    let _guard = INGESTION_PROCESS_TEST_LOCK.lock().await;
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }
    let mut harness = IngestionProcess::start_run_with_grace("au_kpis_ingestion_grace", 1).await;
    let mut stream = TcpStream::connect(&harness.addr).expect("open in-flight metrics connection");
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n")
        .expect("write partial metrics request");

    let started = Instant::now();
    harness.send_sigterm();
    let elapsed = harness.wait_for_exit(Duration::from_secs(3));
    assert!(
        elapsed <= Duration::from_secs(3) && started.elapsed() < Duration::from_secs(3),
        "worker exceeded the configured shutdown grace period"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn once_mode_rejects_unsupported_dataflow_before_external_io() {
    let _guard = INGESTION_PROCESS_TEST_LOCK.lock().await;
    let output = Command::new(cargo_bin("au-kpis-ingestion"))
        .args(["--once", "--source", "abs", "--dataflow", "wpi"])
        .output()
        .expect("run unsupported dataflow");

    assert!(
        !output.status.success(),
        "unsupported dataflow unexpectedly passed"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unsupported dataflow"),
        "stderr should explain unsupported dataflow, got: {stderr}"
    );
}

#[test]
fn dockerfile_defaults_to_run_but_keeps_cli_overridable() {
    let dockerfile_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../infra/docker/au-kpis-ingestion.Dockerfile");
    let dockerfile = std::fs::read_to_string(dockerfile_path).expect("read ingestion Dockerfile");

    assert!(
        dockerfile.contains("ENTRYPOINT [\"/usr/local/bin/au-kpis-ingestion\"]"),
        "image entrypoint should stay on the binary so callers can override the command"
    );
    assert!(
        dockerfile.contains("CMD [\"run\"]"),
        "image should default to run mode via CMD"
    );
}

#[test]
fn minio_bucket_name_normalizes_test_identifiers() {
    assert_eq!(
        minio_bucket_name("au_kpis_ingestion_once"),
        "au-kpis-ingestion-once-artifacts"
    );
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}

async fn serve_abs_cpi_once() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let read = stream.read(&mut request).await.expect("read request");
            let request = String::from_utf8_lossy(&request[..read]);
            if request.starts_with("GET /rest/dataflow/ABS/CPI") {
                write_response(
                    &mut stream,
                    "application/vnd.sdmx.structure+json",
                    DATAFLOW_LISTING.as_bytes(),
                )
                .await;
            } else if request.starts_with("GET /rest/data/ABS,CPI,2.0.0/all") {
                write_response(&mut stream, "application/vnd.sdmx.data+json", CPI_FIXTURE).await;
            } else {
                let body = b"not found";
                let response = format!(
                    "HTTP/1.1 404 Not Found\r\ncontent-length: {}\r\n\r\n",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("write 404 headers");
                stream.write_all(body).await.expect("write 404 body");
            }
        }
    });

    format!("http://{addr}/rest")
}

async fn write_response(stream: &mut tokio::net::TcpStream, content_type: &str, body: &[u8]) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\r\n",
        body.len(),
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write response headers");
    stream.write_all(body).await.expect("write response body");
}

struct IngestionProcess {
    database_url: String,
    cache_url: Option<String>,
    object_store: Option<ObjectStoreEnv>,
    addr: String,
    startup_file: PathBuf,
    child: Child,
    pool: sqlx::PgPool,
    _timescale: TimescaleHarness,
    _redis: Option<RedisHarness>,
    _minio: Option<MinioHarness>,
}

impl IngestionProcess {
    async fn for_once(database: &str, fixture_base_url: &str) -> Self {
        let timescale = start_timescale(database)
            .await
            .expect("start timescale test container");
        let redis = start_redis().await.expect("start redis test container");
        let minio = start_minio(minio_bucket_name(database))
            .await
            .expect("start minio test container");
        wait_for_object_store_ready(&ObjectStoreEnv::from_minio(&minio)).await;
        let pool = connect_with_retry(timescale.url()).await;
        seed_cpi_reference_data(&pool).await;
        Self {
            database_url: timescale.url().to_string(),
            cache_url: Some(redis.url().to_string()),
            object_store: Some(ObjectStoreEnv::from_minio(&minio)),
            addr: String::new(),
            startup_file: unique_startup_file(),
            child: Command::new("true")
                .spawn()
                .expect("spawn completed placeholder"),
            pool,
            _timescale: timescale,
            _redis: Some(redis),
            _minio: Some(minio),
        }
        .with_abs_fixture(fixture_base_url)
    }

    async fn for_once_without_cache(database: &str, fixture_base_url: &str) -> Self {
        let timescale = start_timescale(database)
            .await
            .expect("start timescale test container");
        let minio = start_minio(minio_bucket_name(database))
            .await
            .expect("start minio test container");
        wait_for_object_store_ready(&ObjectStoreEnv::from_minio(&minio)).await;
        let pool = connect_with_retry(timescale.url()).await;
        seed_cpi_reference_data(&pool).await;
        Self {
            database_url: timescale.url().to_string(),
            cache_url: None,
            object_store: Some(ObjectStoreEnv::from_minio(&minio)),
            addr: String::new(),
            startup_file: unique_startup_file(),
            child: Command::new("true")
                .spawn()
                .expect("spawn completed placeholder"),
            pool,
            _timescale: timescale,
            _redis: None,
            _minio: Some(minio),
        }
        .with_abs_fixture(fixture_base_url)
    }

    async fn for_once_without_object_store(database: &str, fixture_base_url: &str) -> Self {
        let timescale = start_timescale(database)
            .await
            .expect("start timescale test container");
        let pool = connect_with_retry(timescale.url()).await;
        seed_cpi_reference_data(&pool).await;
        Self {
            database_url: timescale.url().to_string(),
            cache_url: None,
            object_store: None,
            addr: String::new(),
            startup_file: unique_startup_file(),
            child: Command::new("true")
                .spawn()
                .expect("spawn completed placeholder"),
            pool,
            _timescale: timescale,
            _redis: None,
            _minio: None,
        }
        .with_abs_fixture(fixture_base_url)
    }

    async fn start_run(database: &str) -> Self {
        Self::start_run_with_grace(database, 30).await
    }

    async fn start_run_with_grace(database: &str, shutdown_grace_secs: u64) -> Self {
        let timescale = start_timescale(database)
            .await
            .expect("start timescale test container");
        let redis = start_redis().await.expect("start redis test container");
        let minio = start_minio(minio_bucket_name(database))
            .await
            .expect("start minio test container");
        wait_for_object_store_ready(&ObjectStoreEnv::from_minio(&minio)).await;
        let pool = connect_with_retry(timescale.url()).await;
        seed_cpi_reference_data(&pool).await;
        let startup_file = unique_startup_file();
        let mut command = base_command(
            timescale.url(),
            Some(redis.url()),
            &startup_file,
            "http://127.0.0.1:1/rest",
        );
        command
            .env(
                "AU_KPIS_HTTP__SHUTDOWN_GRACE_PERIOD_SECS",
                shutdown_grace_secs.to_string(),
            )
            .arg("run");
        apply_object_store_env(&mut command, &ObjectStoreEnv::from_minio(&minio));
        let mut child = command.spawn().expect("spawn au-kpis-ingestion run");
        let addr = wait_for_startup_file(&startup_file, &mut child);
        Self {
            database_url: timescale.url().to_string(),
            cache_url: Some(redis.url().to_string()),
            object_store: Some(ObjectStoreEnv::from_minio(&minio)),
            addr,
            startup_file,
            child,
            pool,
            _timescale: timescale,
            _redis: Some(redis),
            _minio: Some(minio),
        }
    }

    fn with_abs_fixture(mut self, fixture_base_url: &str) -> Self {
        self.addr = fixture_base_url.to_string();
        self
    }

    fn command(&self) -> Command {
        let mut command = base_command(
            &self.database_url,
            self.cache_url.as_deref(),
            &self.startup_file,
            &self.addr,
        );
        if let Some(object_store) = &self.object_store {
            apply_object_store_env(&mut command, object_store);
        }
        command
    }

    fn pool(&self) -> &sqlx::PgPool {
        &self.pool
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
            panic!("au-kpis-ingestion exited unexpectedly: {status}");
        }
    }

    fn wait_for_exit(&mut self, within: Duration) -> Duration {
        let started = Instant::now();
        let deadline = Instant::now() + within;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child") {
                assert!(
                    status.success(),
                    "au-kpis-ingestion exited unsuccessfully: {status}"
                );
                return started.elapsed();
            }

            assert!(
                Instant::now() < deadline,
                "au-kpis-ingestion did not exit within {}s of SIGTERM",
                within.as_secs()
            );
            thread::sleep(Duration::from_millis(100));
        }
    }
}

#[derive(Clone, Debug)]
struct ObjectStoreEnv {
    endpoint: String,
    bucket: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
    allow_http: String,
}

impl ObjectStoreEnv {
    fn from_minio(minio: &MinioHarness) -> Self {
        Self {
            endpoint: minio.endpoint().to_string(),
            bucket: minio.bucket().to_string(),
            access_key_id: minio.access_key().to_string(),
            secret_access_key: minio.secret_key().to_string(),
            region: "us-east-1".to_string(),
            allow_http: "true".to_string(),
        }
    }
}

impl Drop for IngestionProcess {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.startup_file);
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn base_command(
    database_url: &str,
    cache_url: Option<&str>,
    startup_file: &PathBuf,
    abs_base_url: &str,
) -> Command {
    let mut command = Command::new(cargo_bin("au-kpis-ingestion"));
    command
        .env("AU_KPIS_HTTP__BIND", "127.0.0.1:0")
        .env("AU_KPIS_DATABASE__URL", database_url)
        .env("AU_KPIS_ABS_BASE_URL", abs_base_url)
        .env("AU_KPIS_STARTUP_NOTIFY_FILE", startup_file)
        .env("LLVM_PROFILE_FILE", ignored_child_coverage_profile())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    clear_object_store_env(&mut command);
    if let Some(cache_url) = cache_url {
        command.env("AU_KPIS_CACHE__URL", cache_url);
    }
    command
}

fn object_store_env() -> ObjectStoreEnv {
    ObjectStoreEnv {
        endpoint: "http://127.0.0.1:9000".to_string(),
        bucket: "ambient-object-store".to_string(),
        access_key_id: "ambient-key".to_string(),
        secret_access_key: "ambient-secret".to_string(),
        region: "us-east-1".to_string(),
        allow_http: "true".to_string(),
    }
}

fn minio_bucket_name(database: &str) -> String {
    let mut bucket: String = database
        .chars()
        .map(|ch| match ch {
            'a'..='z' | '0'..='9' => ch,
            _ => '-',
        })
        .collect();
    bucket.push_str("-artifacts");
    bucket
}

async fn wait_for_object_store_ready(object_store: &ObjectStoreEnv) {
    let store = AmazonS3Builder::new()
        .with_endpoint(&object_store.endpoint)
        .with_region(&object_store.region)
        .with_bucket_name(&object_store.bucket)
        .with_access_key_id(&object_store.access_key_id)
        .with_secret_access_key(&object_store.secret_access_key)
        .with_allow_http(object_store.allow_http == "true")
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("build AmazonS3 client");
    let probe = Path::from("readiness-probe");

    for _ in 0..20 {
        // Mirror the storage crate's own MinIO readiness check. The container
        // can accept metadata requests before the full signed object path is
        // ready, so exercise a write/read/delete cycle instead of `head` only.
        if store.put(&probe, b"ready".to_vec().into()).await.is_ok()
            && store.head(&probe).await.is_ok()
            && matches!(
                store.delete(&probe).await,
                Ok(()) | Err(ObjectStoreError::NotFound { .. })
            )
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    panic!("object store did not become ready within the retry window");
}

fn clear_object_store_env(command: &mut Command) {
    for key in [
        "AU_KPIS_OBJECT_STORE__ENDPOINT",
        "AU_KPIS_OBJECT_STORE__BUCKET",
        "AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID",
        "AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY",
        "AU_KPIS_OBJECT_STORE__REGION",
        "AU_KPIS_OBJECT_STORE__ALLOW_HTTP",
    ] {
        command.env_remove(key);
    }
}

fn apply_object_store_env(command: &mut Command, object_store: &ObjectStoreEnv) {
    command
        .env("AU_KPIS_OBJECT_STORE__ENDPOINT", &object_store.endpoint)
        .env("AU_KPIS_OBJECT_STORE__BUCKET", &object_store.bucket)
        .env(
            "AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID",
            &object_store.access_key_id,
        )
        .env(
            "AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY",
            &object_store.secret_access_key,
        )
        .env("AU_KPIS_OBJECT_STORE__REGION", &object_store.region)
        .env("AU_KPIS_OBJECT_STORE__ALLOW_HTTP", &object_store.allow_http);
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

async fn seed_cpi_reference_data(pool: &sqlx::PgPool) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au', NULL)
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(pool)
    .await
    .expect("insert source");

    sqlx::query(
        "INSERT INTO measures (id, name, description, unit, scale)
         VALUES ('index', 'CPI index', NULL, 'index', NULL)
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(pool)
    .await
    .expect("insert measure");

    sqlx::query(
        "INSERT INTO dataflows (
             id, source_id, name, description, dimensions, measures,
             frequency, license, attribution, source_url
         )
         VALUES (
             'abs.cpi', 'abs', 'Consumer Price Index', NULL,
             ARRAY['region', 'measure'], ARRAY['index'], 'quarterly', 'CC-BY-4.0',
             'Source: Australian Bureau of Statistics',
             'https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia'
         )
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(pool)
    .await
    .expect("insert dataflow");
}

fn unique_startup_file() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "au-kpis-ingestion-startup-{}-{}.txt",
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
        "au-kpis-ingestion-child-coverage-{}-%p-%m.profraw",
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

    panic!("au-kpis-ingestion never reported its bound address");
}

fn http_get(addr: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect to ingestion metrics server");
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
