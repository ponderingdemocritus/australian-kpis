use std::time::Duration;

use au_kpis_adapter::{AdapterHttpClient, SourceAdapter};
use au_kpis_adapter_abs::AbsAdapter;
use au_kpis_config::DatabaseConfig;
use au_kpis_db::{PgPool, connect, get_artifact, migrate};
use au_kpis_ingestion_core::fetch_ctx;
use au_kpis_storage::BlobStore;
use au_kpis_testing::timescale::start_timescale;
use chrono::{TimeZone, Utc};
use object_store::memory::InMemory;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const SDMX_FIXTURE: &[u8] = br#"{"data":{"dataSets":[{"observations":{"0:0":[123.4]}}]}}"#;

async fn connect_with_retry(cfg: &DatabaseConfig) -> PgPool {
    let mut last_err = None;
    for _ in 0..10 {
        match connect(cfg).await {
            Ok(pool) => return pool,
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    panic!("timescaledb did not accept connections: {last_err:?}");
}

async fn seed_abs_source(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind("abs")
    .bind("Australian Bureau of Statistics")
    .bind("https://data.api.abs.gov.au")
    .bind("Official Australian statistical agency")
    .execute(pool)
    .await
    .expect("seed ABS source");
}

async fn serve_artifact_once() -> (String, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await.expect("read request");

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/vnd.sdmx.data+json\r\netag: \"fixture-etag\"\r\nx-audit: first\r\nx-audit: second\r\ncontent-length: {}\r\n\r\n",
            SDMX_FIXTURE.len(),
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");
        stream.write_all(SDMX_FIXTURE).await.expect("write body");
    });

    (
        format!("http://{addr}/rest"),
        format!("http://{addr}/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD"),
    )
}

fn cpi_job(source_url: String) -> au_kpis_adapter::DiscoveredJob {
    let job = AbsAdapter::current_jobs(
        &AbsAdapter::parse_dataflow_listing(
            r#"{
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
            }"#,
        )
        .expect("parse dataflow listing"),
    )
    .into_iter()
    .next()
    .expect("one CPI job");

    au_kpis_adapter::DiscoveredJob { source_url, ..job }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abs_fetch_records_artifact_provenance_through_ingestion_context() {
    let timescale = start_timescale("au_kpis_fetch_test")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    seed_abs_source(&pool).await;

    let (base_url, source_url) = serve_artifact_once().await;
    let adapter = AbsAdapter::builder().base_url(base_url).build();
    let job = cpi_job(source_url);
    let started_at = Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap();
    let ctx = fetch_ctx(
        AdapterHttpClient::new(adapter.manifest().rate_limit),
        BlobStore::new(InMemory::new()),
        started_at,
        pool.clone(),
    );

    let artifact = adapter.fetch(job, &ctx).await.expect("fetch ABS artifact");
    let stored = get_artifact(&pool, artifact.id)
        .await
        .expect("load artifact")
        .expect("artifact row exists");

    assert_eq!(stored.id, artifact.id);
    assert_eq!(stored.source_id, artifact.source_id);
    assert_eq!(stored.source_url, artifact.source_url);
    assert_eq!(stored.content_type, artifact.content_type);
    assert_eq!(stored.size_bytes, artifact.size_bytes);
    assert_eq!(stored.storage_key, artifact.storage_key);
    assert_eq!(
        stored.fetched_at.timestamp_micros(),
        artifact.fetched_at.timestamp_micros()
    );
    assert_eq!(stored.response_headers["etag"], ["\"fixture-etag\""]);
    assert_eq!(stored.response_headers["x-audit"], ["first", "second"]);
}
