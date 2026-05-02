use std::time::Duration;

use au_kpis_adapter::{AdapterHttpClient, Adapters, SourceAdapter};
use au_kpis_adapter_abs::AbsAdapter;
use au_kpis_config::DatabaseConfig;
use au_kpis_db::{connect, migrate};
use au_kpis_domain::ids::SourceId;
use au_kpis_ingestion_core::{IngestionPipeline, PipelineContexts, PipelineOptions, fetch_ctx};
use au_kpis_storage::BlobStore;
use au_kpis_testing::timescale::start_timescale;
use chrono::{TimeZone, Utc};
use object_store::memory::InMemory;
use sqlx::PgPool;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;

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

const CPI_FIXTURE: &[u8] = include_bytes!("../../adapters/abs/tests/fixtures/cpi_sdmx.json");

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

async fn seed_cpi_reference_data(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au', NULL)",
    )
    .execute(pool)
    .await
    .expect("insert source");

    sqlx::query(
        "INSERT INTO measures (id, name, description, unit, scale)
         VALUES ('index', 'CPI index', NULL, 'index', NULL)",
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
         )",
    )
    .execute(pool)
    .await
    .expect("insert dataflow");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_discovers_fetches_parses_and_loads_cpi_fixture() {
    let timescale = start_timescale("au_kpis_pipeline_test")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    seed_cpi_reference_data(&pool).await;

    let base_url = serve_abs_cpi_once().await;
    let adapter = AbsAdapter::builder().base_url(base_url).build();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let mut adapters = Adapters::builder();
    adapters.register(adapter).expect("register ABS adapter");
    let adapters = adapters.build();

    let started_at = Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap();
    let blob_store = BlobStore::new(InMemory::new());
    let contexts = PipelineContexts {
        discovery: au_kpis_adapter::DiscoveryCtx::new(http.clone(), started_at),
        fetch: fetch_ctx(http.clone(), blob_store.clone(), started_at, pool.clone()),
        parse: au_kpis_adapter::ParseCtx::new(http, blob_store, started_at),
    };

    let stats = IngestionPipeline::new(adapters, pool.clone())
        .with_options(PipelineOptions {
            channel_capacity: 2,
            ..PipelineOptions::default()
        })
        .run_source(
            SourceId::new("abs").expect("static source id is valid"),
            contexts,
            CancellationToken::new(),
        )
        .await
        .expect("run CPI pipeline");

    assert_eq!(stats.discovered, 1);
    assert_eq!(stats.fetched, 1);
    assert_eq!(stats.parsed, 6);
    assert_eq!(stats.loaded.observations_loaded, 6);
    assert_eq!(stats.loaded.series_upserted, 2);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 6);

    let latest_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations_latest")
        .fetch_one(&pool)
        .await
        .expect("count latest observations");
    assert_eq!(latest_count, 6);
}
