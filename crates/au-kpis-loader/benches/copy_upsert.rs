use std::{
    collections::BTreeMap,
    time::{Duration as StdDuration, Instant},
};

use au_kpis_config::DatabaseConfig;
use au_kpis_db::{connect, migrate};
use au_kpis_domain::{
    Observation, ObservationStatus, SeriesDescriptor, TimePrecision,
    ids::{ArtifactId, CodeId, DataflowId, DimensionId, MeasureId, SeriesKey},
};
use au_kpis_loader::{LoadItem, LoadOptions, load_batch_with_options};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use sqlx::PgPool;

const ROWS_PER_BATCH: usize = 10_000;

struct BenchDb {
    pool: PgPool,
    _timescale: au_kpis_testing::timescale::TimescaleHarness,
}

async fn bench_db() -> BenchDb {
    let timescale = au_kpis_testing::timescale::start_timescale("au_kpis_loader_bench")
        .await
        .expect("start timescaledb benchmark container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };

    let mut last_err = None;
    for _ in 0..10 {
        match connect(&cfg).await {
            Ok(pool) => {
                migrate(&pool).await.expect("apply migrations");
                seed_reference_data(&pool).await;
                return BenchDb {
                    pool,
                    _timescale: timescale,
                };
            }
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(StdDuration::from_millis(500)).await;
            }
        }
    }
    panic!("timescaledb did not accept benchmark connections: {last_err:?}");
}

async fn seed_reference_data(pool: &PgPool) {
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
             ARRAY['region'], ARRAY['index'], 'quarterly', 'CC-BY-4.0',
             'Source: Australian Bureau of Statistics',
             'https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia'
         )",
    )
    .execute(pool)
    .await
    .expect("insert dataflow");

    let artifact = ArtifactId::of_content(b"loader copy benchmark artifact");
    sqlx::query(
        "INSERT INTO artifacts (
             id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         )
         VALUES ($1, 'abs', 'https://example.test/cpi.json', 'application/json',
                 '{}'::jsonb, 128, $2, $3)",
    )
    .bind(artifact.digest().as_bytes().as_slice())
    .bind(format!("artifacts/{artifact}"))
    .bind(Utc.with_ymd_and_hms(2024, 4, 24, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert artifact");
}

fn descriptor() -> SeriesDescriptor {
    let dataflow_id = DataflowId::new("abs.cpi").expect("static dataflow id is valid");
    let measure_id = MeasureId::new("index").expect("static measure id is valid");
    let dimensions: BTreeMap<DimensionId, CodeId> = [(
        DimensionId::new("region").expect("static dimension id is valid"),
        CodeId::new("AUS").expect("static code id is valid"),
    )]
    .into_iter()
    .collect();
    let series_key = SeriesKey::derive(
        &dataflow_id,
        &measure_id,
        dimensions
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );

    SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id,
        dimensions,
        unit: "index".to_string(),
    }
}

fn load_items(case: u64, descriptor: &SeriesDescriptor, artifact: ArtifactId) -> Vec<LoadItem> {
    let base =
        Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap() + ChronoDuration::days(case as i64);
    (0..ROWS_PER_BATCH)
        .map(|idx| {
            let time = base + ChronoDuration::seconds(idx as i64);
            LoadItem {
                series: descriptor.clone(),
                observation: Observation {
                    series_key: descriptor.series_key,
                    time,
                    time_precision: TimePrecision::Day,
                    value: Some(100.0 + f64::from(idx as u32) / 100.0),
                    status: ObservationStatus::Normal,
                    revision_no: 0,
                    attributes: BTreeMap::new(),
                    ingested_at: Utc.with_ymd_and_hms(2024, 4, 24, 0, 0, 0).unwrap(),
                    source_artifact_id: artifact,
                },
            }
        })
        .collect()
}

fn bench_loader_copy_upsert(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let db = runtime.block_on(bench_db());
    let pool = db.pool.clone();
    let descriptor = descriptor();
    let artifact = ArtifactId::of_content(b"loader copy benchmark artifact");
    let options = LoadOptions {
        max_rows: ROWS_PER_BATCH,
        max_bytes: 10 * 1024 * 1024,
    };
    let mut case = 0_u64;

    let mut group = c.benchmark_group("loader_copy");
    group.throughput(Throughput::Elements(ROWS_PER_BATCH as u64));
    group.bench_function("copy_upsert_10k_rows_under_500ms", |b| {
        b.iter_custom(|iters| {
            runtime.block_on(async {
                let mut elapsed = StdDuration::ZERO;
                for _ in 0..iters {
                    let items = load_items(case, &descriptor, artifact);
                    case += 1;
                    let started = Instant::now();
                    let stats = load_batch_with_options(&pool, items, options)
                        .await
                        .expect("load 10k benchmark rows");
                    elapsed += started.elapsed();
                    assert_eq!(stats.observations_loaded, ROWS_PER_BATCH as u64);
                    assert_eq!(stats.parse_errors, 0);
                    black_box(stats);
                }
                elapsed
            })
        });
    });
    group.finish();

    drop(pool);
    runtime.block_on(async move {
        drop(db);
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(StdDuration::from_secs(1))
        .measurement_time(StdDuration::from_secs(3))
        .sample_size(10);
    targets = bench_loader_copy_upsert
}
criterion_main!(benches);
