use std::{collections::BTreeMap, time::Duration};

use au_kpis_config::DatabaseConfig;
use au_kpis_db::{
    ArtifactLoadCompletion, connect, get_artifact, migrate, record_artifact_load_completion,
    repair_artifact_storage_key, upsert_artifact, upsert_artifact_record,
};
use au_kpis_domain::{Artifact, ArtifactId, SourceId};
use au_kpis_error::{Classify, ErrorClass};
use au_kpis_testing::timescale::start_timescale;
use chrono::{TimeZone, Utc};
use sqlx::PgPool;

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

async fn seed_source(pool: &PgPool, id: &str, name: &str) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(id)
    .bind(name)
    .bind(format!("https://{id}.example.test"))
    .bind("Test source")
    .execute(pool)
    .await
    .expect("seed source");
}

async fn seed_dataflow(pool: &PgPool, id: &str, source_id: &str) {
    sqlx::query(
        "INSERT INTO dataflows
            (id, source_id, name, dimensions, measures, frequency, license, attribution, source_url)
         VALUES
            ($1, $2, $3, ARRAY['region'], ARRAY['value'], 'monthly',
             'test', 'test attribution', $4)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(id)
    .bind(source_id)
    .bind(format!("{source_id} fixture"))
    .bind(format!("https://{source_id}.example.test/{id}"))
    .execute(pool)
    .await
    .expect("seed dataflow");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_artifact_persists_first_seen_provenance() {
    let timescale = start_timescale("au_kpis_test")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    seed_abs_source(&pool).await;

    let id = ArtifactId::of_content(b"sdmx-json");
    let fetched_at = Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap();
    let artifact = Artifact {
        id,
        fetch_id: None,
        source_id: SourceId::new("abs").unwrap(),
        source_url: "https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all".into(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::from([
            ("etag".to_string(), vec!["\"fixture-etag\"".to_string()]),
            (
                "last-modified".to_string(),
                vec!["Wed, 29 Apr 2026 00:00:00 GMT".to_string()],
            ),
            (
                "x-audit".to_string(),
                vec!["first".to_string(), "second".to_string()],
            ),
        ]),
        size_bytes: 9,
        storage_key: format!("artifacts/{}", id.to_hex()),
        fetched_at,
    };

    upsert_artifact(&pool, &artifact)
        .await
        .expect("upsert artifact");

    let later_duplicate = Artifact {
        source_url: "https://mirror.example.invalid/rest/data/ABS,CPI,2.0.0/all".into(),
        content_type: "application/octet-stream".into(),
        response_headers: BTreeMap::from([(
            "etag".to_string(),
            vec!["\"mirror-etag\"".to_string()],
        )]),
        size_bytes: 99,
        storage_key: "artifacts/mirror-copy".into(),
        fetched_at: Utc.with_ymd_and_hms(2026, 4, 30, 0, 0, 0).unwrap(),
        ..artifact.clone()
    };
    upsert_artifact(&pool, &later_duplicate)
        .await
        .expect("duplicate artifact records fetch provenance");
    let returned = upsert_artifact_record(&pool, &later_duplicate)
        .await
        .expect("duplicate artifact returns current fetch row");

    let stored = get_artifact(&pool, id)
        .await
        .expect("load artifact")
        .expect("artifact row exists");

    assert_eq!(stored, artifact);
    assert_eq!(returned.id, later_duplicate.id);
    assert_eq!(returned.source_id, later_duplicate.source_id);
    assert_eq!(returned.source_url, later_duplicate.source_url);
    assert_eq!(returned.content_type, later_duplicate.content_type);
    assert_eq!(returned.response_headers, later_duplicate.response_headers);
    assert_eq!(returned.storage_key, later_duplicate.storage_key);
    assert!(returned.fetch_id.is_some());

    let repaired = Artifact {
        storage_key: format!("artifacts/repaired-{}", id.to_hex()),
        ..stored.clone()
    };
    let returned = repair_artifact_storage_key(&pool, &repaired, &stored.storage_key)
        .await
        .expect("repair storage key");
    let stored = get_artifact(&pool, id)
        .await
        .expect("load repaired artifact")
        .expect("artifact row exists");
    assert_eq!(stored.storage_key, repaired.storage_key);
    assert_eq!(returned.storage_key, repaired.storage_key);

    let returned = repair_artifact_storage_key(&pool, &repaired, &artifact.storage_key)
        .await
        .expect("already repaired storage key is idempotent");
    assert_eq!(returned.storage_key, repaired.storage_key);

    let stale_repair = Artifact {
        storage_key: "artifacts/stale-repair".into(),
        ..stored.clone()
    };
    let err = repair_artifact_storage_key(&pool, &stale_repair, "artifacts/missing-cold")
        .await
        .expect_err("stale compare-and-set repair should fail");
    assert_eq!(err.class(), ErrorClass::Validation);

    let legacy_id = ArtifactId::of_content(b"legacy-sdmx-json");
    let legacy = Artifact {
        id: legacy_id,
        fetch_id: None,
        response_headers: BTreeMap::new(),
        storage_key: format!("artifacts/{}", legacy_id.to_hex()),
        ..artifact.clone()
    };
    upsert_artifact_record(&pool, &legacy)
        .await
        .expect("insert legacy artifact");
    let legacy_refetch = Artifact {
        response_headers: BTreeMap::from([(
            "etag".to_string(),
            vec!["\"legacy-etag\"".to_string()],
        )]),
        ..legacy.clone()
    };
    let returned = upsert_artifact_record(&pool, &legacy_refetch)
        .await
        .expect("backfill empty response headers on duplicate");
    assert_eq!(returned.response_headers, legacy_refetch.response_headers);

    let missing_source_id = ArtifactId::of_content(b"missing-source-sdmx-json");
    let missing_source = Artifact {
        id: missing_source_id,
        fetch_id: None,
        source_id: SourceId::new("missing-source").unwrap(),
        storage_key: format!("artifacts/{}", missing_source_id.to_hex()),
        ..artifact
    };
    let err = upsert_artifact_record(&pool, &missing_source)
        .await
        .expect_err("missing source attribution should fail permanently");
    assert_eq!(err.class(), ErrorClass::Validation);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_content_records_each_fetch_provenance() {
    let timescale = start_timescale("au_kpis_artifact_fetches")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    seed_source(&pool, "abs", "Australian Bureau of Statistics").await;
    seed_source(&pool, "rba", "Reserve Bank of Australia").await;
    seed_dataflow(&pool, "abs.cpi", "abs").await;
    seed_dataflow(&pool, "rba.cpi", "rba").await;

    let id = ArtifactId::of_content(b"same-upstream-bytes");
    let first = Artifact {
        id,
        fetch_id: None,
        source_id: SourceId::new("abs").unwrap(),
        source_url: "https://data.api.abs.gov.au/rest/data/CPI".into(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::from([("etag".to_string(), vec!["\"abs\"".to_string()])]),
        size_bytes: 19,
        storage_key: format!("artifacts/{}", id.to_hex()),
        fetched_at: Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
    };
    let second = Artifact {
        source_id: SourceId::new("rba").unwrap(),
        source_url: "https://rba.example.test/mirror/cpi.json".into(),
        response_headers: BTreeMap::from([("etag".to_string(), vec!["\"rba\"".to_string()])]),
        fetched_at: Utc.with_ymd_and_hms(2026, 4, 30, 0, 0, 0).unwrap(),
        ..first.clone()
    };

    let first = upsert_artifact_record(&pool, &first)
        .await
        .expect("record first fetch");
    let second = upsert_artifact_record(&pool, &second)
        .await
        .expect("record second fetch");

    assert_eq!(first.id, id);
    assert_eq!(second.id, id);
    assert_ne!(
        first.fetch_id, second.fetch_id,
        "each upstream retrieval needs its own fetch provenance row"
    );

    let artifact_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM artifacts WHERE id = $1")
        .bind(id.digest().as_bytes().as_slice())
        .fetch_one(&pool)
        .await
        .expect("count blob rows");
    assert_eq!(artifact_rows, 1);

    let fetches: Vec<(String, String)> = sqlx::query_as(
        "SELECT source_id, source_url
         FROM artifact_fetches
         WHERE artifact_id = $1
         ORDER BY id",
    )
    .bind(id.digest().as_bytes().as_slice())
    .fetch_all(&pool)
    .await
    .expect("read fetch provenance rows");
    assert_eq!(
        fetches,
        vec![
            (
                "abs".to_string(),
                "https://data.api.abs.gov.au/rest/data/CPI".to_string()
            ),
            (
                "rba".to_string(),
                "https://rba.example.test/mirror/cpi.json".to_string()
            ),
        ]
    );

    record_artifact_load_completion(
        &pool,
        ArtifactLoadCompletion {
            artifact_id: id,
            artifact_fetch_id: second.fetch_id,
            source_id: &SourceId::new("rba").unwrap(),
            dataflow_id: &au_kpis_domain::DataflowId::new("rba.cpi").unwrap(),
            observations_parsed: 2,
            observations_loaded: 2,
            job_id: Some("job-rba-mirror"),
            trace_parent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        },
    )
    .await
    .expect("record artifact load");

    let load_fetch: Option<i64> = sqlx::query_scalar(
        "SELECT artifact_fetch_id
         FROM artifact_loads
         WHERE artifact_id = $1 AND source_id = 'rba' AND dataflow_id = 'rba.cpi'",
    )
    .bind(id.digest().as_bytes().as_slice())
    .fetch_one(&pool)
    .await
    .expect("read load fetch provenance");
    assert_eq!(load_fetch, second.fetch_id);
}
