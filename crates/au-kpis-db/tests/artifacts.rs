use std::{collections::BTreeMap, time::Duration};

use au_kpis_config::DatabaseConfig;
use au_kpis_db::{
    connect, get_artifact, migrate, repair_artifact_storage_key, upsert_artifact,
    upsert_artifact_record,
};
use au_kpis_domain::{Artifact, ArtifactId, SourceId};
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
        .expect("duplicate artifact insert is a no-op");
    let returned = upsert_artifact_record(&pool, &later_duplicate)
        .await
        .expect("duplicate artifact returns durable row");

    let stored = get_artifact(&pool, id)
        .await
        .expect("load artifact")
        .expect("artifact row exists");

    assert_eq!(stored, artifact);
    assert_eq!(returned, artifact);

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

    let stale_repair = Artifact {
        storage_key: "artifacts/stale-repair".into(),
        ..stored.clone()
    };
    let returned = repair_artifact_storage_key(&pool, &stale_repair, "artifacts/missing-cold")
        .await
        .expect("stale repair is ignored");
    assert_eq!(returned.storage_key, repaired.storage_key);

    let legacy_id = ArtifactId::of_content(b"legacy-sdmx-json");
    let legacy = Artifact {
        id: legacy_id,
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
}
