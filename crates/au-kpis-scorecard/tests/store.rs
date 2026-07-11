use std::time::Duration;

use au_kpis_db::migrate;
use au_kpis_scorecard::{
    ApsCorrection, HistoryView, PublicationState, load_aps_history, load_aps_snapshot,
    load_latest_aps_snapshot, materialize_aps_snapshot,
};
use au_kpis_testing::timescale::start_timescale;
use chrono::NaiveDate;
use sqlx::postgres::PgPoolOptions;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daily_publication_is_idempotent_and_corrections_are_append_only() {
    let timescale = start_timescale("au_kpis_scorecard_store")
        .await
        .expect("start Timescale test container");
    let mut last_error = None;
    let mut pool = None;
    for _ in 0..10 {
        match PgPoolOptions::new()
            .max_connections(4)
            .connect(timescale.url())
            .await
        {
            Ok(connected) => {
                pool = Some(connected);
                break;
            }
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    let pool =
        pool.unwrap_or_else(|| panic!("Timescale did not accept connections: {last_error:?}"));
    migrate(&pool).await.expect("apply migrations");

    let snapshot_date = NaiveDate::from_ymd_opt(2026, 6, 22).expect("valid date");
    let first = materialize_aps_snapshot(&pool, snapshot_date, None)
        .await
        .expect("materialize no-data publication");
    assert_eq!(
        first.publication_state,
        PublicationState::InsufficientCoverage
    );
    assert_eq!(first.score, None);
    assert_eq!(first.zone, None);
    assert_eq!(first.coverage_pct, 0.0);
    assert_eq!(first.revision, 0);

    let replay = materialize_aps_snapshot(&pool, snapshot_date, None)
        .await
        .expect("replay daily materialization");
    assert_eq!(replay.id, first.id);
    assert_eq!(replay.published_at, first.published_at);

    let corrected = materialize_aps_snapshot(
        &pool,
        snapshot_date,
        Some(ApsCorrection {
            supersedes_snapshot_id: first.id,
            reason: "re-run after reviewed source correction".to_string(),
        }),
    )
    .await
    .expect("append correction");
    assert_eq!(corrected.revision, 1);
    assert_eq!(corrected.supersedes_snapshot_id, Some(first.id));
    assert_ne!(corrected.id, first.id);

    let as_published = load_aps_history(
        &pool,
        HistoryView::AsPublished,
        snapshot_date,
        snapshot_date,
        10,
    )
    .await
    .expect("load as-published history");
    let latest = load_aps_history(&pool, HistoryView::Latest, snapshot_date, snapshot_date, 10)
        .await
        .expect("load corrected history");
    assert_eq!(
        as_published.iter().map(|row| row.id).collect::<Vec<_>>(),
        [first.id]
    );
    assert_eq!(
        latest.iter().map(|row| row.id).collect::<Vec<_>>(),
        [corrected.id]
    );
    assert_eq!(
        load_latest_aps_snapshot(&pool, HistoryView::AsPublished)
            .await
            .expect("load original latest")
            .expect("original latest exists")
            .id,
        first.id
    );
    assert_eq!(
        load_latest_aps_snapshot(&pool, HistoryView::Latest)
            .await
            .expect("load corrected latest")
            .expect("corrected latest exists")
            .id,
        corrected.id
    );
    assert_eq!(
        load_aps_snapshot(&pool, first.id)
            .await
            .expect("load snapshot by id")
            .expect("snapshot exists")
            .id,
        first.id
    );

    let mutation = sqlx::query(
        "UPDATE scorecard_snapshots SET publication_state = publication_state WHERE id = $1",
    )
    .bind(first.id)
    .execute(&pool)
    .await
    .expect_err("published snapshots must be immutable");
    assert!(mutation.to_string().contains("immutable"));
}
