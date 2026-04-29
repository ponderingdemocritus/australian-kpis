use std::time::Duration;

use au_kpis_config::DatabaseConfig;
use au_kpis_db::{connect, migrate};
use au_kpis_domain::{DataflowId, SourceId};
use au_kpis_error::ErrorClass;
use au_kpis_queue::{ApalisPgQueue, CronSchedule, Job, JobKind, Nack, Queue, QueueStage, WorkerId};
use au_kpis_testing::timescale::{TimescaleHarness, start_timescale};
use sqlx::PgPool;

struct QueueDb {
    _timescale: TimescaleHarness,
    pool: PgPool,
}

async fn migrated_pool(database: &str) -> QueueDb {
    let timescale = start_timescale(database)
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };

    let mut last_err = None;
    for _ in 0..10 {
        match connect(&cfg).await {
            Ok(pool) => {
                migrate(&pool).await.expect("apply migrations");
                return QueueDb {
                    _timescale: timescale,
                    pool,
                };
            }
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    panic!("timescaledb did not accept connections: {last_err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_pop_ack_preserves_payload_and_trace_parent() {
    let pool = migrated_pool("au_kpis_queue_roundtrip").await;
    let queue = ApalisPgQueue::new(pool.pool);
    let trace_parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let job = Job::discover(SourceId::new("abs").unwrap()).with_trace_parent(trace_parent);

    let id = queue.push(job.clone()).await.expect("push job");
    let leased = queue
        .pop(QueueStage::Discover, WorkerId::new("worker-a").unwrap())
        .await
        .expect("pop job")
        .expect("job should be available");

    assert_eq!(leased.id(), id);
    assert_eq!(leased.job(), &job);
    assert_eq!(leased.trace_parent(), Some(trace_parent));
    assert_eq!(leased.attempts(), 1);
    assert_eq!(leased.lease_version(), 1);

    queue.ack(&leased).await.expect("ack job");

    let next = queue
        .pop(QueueStage::Discover, WorkerId::new("worker-a").unwrap())
        .await
        .expect("pop after ack");
    assert!(next.is_none(), "acked jobs must not be leased again");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_retries_then_dead_letters_after_attempt_budget() {
    let pool = migrated_pool("au_kpis_queue_retry").await;
    let queue = ApalisPgQueue::new(pool.pool);
    let job = Job::fetch("abs-cpi-2024-q1", SourceId::new("abs").unwrap())
        .with_max_attempts(2)
        .with_trace_parent("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");

    let id = queue.push(job.clone()).await.expect("push job");
    let first = queue
        .pop(QueueStage::Fetch, WorkerId::new("worker-a").unwrap())
        .await
        .expect("first pop")
        .expect("job should be leased");
    queue
        .nack(
            &first,
            Nack::new(ErrorClass::Transient, "upstream timeout").with_retry_after(Duration::ZERO),
        )
        .await
        .expect("nack for retry");

    let second = queue
        .pop(QueueStage::Fetch, WorkerId::new("worker-b").unwrap())
        .await
        .expect("second pop")
        .expect("job should be retried");
    assert_eq!(second.id(), id);
    assert_eq!(second.attempts(), 2);
    assert_eq!(second.lease_version(), 2);
    queue
        .nack(
            &second,
            Nack::new(ErrorClass::Transient, "still timing out"),
        )
        .await
        .expect("nack to dlq");

    let dead = queue.dead_lettered(id).await.expect("read dlq");
    assert_eq!(dead.job(), &job);
    assert_eq!(dead.attempts(), 2);
    assert!(dead.error_message().contains("still timing out"));

    sqlx::query!("DELETE FROM queue_jobs WHERE id = $1", id.get())
        .execute(queue.pool())
        .await
        .expect("delete retained queue row");
    let retained = queue.dead_lettered(id).await.expect("read retained dlq");
    assert_eq!(retained.job(), &job);
    assert!(retained.error_message().contains("still timing out"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_job_group_key_serializes_same_dataflow() {
    let pool = migrated_pool("au_kpis_queue_group").await;
    let queue = ApalisPgQueue::new(pool.pool);
    let dataflow_id = DataflowId::new("abs.cpi").unwrap();
    let worker = WorkerId::new("worker-a").unwrap();

    let first = queue
        .push(Job::load(dataflow_id.clone(), "artifact-a"))
        .await
        .expect("push first load");
    queue
        .push(Job::load(dataflow_id, "artifact-b"))
        .await
        .expect("push second load");

    let leased = queue
        .pop(QueueStage::Load, worker.clone())
        .await
        .expect("pop first")
        .expect("first load should lease");
    assert_eq!(leased.id(), first);

    let blocked = queue
        .pop(QueueStage::Load, worker.clone())
        .await
        .expect("pop while same group is running");
    assert!(
        blocked.is_none(),
        "same dataflow load jobs must not run concurrently"
    );

    queue.ack(&leased).await.expect("ack first load");
    let next = queue
        .pop(QueueStage::Load, worker)
        .await
        .expect("pop second")
        .expect("second load should become available after ack");
    assert_ne!(next.id(), first);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_load_pops_cannot_lease_same_dataflow_group() {
    let pool = migrated_pool("au_kpis_queue_group_race").await;
    let queue = ApalisPgQueue::new(pool.pool);
    let dataflow_id = DataflowId::new("abs.cpi").unwrap();

    queue
        .push(Job::load(dataflow_id.clone(), "artifact-a"))
        .await
        .expect("push first load");
    queue
        .push(Job::load(dataflow_id, "artifact-b"))
        .await
        .expect("push second load");

    let first_queue = queue.clone();
    let second_queue = queue.clone();
    let (first, second) = tokio::join!(
        first_queue.pop(QueueStage::Load, WorkerId::new("worker-a").unwrap()),
        second_queue.pop(QueueStage::Load, WorkerId::new("worker-b").unwrap())
    );

    let leased = [first.expect("first pop"), second.expect("second pop")]
        .into_iter()
        .filter(Option::is_some)
        .count();
    assert_eq!(leased, 1, "only one load job per dataflow may run");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_pop_skips_blocked_group_and_leases_ready_group() {
    let pool = migrated_pool("au_kpis_queue_mixed_groups").await;
    let queue = ApalisPgQueue::new(pool.pool);
    let cpi = DataflowId::new("abs.cpi").unwrap();
    let wages = DataflowId::new("abs.wpi").unwrap();

    queue
        .push(Job::load(cpi.clone(), "artifact-a").with_priority(10))
        .await
        .expect("push first CPI load");
    queue
        .push(Job::load(cpi, "artifact-b").with_priority(9))
        .await
        .expect("push blocked CPI load");
    let wages_id = queue
        .push(Job::load(wages.clone(), "artifact-c").with_priority(8))
        .await
        .expect("push WPI load");

    let first = queue
        .pop(QueueStage::Load, WorkerId::new("worker-a").unwrap())
        .await
        .expect("first pop")
        .expect("first CPI load should lease");
    let next = queue
        .pop(QueueStage::Load, WorkerId::new("worker-b").unwrap())
        .await
        .expect("second pop")
        .expect("different dataflow load should remain leaseable");

    assert_eq!(next.id(), wages_id);
    assert!(matches!(
        next.job().kind(),
        JobKind::Load { dataflow_id, .. } if dataflow_id == &wages
    ));

    queue.ack(&first).await.expect("ack CPI load");
    queue.ack(&next).await.expect("ack WPI load");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_running_jobs_are_reclaimed_after_lease_timeout() {
    let pool = migrated_pool("au_kpis_queue_reclaim").await;
    let queue = ApalisPgQueue::new(pool.pool).with_lease_timeout(Duration::from_millis(1));
    let id = queue
        .push(Job::load(DataflowId::new("abs.cpi").unwrap(), "artifact-a"))
        .await
        .expect("push load");

    let first = queue
        .pop(QueueStage::Load, WorkerId::new("worker-a").unwrap())
        .await
        .expect("first pop")
        .expect("first worker leases job");
    assert_eq!(first.id(), id);
    tokio::time::sleep(Duration::from_millis(5)).await;

    let reclaimed = queue
        .pop(QueueStage::Load, WorkerId::new("worker-b").unwrap())
        .await
        .expect("reclaim stale lease")
        .expect("stale lease should be visible");

    assert_eq!(reclaimed.id(), id);
    assert_eq!(reclaimed.worker_id().as_str(), "worker-b");
    assert_eq!(reclaimed.attempts(), 2);
    assert_eq!(reclaimed.lease_version(), first.lease_version() + 1);
    assert!(queue.ack(&first).await.is_err(), "stale handles cannot ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renew_extends_lease_and_invalidates_old_handle() {
    let pool = migrated_pool("au_kpis_queue_renew").await;
    let queue = ApalisPgQueue::new(pool.pool);
    let id = queue
        .push(Job::parse(
            SourceId::new("abs").unwrap(),
            "artifact-a",
            "raw/abs/a.pdf",
        ))
        .await
        .expect("push parse job");

    let first = queue
        .pop(QueueStage::Parse, WorkerId::new("worker-a").unwrap())
        .await
        .expect("pop parse job")
        .expect("parse job should lease");
    let renewed = queue.renew(&first).await.expect("renew lease");

    assert_eq!(renewed.id(), id);
    assert_eq!(renewed.worker_id().as_str(), "worker-a");
    assert_eq!(renewed.lease_version(), first.lease_version() + 1);
    assert!(
        renewed.leased_at() >= first.leased_at(),
        "renew should not move the lease timestamp backwards"
    );
    assert!(
        queue.ack(&first).await.is_err(),
        "old lease handles must not complete a renewed job"
    );

    queue.ack(&renewed).await.expect("ack renewed lease");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schedule_upserts_cron_registration() {
    let pool = migrated_pool("au_kpis_queue_cron").await;
    let queue = ApalisPgQueue::new(pool.pool);
    let schedule = CronSchedule::new(
        "abs-cpi-discovery",
        "0 8 * * *",
        Job::discover(SourceId::new("abs").unwrap()),
    )
    .unwrap();

    queue
        .schedule(schedule.clone())
        .await
        .expect("insert schedule");
    queue
        .schedule(schedule.with_cron_expression("0 9 * * *").unwrap())
        .await
        .expect("update schedule");

    let saved = queue
        .schedule_by_id("abs-cpi-discovery")
        .await
        .expect("read schedule")
        .expect("schedule should exist");
    assert_eq!(saved.cron_expression(), "0 9 * * *");
    assert!(matches!(saved.job().kind(), JobKind::Discover { .. }));
}
