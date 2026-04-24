use std::time::Duration;

use au_kpis_cache::{CacheClient, TokenBucketConfig};
use serde::{Deserialize, Serialize};
use testcontainers::{
    ContainerAsync, GenericImage,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
};

const REDIS_IMAGE: &str = "redis";
const REDIS_TAG: &str = "7.2-alpine";
const REDIS_PORT: u16 = 6379;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Session {
    user_id: u64,
    scopes: Vec<String>,
}

struct Harness {
    _container: ContainerAsync<GenericImage>,
    client: CacheClient,
}

async fn start_redis() -> Harness {
    let container = GenericImage::new(REDIS_IMAGE, REDIS_TAG)
        .with_exposed_port(ContainerPort::Tcp(REDIS_PORT))
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .expect("start redis container");

    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("host port");
    let url = format!("redis://{host}:{port}");

    let client = CacheClient::connect(&url).await.expect("connect cache");

    Harness {
        _container: container,
        client,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_round_trip_hits_real_redis() {
    let harness = start_redis().await;
    let key = "cache:test:session";
    let value = Session {
        user_id: 7,
        scopes: vec!["read".into(), "write".into()],
    };

    harness
        .client
        .set_json(key, &value, Duration::from_secs(30))
        .await
        .expect("set session");

    let got: Option<Session> = harness.client.get_json(key).await.expect("get session");
    assert_eq!(got, Some(value));

    let deleted = harness.client.delete(key).await.expect("delete session");
    assert!(deleted, "delete should report an existing key");
    assert!(
        harness
            .client
            .get_json::<Session>(key)
            .await
            .expect("get deleted")
            .is_none(),
        "deleted key should miss",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_bucket_denies_then_refills() {
    let harness = start_redis().await;
    let key = "cache:test:ratelimit:refill";
    let config = TokenBucketConfig::new(2, 2, Duration::from_secs(1)).expect("bucket config");

    let first = harness
        .client
        .take_token_bucket(key, config, 1)
        .await
        .expect("first permit");
    assert!(first.allowed);
    assert_eq!(first.remaining, 1);

    let second = harness
        .client
        .take_token_bucket(key, config, 1)
        .await
        .expect("second permit");
    assert!(second.allowed);
    assert_eq!(second.remaining, 0);

    let denied = harness
        .client
        .take_token_bucket(key, config, 1)
        .await
        .expect("third permit");
    assert!(
        !denied.allowed,
        "third request should be denied before the bucket refills"
    );
    assert_eq!(denied.remaining, 0);
    assert!(denied.retry_after > Duration::ZERO);

    let after_refill = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let decision = harness
                .client
                .take_token_bucket(key, config, 1)
                .await
                .expect("refill probe");
            if decision.allowed {
                return decision;
            }
        }
    })
    .await
    .expect("bucket should refill within timeout");
    assert!(after_refill.allowed, "bucket should refill after waiting");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_bucket_supports_sub_one_token_per_second_refill() {
    let harness = start_redis().await;
    let key = "cache:test:ratelimit:hourly-shape";
    let config = TokenBucketConfig::new(1, 1, Duration::from_secs(2)).expect("bucket config");

    let first = harness
        .client
        .take_token_bucket(key, config, 1)
        .await
        .expect("first permit");
    assert!(first.allowed);

    let denied = harness
        .client
        .take_token_bucket(key, config, 1)
        .await
        .expect("second permit");
    assert!(!denied.allowed);

    let refilled = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let decision = harness
                .client
                .take_token_bucket(key, config, 1)
                .await
                .expect("refill probe");
            if decision.allowed {
                return decision;
            }
        }
    })
    .await
    .expect("sub-one-token-per-second bucket should refill");
    assert!(refilled.allowed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_bucket_is_atomic_under_contention() {
    let harness = start_redis().await;
    let key = "cache:test:ratelimit:atomic";
    let client = harness.client.clone();

    let mut tasks = Vec::new();
    for _ in 0..10 {
        let client = client.clone();
        let config = TokenBucketConfig::new(3, 1, Duration::from_secs(1)).expect("bucket config");
        tasks.push(tokio::spawn(async move {
            client.take_token_bucket(key, config, 1).await
        }));
    }

    let mut granted = 0;
    for task in tasks {
        let decision = task.await.expect("join").expect("rate limit decision");
        if decision.allowed {
            granted += 1;
        }
    }

    assert_eq!(granted, 3, "only bucket capacity should be granted");
}
