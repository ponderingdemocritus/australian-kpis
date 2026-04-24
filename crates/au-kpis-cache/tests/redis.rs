use std::time::Duration;

use au_kpis_cache::{CacheClient, TokenBucketConfig};
use au_kpis_testing::redis::start_redis;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Session {
    user_id: u64,
    scopes: Vec<String>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_round_trip_hits_real_redis() {
    let redis = start_redis().await.expect("start redis container");
    let client = CacheClient::connect(redis.url())
        .await
        .expect("connect cache");
    let key = "cache:test:session";
    let value = Session {
        user_id: 7,
        scopes: vec!["read".into(), "write".into()],
    };

    client
        .set_json(key, &value, Duration::from_secs(30))
        .await
        .expect("set session");

    let got: Option<Session> = client.get_json(key).await.expect("get session");
    assert_eq!(got, Some(value));

    let deleted = client.delete(key).await.expect("delete session");
    assert!(deleted, "delete should report an existing key");
    assert!(
        client
            .get_json::<Session>(key)
            .await
            .expect("get deleted")
            .is_none(),
        "deleted key should miss",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_bucket_denies_then_refills() {
    let redis = start_redis().await.expect("start redis container");
    let client = CacheClient::connect(redis.url())
        .await
        .expect("connect cache");
    let key = "cache:test:ratelimit:refill";
    let config = TokenBucketConfig::new(2, 2, Duration::from_secs(1)).expect("bucket config");

    let first = client
        .take_token_bucket(key, config, 1)
        .await
        .expect("first permit");
    assert!(first.allowed);
    assert_eq!(first.remaining, 1);

    let second = client
        .take_token_bucket(key, config, 1)
        .await
        .expect("second permit");
    assert!(second.allowed);
    assert_eq!(second.remaining, 0);

    let denied = client
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
            let decision = client
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
    let redis = start_redis().await.expect("start redis container");
    let client = CacheClient::connect(redis.url())
        .await
        .expect("connect cache");
    let key = "cache:test:ratelimit:hourly-shape";
    let config = TokenBucketConfig::new(1, 1, Duration::from_secs(2)).expect("bucket config");

    let first = client
        .take_token_bucket(key, config, 1)
        .await
        .expect("first permit");
    assert!(first.allowed);

    let denied = client
        .take_token_bucket(key, config, 1)
        .await
        .expect("second permit");
    assert!(!denied.allowed);

    let refilled = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let decision = client
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
    let redis = start_redis().await.expect("start redis container");
    let client = CacheClient::connect(redis.url())
        .await
        .expect("connect cache");
    let key = "cache:test:ratelimit:atomic";
    let client = client.clone();

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
