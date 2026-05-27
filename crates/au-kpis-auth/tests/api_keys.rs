use std::sync::Arc;

use au_kpis_auth::{ApiKeyManager, AuthError, CreateApiKeyRequest};
use au_kpis_cache::CacheClient;
use au_kpis_config::DatabaseConfig;
use au_kpis_db::{connect, migrate};
use au_kpis_testing::{
    redis::{RedisHarness, start_redis},
    timescale::{TimescaleHarness, start_timescale},
};
use chrono::{DateTime, Utc};
use sqlx::PgPool;

struct TestContext {
    _postgres: TimescaleHarness,
    _redis: RedisHarness,
    pool: PgPool,
    manager: ApiKeyManager,
}

impl TestContext {
    async fn start(database: &str) -> Self {
        let postgres = start_timescale(database)
            .await
            .expect("start timescaledb container");
        let redis = start_redis().await.expect("start redis container");
        let pool = connect(&DatabaseConfig {
            url: postgres.url().to_string(),
        })
        .await
        .expect("connect postgres");
        migrate(&pool).await.expect("apply migrations");
        let cache = Arc::new(
            CacheClient::connect(redis.url())
                .await
                .expect("connect redis"),
        );
        let manager = ApiKeyManager::new(pool.clone(), cache);

        Self {
            _postgres: postgres,
            _redis: redis,
            pool,
            manager,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_verify_revoke_hashes_keys_uses_cache_and_audits() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let ctx = TestContext::start("au_kpis_auth_roundtrip").await;
    let created = ctx
        .manager
        .create_key(CreateApiKeyRequest {
            name: "daily batch".into(),
            scopes: vec!["observations:read".into()],
            rate_limit_tier: "free".into(),
            actor: "platform-admin@example.com".into(),
        })
        .await
        .expect("create api key");

    assert!(
        created.plaintext.starts_with("auk_live_"),
        "new keys must use the leak-detection prefix"
    );

    let persisted: (String, Vec<String>, String, String) = sqlx::query_as(
        "SELECT key_hash, scopes, rate_limit_tier, row_to_json(api_keys)::text
         FROM api_keys
         WHERE id = $1",
    )
    .bind(created.id)
    .fetch_one(&ctx.pool)
    .await
    .expect("fetch stored key");

    assert!(persisted.0.starts_with("$argon2id$"));
    assert_ne!(persisted.0, created.plaintext);
    assert!(
        !persisted.3.contains(&created.plaintext),
        "plaintext key must never be persisted"
    );
    assert_eq!(persisted.1, vec!["observations:read".to_string()]);
    assert_eq!(persisted.2, "free");

    let verified = ctx
        .manager
        .verify(&created.plaintext)
        .await
        .expect("valid key verifies");
    assert_eq!(verified.id, created.id);
    assert_eq!(verified.name, "daily batch");
    assert_eq!(verified.scopes, vec!["observations:read".to_string()]);
    assert_eq!(verified.rate_limit_tier, "free");

    sqlx::query("UPDATE api_keys SET name = 'renamed in database' WHERE id = $1")
        .bind(created.id)
        .execute(&ctx.pool)
        .await
        .expect("rename stored key");
    let cached = ctx
        .manager
        .verify(&created.plaintext)
        .await
        .expect("cached key verifies");
    assert_eq!(
        cached.name, "daily batch",
        "second verification should read the Redis-cached lookup"
    );

    let invalid = ctx.manager.verify("auk_live_invalid").await;
    assert!(matches!(invalid, Err(AuthError::InvalidApiKey)));

    ctx.manager
        .revoke_key(created.id, "security-admin@example.com")
        .await
        .expect("revoke key");
    let revoked = ctx.manager.verify(&created.plaintext).await;
    assert!(matches!(revoked, Err(AuthError::InvalidApiKey)));

    let audit_rows: Vec<(String, String, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT action, actor, occurred_at, retention_until
         FROM api_key_audit_log
         WHERE api_key_id = $1
         ORDER BY occurred_at ASC",
    )
    .bind(created.id)
    .fetch_all(&ctx.pool)
    .await
    .expect("fetch audit log");

    assert_eq!(audit_rows.len(), 2);
    assert_eq!(audit_rows[0].0, "created");
    assert_eq!(audit_rows[0].1, "platform-admin@example.com");
    assert_eq!(audit_rows[1].0, "revoked");
    assert_eq!(audit_rows[1].1, "security-admin@example.com");
    for (_, _, occurred_at, retention_until) in audit_rows {
        let retention = retention_until - occurred_at;
        assert!(retention >= chrono::Duration::days(365));
        assert!(retention <= chrono::Duration::days(366));
    }
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
