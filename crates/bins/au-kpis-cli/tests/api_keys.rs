use assert_cmd::cargo::cargo_bin;
use au_kpis_config::DatabaseConfig;
use au_kpis_db::{connect, migrate};
use au_kpis_testing::{redis::start_redis, timescale::start_timescale};
use serde_json::Value;
use sqlx::PgPool;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_key_admin_commands_create_print_once_and_revoke_with_audit() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let postgres = start_timescale("au_kpis_cli_api_keys")
        .await
        .expect("start timescaledb container");
    let redis = start_redis().await.expect("start redis container");
    let pool = connect(&DatabaseConfig {
        url: postgres.url().to_string(),
    })
    .await
    .expect("connect postgres");
    migrate(&pool).await.expect("apply migrations");

    let create = cli(postgres.url(), redis.url())
        .args([
            "api-keys",
            "create",
            "--name",
            "sdk client",
            "--scope",
            "observations:read",
            "--actor",
            "platform-admin@example.com",
        ])
        .output()
        .expect("run api key create");
    assert!(
        create.status.success(),
        "create command failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert!(
        create.stderr.is_empty(),
        "plaintext key should not be echoed to stderr"
    );

    let body: Value = serde_json::from_slice(&create.stdout).expect("create json");
    let id = body["id"].as_str().expect("id");
    let plaintext = body["api_key"].as_str().expect("api_key");
    assert!(plaintext.starts_with("auk_live_"));
    assert_eq!(body["name"], "sdk client");
    assert_eq!(body["scopes"], serde_json::json!(["observations:read"]));

    let stored: String = sqlx::query_scalar("SELECT row_to_json(api_keys)::text FROM api_keys")
        .fetch_one(&pool)
        .await
        .expect("fetch stored key");
    assert!(!stored.contains(plaintext));

    let revoke = cli(postgres.url(), redis.url())
        .args([
            "api-keys",
            "revoke",
            "--id",
            id,
            "--actor",
            "security-admin@example.com",
        ])
        .output()
        .expect("run api key revoke");
    assert!(
        revoke.status.success(),
        "revoke command failed: {}",
        String::from_utf8_lossy(&revoke.stderr)
    );

    let audit_actions: Vec<String> =
        sqlx::query_scalar("SELECT action FROM api_key_audit_log ORDER BY occurred_at ASC")
            .fetch_all(&pool)
            .await
            .expect("fetch audit actions");
    assert_eq!(audit_actions, vec!["created", "revoked"]);
    assert_revoked(&pool).await;
}

fn cli(database_url: &str, cache_url: &str) -> std::process::Command {
    let mut command = std::process::Command::new(cargo_bin("au-kpis-cli"));
    command
        .env("AU_KPIS_DATABASE__URL", database_url)
        .env("AU_KPIS_CACHE__URL", cache_url);
    command
}

async fn assert_revoked(pool: &PgPool) {
    let revoked: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT revoked_at FROM api_keys LIMIT 1")
            .fetch_one(pool)
            .await
            .expect("fetch revoked_at");
    assert!(revoked.is_some());
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
