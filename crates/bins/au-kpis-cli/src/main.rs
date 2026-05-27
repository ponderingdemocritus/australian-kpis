//! Admin CLI (migrations, backfills).

use std::sync::Arc;

use anyhow::Context;
use au_kpis_auth::{ApiKeyManager, CreateApiKeyRequest};
use au_kpis_cache::CacheClient;
use au_kpis_config::load;
use au_kpis_db::connect as connect_db;
use clap::{Parser, Subcommand};
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "au-kpis-cli")]
#[command(about = "Administrative tools for australian-kpis")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(name = "api-keys")]
    ApiKeys {
        #[command(subcommand)]
        command: ApiKeyCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ApiKeyCommand {
    Create {
        #[arg(long)]
        name: String,
        #[arg(long = "scope")]
        scopes: Vec<String>,
        #[arg(long, default_value = "free")]
        rate_limit_tier: String,
        #[arg(long)]
        actor: String,
    },
    Revoke {
        #[arg(long)]
        id: Uuid,
        #[arg(long)]
        actor: String,
    },
}

#[derive(Debug, Serialize)]
struct CreatedApiKeyOutput {
    id: Uuid,
    name: String,
    scopes: Vec<String>,
    rate_limit_tier: String,
    api_key: String,
}

#[derive(Debug, Serialize)]
struct RevokedApiKeyOutput {
    id: Uuid,
    revoked: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = load(None).context("load config")?;
    let db = connect_db(&config.database)
        .await
        .context("connect postgres database")?;
    let cache = Arc::new(
        CacheClient::connect(&config.cache.url)
            .await
            .context("connect redis cache")?,
    );
    let manager = ApiKeyManager::new(db, cache);

    match cli.command {
        Commands::ApiKeys { command } => match command {
            ApiKeyCommand::Create {
                name,
                scopes,
                rate_limit_tier,
                actor,
            } => {
                let created = manager
                    .create_key(CreateApiKeyRequest {
                        name,
                        scopes,
                        rate_limit_tier,
                        actor,
                    })
                    .await
                    .context("create api key")?;
                print_json(&CreatedApiKeyOutput {
                    id: created.id,
                    name: created.name,
                    scopes: created.scopes,
                    rate_limit_tier: created.rate_limit_tier,
                    api_key: created.plaintext,
                })?;
            }
            ApiKeyCommand::Revoke { id, actor } => {
                manager
                    .revoke_key(id, &actor)
                    .await
                    .context("revoke api key")?;
                print_json(&RevokedApiKeyOutput { id, revoked: true })?;
            }
        },
    }

    Ok(())
}

fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(std::io::stdout(), value).context("write json output")?;
    println!();
    Ok(())
}
