//! Admin CLI (migrations, backfills).

use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use au_kpis_auth::{ApiKeyManager, CreateApiKeyRequest};
use au_kpis_cache::CacheClient;
use au_kpis_config::load;
use au_kpis_db::connect as connect_db;
use chrono::NaiveDate;
use clap::{Parser, Subcommand};
use serde::Serialize;
use uuid::Uuid;

mod operator;

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
    /// Pause or resume source schedule admission.
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
    /// Inspect or retry durable queue state.
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },
    /// Reprocess an immutable artifact with a new parser version.
    Artifact {
        #[command(subcommand)]
        command: ArtifactCommand,
    },
    /// Inspect a durable ingestion generation.
    Generation {
        #[command(subcommand)]
        command: GenerationCommand,
    },
    /// Load reviewed canonical manual inputs.
    #[command(name = "manual-input")]
    ManualInput {
        #[command(subcommand)]
        command: ManualInputCommand,
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

#[derive(Debug, Subcommand)]
enum SourceCommand {
    Pause {
        #[arg(long)]
        dataflow: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
    Resume {
        #[arg(long)]
        dataflow: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
}

#[derive(Debug, Subcommand)]
enum QueueCommand {
    #[command(name = "retry-dlq")]
    RetryDlq {
        #[arg(long)]
        job_id: i64,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
}

#[derive(Debug, Subcommand)]
enum ArtifactCommand {
    Reparse {
        #[arg(long)]
        artifact_id: String,
        #[arg(long)]
        dataflow: String,
        #[arg(long)]
        parser_version: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
}

#[derive(Debug, Subcommand)]
enum GenerationCommand {
    Inspect {
        #[arg(long)]
        id: Uuid,
    },
}

#[derive(Debug, Subcommand)]
enum ManualInputCommand {
    Load {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        dataflow: String,
        #[arg(long)]
        source_url: String,
        #[arg(long)]
        license: String,
        #[arg(long)]
        retrieved_at: NaiveDate,
        #[arg(long)]
        reviewer_role: String,
        #[arg(long)]
        reviewed_at: NaiveDate,
        #[arg(long)]
        evidence_notes: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
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
    match cli.command {
        Commands::ApiKeys { command } => {
            let cache = Arc::new(
                CacheClient::connect(&config.cache.url)
                    .await
                    .context("connect redis cache")?,
            );
            let manager = ApiKeyManager::new(db, cache);
            match command {
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
            }
        }
        Commands::Source { command } => match command {
            SourceCommand::Pause {
                dataflow,
                actor,
                reason,
            } => print_json(
                &operator::set_source_control(&db, &dataflow, &actor, &reason, true).await?,
            )?,
            SourceCommand::Resume {
                dataflow,
                actor,
                reason,
            } => print_json(
                &operator::set_source_control(&db, &dataflow, &actor, &reason, false).await?,
            )?,
        },
        Commands::Queue { command } => match command {
            QueueCommand::RetryDlq {
                job_id,
                actor,
                reason,
            } => print_json(&operator::retry_dead_letter(&db, job_id, &actor, &reason).await?)?,
        },
        Commands::Artifact { command } => match command {
            ArtifactCommand::Reparse {
                artifact_id,
                dataflow,
                parser_version,
                actor,
                reason,
            } => print_json(
                &operator::reparse_artifact(
                    &db,
                    &artifact_id,
                    &dataflow,
                    &parser_version,
                    &actor,
                    &reason,
                )
                .await?,
            )?,
        },
        Commands::Generation { command } => match command {
            GenerationCommand::Inspect { id } => {
                print_json(&operator::inspect_generation(&db, id).await?)?;
            }
        },
        Commands::ManualInput { command } => match command {
            ManualInputCommand::Load {
                file,
                dataflow,
                source_url,
                license,
                retrieved_at,
                reviewer_role,
                reviewed_at,
                evidence_notes,
                actor,
                reason,
            } => print_json(
                &operator::load_manual_input(
                    &db,
                    &file,
                    operator::ManualReview {
                        dataflow: &dataflow,
                        source_url: &source_url,
                        license: &license,
                        retrieved_at,
                        reviewer_role: &reviewer_role,
                        reviewed_at,
                        evidence_notes: &evidence_notes,
                        actor: &actor,
                        reason: &reason,
                    },
                )
                .await?,
            )?,
        },
    }

    Ok(())
}

fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(std::io::stdout(), value).context("write json output")?;
    println!();
    Ok(())
}
