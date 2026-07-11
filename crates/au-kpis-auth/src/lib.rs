//! API key lifecycle and verification.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng as PasswordOsRng},
};
use au_kpis_cache::{CacheClient, CacheError};
use au_kpis_error::{Classify, ErrorClass};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tracing::instrument;
use uuid::Uuid;

const API_KEY_PREFIX: &str = "auk_live_";
const API_KEY_SECRET_BYTES: usize = 32;
const API_KEY_CACHE_TTL: Duration = Duration::from_secs(60);
const ARGON2_VERIFY_CONCURRENCY: usize = 32;
static ARGON2_VERIFY_ADMISSION: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

/// Request body for creating an API key through an admin-only flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateApiKeyRequest {
    /// Human-readable key name.
    pub name: String,
    /// Authorization scopes attached to the key.
    pub scopes: Vec<String>,
    /// Rate-limit tier consumed by downstream rate-limit middleware.
    pub rate_limit_tier: String,
    /// Administrative actor issuing the key.
    pub actor: String,
}

/// Newly-created API key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedApiKey {
    /// Stable key identifier, embedded in the plaintext key and stored in DB.
    pub id: Uuid,
    /// Plaintext key shown once to the admin caller.
    pub plaintext: String,
    /// Human-readable key name.
    pub name: String,
    /// Authorization scopes attached to the key.
    pub scopes: Vec<String>,
    /// Rate-limit tier consumed by downstream rate-limit middleware.
    pub rate_limit_tier: String,
}

/// API key identity produced by successful verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedApiKey {
    /// Stable API key identifier.
    pub id: Uuid,
    /// Human-readable key name.
    pub name: String,
    /// Authorization scopes attached to the key.
    pub scopes: Vec<String>,
    /// Rate-limit tier consumed by downstream rate-limit middleware.
    pub rate_limit_tier: String,
}

/// Errors returned by API-key lifecycle and verification.
#[derive(Debug, Error)]
pub enum AuthError {
    /// Supplied API key is missing, malformed, unknown, invalid, or revoked.
    #[error("invalid api key")]
    InvalidApiKey,
    /// Caller supplied invalid admin input.
    #[error("validation: {0}")]
    Validation(String),
    /// Password hashing failed.
    #[error("password hash: {0}")]
    PasswordHash(#[from] argon2::password_hash::Error),
    /// Database access failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// Cache access failed.
    #[error(transparent)]
    Cache(#[from] CacheError),
    /// A bounded password-verification task could not complete.
    #[error("password verification worker failed: {0}")]
    VerificationWorker(String),
}

impl Classify for AuthError {
    fn class(&self) -> ErrorClass {
        match self {
            AuthError::InvalidApiKey | AuthError::Validation(_) => ErrorClass::Validation,
            AuthError::PasswordHash(_) => ErrorClass::Permanent,
            AuthError::Db(_) | AuthError::Cache(_) | AuthError::VerificationWorker(_) => {
                ErrorClass::Transient
            }
        }
    }
}

/// API-key lifecycle and verification service.
#[derive(Debug, Clone)]
pub struct ApiKeyManager {
    db: PgPool,
    cache: Arc<CacheClient>,
}

impl ApiKeyManager {
    /// Construct a manager from shared database and cache clients.
    pub fn new(db: PgPool, cache: Arc<CacheClient>) -> Self {
        Self { db, cache }
    }

    /// Create a new API key, persist only its argon2id hash, and audit issuance.
    #[instrument(skip(self, request), fields(api_key.name = %request.name, actor = %request.actor))]
    pub async fn create_key(
        &self,
        request: CreateApiKeyRequest,
    ) -> Result<CreatedApiKey, AuthError> {
        validate_create_request(&request)?;

        let id = Uuid::new_v4();
        let plaintext = generate_plaintext_key(id);
        let key_hash = hash_key(&plaintext)?;
        let mut tx = self.db.begin().await?;

        sqlx::query(
            "INSERT INTO api_keys (id, key_hash, name, scopes, rate_limit_tier)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(id)
        .bind(&key_hash)
        .bind(&request.name)
        .bind(&request.scopes)
        .bind(&request.rate_limit_tier)
        .execute(&mut *tx)
        .await?;

        insert_audit_log(&mut tx, id, "created", &request.actor).await?;
        tx.commit().await?;

        Ok(CreatedApiKey {
            id,
            plaintext,
            name: request.name,
            scopes: request.scopes,
            rate_limit_tier: request.rate_limit_tier,
        })
    }

    /// Verify a plaintext API key using Redis cache and constant-time comparison.
    #[instrument(skip(self, plaintext_key), fields(api_key.id))]
    pub async fn verify(&self, plaintext_key: &str) -> Result<VerifiedApiKey, AuthError> {
        let parsed = parse_plaintext_key(plaintext_key)?;
        tracing::Span::current().record("api_key.id", parsed.id.to_string());

        let cache_key = cache_key(parsed.id);
        match self.cache.get_json::<CachedApiKey>(&cache_key).await {
            Ok(Some(cached)) => return verify_cached_key(plaintext_key, cached),
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%error, "API key cache unavailable; verifying against Postgres");
            }
        }

        let Some(stored) = fetch_active_key(&self.db, parsed.id).await? else {
            return Err(AuthError::InvalidApiKey);
        };

        verify_argon2_hash_bounded(plaintext_key, &stored.key_hash).await?;
        let verified = VerifiedApiKey {
            id: stored.id,
            name: stored.name,
            scopes: stored.scopes,
            rate_limit_tier: stored.rate_limit_tier,
        };
        let cached = CachedApiKey::from_verified(plaintext_key, &verified);
        if let Err(error) = self
            .cache
            .set_json(&cache_key, &cached, API_KEY_CACHE_TTL)
            .await
        {
            tracing::warn!(%error, "API key cache unavailable after Postgres verification");
        }

        sqlx::query("UPDATE api_keys SET last_used_at = now() WHERE id = $1")
            .bind(stored.id)
            .execute(&self.db)
            .await?;

        Ok(verified)
    }

    /// Revoke an API key, audit the action, and evict the cached verifier.
    #[instrument(skip(self, actor), fields(api_key.id = %id, actor = %actor))]
    pub async fn revoke_key(&self, id: Uuid, actor: &str) -> Result<(), AuthError> {
        if actor.trim().is_empty() {
            return Err(AuthError::Validation("actor is required".into()));
        }

        let mut tx = self.db.begin().await?;
        let result = sqlx::query(
            "UPDATE api_keys
             SET revoked_at = COALESCE(revoked_at, now())
             WHERE id = $1",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            return Err(AuthError::InvalidApiKey);
        }

        insert_audit_log(&mut tx, id, "revoked", actor).await?;
        tx.commit().await?;
        self.cache.delete(&cache_key(id)).await?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedApiKey {
    id: Uuid,
}

#[derive(Debug)]
struct StoredApiKey {
    id: Uuid,
    key_hash: String,
    name: String,
    scopes: Vec<String>,
    rate_limit_tier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedApiKey {
    fingerprint: String,
    verified: VerifiedApiKey,
}

impl CachedApiKey {
    fn from_verified(plaintext_key: &str, verified: &VerifiedApiKey) -> Self {
        Self {
            fingerprint: key_fingerprint(plaintext_key),
            verified: verified.clone(),
        }
    }
}

fn validate_create_request(request: &CreateApiKeyRequest) -> Result<(), AuthError> {
    if request.name.trim().is_empty() {
        return Err(AuthError::Validation("name is required".into()));
    }
    if request.rate_limit_tier.trim().is_empty() {
        return Err(AuthError::Validation("rate_limit_tier is required".into()));
    }
    if request.actor.trim().is_empty() {
        return Err(AuthError::Validation("actor is required".into()));
    }
    Ok(())
}

fn generate_plaintext_key(id: Uuid) -> String {
    let mut secret = [0_u8; API_KEY_SECRET_BYTES];
    rand::rngs::OsRng.fill_bytes(&mut secret);
    format!(
        "{API_KEY_PREFIX}{}_{}",
        id.simple(),
        URL_SAFE_NO_PAD.encode(secret)
    )
}

fn parse_plaintext_key(plaintext_key: &str) -> Result<ParsedApiKey, AuthError> {
    let Some(remainder) = plaintext_key.strip_prefix(API_KEY_PREFIX) else {
        return Err(AuthError::InvalidApiKey);
    };
    let Some((id, secret)) = remainder.split_once('_') else {
        return Err(AuthError::InvalidApiKey);
    };
    if secret.is_empty() || id.is_empty() {
        return Err(AuthError::InvalidApiKey);
    }
    let id = Uuid::parse_str(id).map_err(|_| AuthError::InvalidApiKey)?;
    Ok(ParsedApiKey { id })
}

fn hash_key(plaintext_key: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut PasswordOsRng);
    Ok(Argon2::default()
        .hash_password(plaintext_key.as_bytes(), &salt)?
        .to_string())
}

fn verify_argon2_hash(plaintext_key: &str, key_hash: &str) -> Result<(), AuthError> {
    let parsed_hash = PasswordHash::new(key_hash)?;
    Argon2::default()
        .verify_password(plaintext_key.as_bytes(), &parsed_hash)
        .map_err(|_| AuthError::InvalidApiKey)
}

async fn verify_argon2_hash_bounded(plaintext_key: &str, key_hash: &str) -> Result<(), AuthError> {
    let semaphore = ARGON2_VERIFY_ADMISSION
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(ARGON2_VERIFY_CONCURRENCY)))
        .clone();
    let permit = semaphore
        .acquire_owned()
        .await
        .map_err(|error| AuthError::VerificationWorker(error.to_string()))?;
    let plaintext_key = plaintext_key.to_owned();
    let key_hash = key_hash.to_owned();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        verify_argon2_hash(&plaintext_key, &key_hash)
    })
    .await
    .map_err(|error| AuthError::VerificationWorker(error.to_string()))?
}

fn key_fingerprint(plaintext_key: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(plaintext_key.as_bytes()))
}

fn verify_cached_key(
    plaintext_key: &str,
    cached: CachedApiKey,
) -> Result<VerifiedApiKey, AuthError> {
    let supplied = key_fingerprint(plaintext_key);
    if supplied
        .as_bytes()
        .ct_eq(cached.fingerprint.as_bytes())
        .into()
    {
        Ok(cached.verified)
    } else {
        Err(AuthError::InvalidApiKey)
    }
}

fn cache_key(id: Uuid) -> String {
    format!("auth:api-key:{id}")
}

async fn fetch_active_key(db: &PgPool, id: Uuid) -> Result<Option<StoredApiKey>, AuthError> {
    sqlx::query_as::<_, (Uuid, String, String, Vec<String>, String)>(
        "SELECT id, key_hash, name, scopes, rate_limit_tier
         FROM api_keys
         WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(id)
    .fetch_optional(db)
    .await
    .map(|row| {
        row.map(
            |(id, key_hash, name, scopes, rate_limit_tier)| StoredApiKey {
                id,
                key_hash,
                name,
                scopes,
                rate_limit_tier,
            },
        )
    })
    .map_err(AuthError::from)
}

async fn insert_audit_log(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    api_key_id: Uuid,
    action: &str,
    actor: &str,
) -> Result<(), AuthError> {
    sqlx::query(
        "INSERT INTO api_key_audit_log (api_key_id, action, actor)
         VALUES ($1, $2, $3)",
    )
    .bind(api_key_id)
    .bind(action)
    .bind(actor)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_key_roundtrips_id_and_prefix() {
        let id = Uuid::new_v4();
        let plaintext = generate_plaintext_key(id);

        assert!(plaintext.starts_with(API_KEY_PREFIX));
        assert_eq!(parse_plaintext_key(&plaintext).expect("parse key").id, id);
    }

    #[test]
    fn cached_verifier_uses_constant_time_fingerprint_match() {
        let verified = VerifiedApiKey {
            id: Uuid::new_v4(),
            name: "client".into(),
            scopes: vec!["observations:read".into()],
            rate_limit_tier: "free".into(),
        };
        let plaintext = generate_plaintext_key(verified.id);
        let cached = CachedApiKey::from_verified(&plaintext, &verified);

        assert_eq!(
            verify_cached_key(&plaintext, cached.clone()).expect("cache hit"),
            verified
        );
        assert!(matches!(
            verify_cached_key("auk_live_00000000000000000000000000000000_bad", cached),
            Err(AuthError::InvalidApiKey)
        ));
    }

    #[test]
    fn create_request_validation_reports_each_required_field() {
        let valid = CreateApiKeyRequest {
            name: "client".into(),
            scopes: vec!["observations:read".into()],
            rate_limit_tier: "free".into(),
            actor: "admin".into(),
        };

        assert!(validate_create_request(&valid).is_ok());

        let mut missing_name = valid.clone();
        missing_name.name = " ".into();
        assert!(validation_message(validate_create_request(&missing_name)).contains("name"));

        let mut missing_tier = valid.clone();
        missing_tier.rate_limit_tier = "\t".into();
        assert!(
            validation_message(validate_create_request(&missing_tier)).contains("rate_limit_tier")
        );

        let mut missing_actor = valid;
        missing_actor.actor.clear();
        assert!(validation_message(validate_create_request(&missing_actor)).contains("actor"));
    }

    fn validation_message(result: Result<(), AuthError>) -> String {
        match result {
            Err(AuthError::Validation(message)) => message,
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn plaintext_key_parser_rejects_malformed_inputs() {
        let valid_id = Uuid::new_v4().simple().to_string();

        for candidate in [
            "wrong_prefix",
            "auk_live_missing_separator",
            "auk_live_missingseparator",
            "auk_live__secret",
            &format!("auk_live_{valid_id}_"),
            "auk_live_not-a-uuid_secret",
        ] {
            assert!(
                matches!(
                    parse_plaintext_key(candidate),
                    Err(AuthError::InvalidApiKey)
                ),
                "candidate should be rejected: {candidate}"
            );
        }
    }

    #[test]
    fn argon_hash_verifier_accepts_matching_plaintext_only() {
        let plaintext = generate_plaintext_key(Uuid::new_v4());
        let hash = hash_key(&plaintext).expect("hash key");

        verify_argon2_hash(&plaintext, &hash).expect("matching plaintext verifies");
        assert!(matches!(
            verify_argon2_hash("auk_live_00000000000000000000000000000000_wrong", &hash),
            Err(AuthError::InvalidApiKey)
        ));
    }

    #[test]
    fn auth_errors_classify_by_retryability() {
        assert_eq!(AuthError::InvalidApiKey.class(), ErrorClass::Validation);
        assert_eq!(
            AuthError::Validation("bad input".into()).class(),
            ErrorClass::Validation
        );
        assert_eq!(
            hash_key("not used")
                .and_then(|_| {
                    PasswordHash::new("not a phc string")
                        .map_err(AuthError::from)
                        .map(|_| ())
                })
                .unwrap_err()
                .class(),
            ErrorClass::Permanent
        );
        assert_eq!(
            AuthError::Db(sqlx::Error::RowNotFound).class(),
            ErrorClass::Transient
        );
        assert_eq!(
            AuthError::Cache(CacheError::Validation("bad cache".into())).class(),
            ErrorClass::Transient
        );
    }
}
