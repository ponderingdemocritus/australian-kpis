//! Redis cache abstraction.
//!
//! This crate owns two primitives from `Spec.md § Stack (locked)`:
//!
//! - a typed JSON key-value cache for small hot-path values
//! - an atomic token-bucket rate limiter implemented as a Redis Lua script
//!
//! The public API stays intentionally small until downstream crates start
//! using it.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    fmt::Debug,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use au_kpis_error::{Classify, CoreError, ErrorClass};
use fred::prelude::*;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tracing::instrument;

const RATE_LIMIT_LUA: &str = r#"
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_second = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

local state = redis.call('HMGET', key, 'tokens_ms', 'last_ms')
local capacity_ms = capacity * 1000
local requested_ms = requested * 1000

local tokens_ms = tonumber(state[1])
if tokens_ms == nil then
  tokens_ms = capacity_ms
end

local last_ms = tonumber(state[2])
if last_ms == nil then
  last_ms = now_ms
end

local elapsed_ms = now_ms - last_ms
if elapsed_ms < 0 then
  elapsed_ms = 0
end

local refill_ms = elapsed_ms * refill_per_second
tokens_ms = math.min(capacity_ms, tokens_ms + refill_ms)

local allowed = 0
local retry_after_ms = 0
if tokens_ms >= requested_ms then
  allowed = 1
  tokens_ms = tokens_ms - requested_ms
else
  retry_after_ms = math.ceil((requested_ms - tokens_ms) / refill_per_second)
end

redis.call('HSET', key, 'tokens_ms', tokens_ms, 'last_ms', now_ms)
local ttl_ms = math.max(1000, math.ceil(capacity_ms / refill_per_second))
redis.call('PEXPIRE', key, ttl_ms)

return {allowed, math.floor(tokens_ms / 1000), retry_after_ms}
"#;

/// Result of a token-bucket rate-limit check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitDecision {
    /// Whether the request can proceed immediately.
    pub allowed: bool,
    /// Whole tokens remaining after the decision.
    pub remaining: u32,
    /// Delay before retry when [`Self::allowed`] is `false`.
    pub retry_after: Duration,
}

/// Errors returned by [`CacheClient`].
#[derive(Debug, Error)]
pub enum CacheError {
    /// Redis command failure.
    #[error("redis: {0}")]
    Redis(#[from] fred::error::RedisError),

    /// JSON serialization or deserialization failure.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// Caller-supplied arguments violated a precondition.
    #[error("validation: {0}")]
    Validation(String),
}

impl Classify for CacheError {
    fn class(&self) -> ErrorClass {
        match self {
            CacheError::Redis(_) => ErrorClass::Transient,
            CacheError::Core(err) => err.class(),
            CacheError::Validation(_) => ErrorClass::Validation,
        }
    }
}

/// Backend abstraction used to keep unit tests fast and deterministic.
#[async_trait]
pub trait CacheBackend: Debug + Send + Sync {
    /// Read a raw string value.
    async fn get(&self, key: &str) -> Result<Option<String>, CacheError>;

    /// Write a raw string value with a TTL.
    async fn set(&self, key: &str, value: String, ttl: Duration) -> Result<(), CacheError>;

    /// Delete a key, returning `true` when a value existed.
    async fn delete(&self, key: &str) -> Result<bool, CacheError>;

    /// Consume tokens from a bucket, atomically.
    async fn take_token_bucket(
        &self,
        key: &str,
        capacity: u32,
        refill_per_second: u32,
        requested: u32,
        now_ms: u64,
    ) -> Result<RateLimitDecision, CacheError>;
}

#[derive(Debug, Clone)]
struct FredBackend {
    pool: RedisPool,
}

#[async_trait]
impl CacheBackend for FredBackend {
    async fn get(&self, key: &str) -> Result<Option<String>, CacheError> {
        Ok(self.pool.get(key).await?)
    }

    async fn set(&self, key: &str, value: String, ttl: Duration) -> Result<(), CacheError> {
        let ttl_ms = duration_millis_i64(ttl)?;
        let _: () = self
            .pool
            .set(key, value, Some(Expiration::PX(ttl_ms)), None, false)
            .await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, CacheError> {
        let deleted: i64 = self.pool.del(key).await?;
        Ok(deleted > 0)
    }

    async fn take_token_bucket(
        &self,
        key: &str,
        capacity: u32,
        refill_per_second: u32,
        requested: u32,
        now_ms: u64,
    ) -> Result<RateLimitDecision, CacheError> {
        let result: Vec<i64> = self
            .pool
            .eval(
                RATE_LIMIT_LUA,
                vec![key],
                vec![
                    i64::from(capacity),
                    i64::from(refill_per_second),
                    i64::from(requested),
                    i64::try_from(now_ms).map_err(|_| {
                        CacheError::Validation("current time exceeds Redis script range".into())
                    })?,
                ],
            )
            .await?;

        if result.len() != 3 {
            return Err(CacheError::Validation(format!(
                "rate-limit script returned {} values, expected 3",
                result.len()
            )));
        }

        Ok(RateLimitDecision {
            allowed: result[0] == 1,
            remaining: u32::try_from(result[1]).map_err(|_| {
                CacheError::Validation(format!(
                    "rate-limit script returned invalid remaining token count: {}",
                    result[1]
                ))
            })?,
            retry_after: Duration::from_millis(u64::try_from(result[2]).map_err(|_| {
                CacheError::Validation(format!(
                    "rate-limit script returned invalid retry delay: {}",
                    result[2]
                ))
            })?),
        })
    }
}

/// Redis-backed typed cache + rate-limit client.
#[derive(Debug, Clone)]
pub struct CacheClient {
    backend: Arc<dyn CacheBackend>,
}

impl CacheClient {
    /// Connect to Redis using a small round-robin `fred` pool.
    #[instrument(skip(url))]
    pub async fn connect(url: &str) -> Result<Self, CacheError> {
        let config = RedisConfig::from_url(url)?;
        let pool = Builder::from_config(config).build_pool(2)?;
        let _connection_task = pool.init().await?;
        Ok(Self::from_backend(FredBackend { pool }))
    }

    /// Construct a cache client from a custom backend.
    pub fn from_backend<B>(backend: B) -> Self
    where
        B: CacheBackend + 'static,
    {
        Self {
            backend: Arc::new(backend),
        }
    }

    /// Read a typed JSON value from Redis.
    #[instrument(skip(self), fields(cache.key = key))]
    pub async fn get_json<T>(&self, key: &str) -> Result<Option<T>, CacheError>
    where
        T: DeserializeOwned,
    {
        self.backend
            .get(key)
            .await?
            .map(|raw| serde_json::from_str(&raw).map_err(CoreError::from))
            .transpose()
            .map_err(CacheError::from)
    }

    /// Store a typed JSON value with a TTL.
    #[instrument(skip(self, value), fields(cache.key = key, cache.ttl_ms = ttl.as_millis()))]
    pub async fn set_json<T>(&self, key: &str, value: &T, ttl: Duration) -> Result<(), CacheError>
    where
        T: Serialize,
    {
        if ttl.is_zero() {
            return Err(CacheError::Validation(
                "cache TTL must be greater than zero".into(),
            ));
        }

        let payload = serde_json::to_string(value).map_err(CoreError::from)?;
        self.backend.set(key, payload, ttl).await
    }

    /// Delete a cached value.
    #[instrument(skip(self), fields(cache.key = key))]
    pub async fn delete(&self, key: &str) -> Result<bool, CacheError> {
        self.backend.delete(key).await
    }

    /// Atomically take tokens from a bucket keyed in Redis.
    ///
    /// `capacity`, `refill_per_second`, and `requested` must all be positive,
    /// and `requested` cannot exceed `capacity`.
    #[instrument(skip(self), fields(rate_limit.key = key))]
    pub async fn take_token_bucket(
        &self,
        key: &str,
        capacity: u32,
        refill_per_second: u32,
        requested: u32,
    ) -> Result<RateLimitDecision, CacheError> {
        validate_bucket_args(capacity, refill_per_second, requested)?;
        self.backend
            .take_token_bucket(
                key,
                capacity,
                refill_per_second,
                requested,
                unix_time_millis()?,
            )
            .await
    }
}

fn validate_bucket_args(
    capacity: u32,
    refill_per_second: u32,
    requested: u32,
) -> Result<(), CacheError> {
    if capacity == 0 {
        return Err(CacheError::Validation(
            "token bucket capacity must be greater than zero".into(),
        ));
    }
    if refill_per_second == 0 {
        return Err(CacheError::Validation(
            "token bucket refill rate must be greater than zero".into(),
        ));
    }
    if requested == 0 {
        return Err(CacheError::Validation(
            "token bucket request size must be greater than zero".into(),
        ));
    }
    if requested > capacity {
        return Err(CacheError::Validation(
            "token bucket request size cannot exceed capacity".into(),
        ));
    }
    Ok(())
}

fn unix_time_millis() -> Result<u64, CacheError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| CacheError::Validation(format!("system clock before unix epoch: {err}")))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| CacheError::Validation("current time exceeds u64 milliseconds".into()))
}

fn duration_millis_i64(duration: Duration) -> Result<i64, CacheError> {
    i64::try_from(duration.as_millis())
        .map_err(|_| CacheError::Validation("TTL exceeds Redis millisecond range".into()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use crate::{CacheBackend, CacheClient, CacheError, RateLimitDecision};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Widget {
        id: u64,
        label: String,
    }

    #[derive(Debug, Clone, Default)]
    struct MockBackend {
        values: Arc<Mutex<HashMap<String, String>>>,
        deleted: Arc<Mutex<Vec<String>>>,
        script_results: Arc<Mutex<VecDeque<(bool, u32, u64)>>>,
    }

    #[async_trait]
    impl CacheBackend for MockBackend {
        async fn get(&self, key: &str) -> Result<Option<String>, CacheError> {
            Ok(self.values.lock().expect("values lock").get(key).cloned())
        }

        async fn set(&self, key: &str, value: String, _ttl: Duration) -> Result<(), CacheError> {
            self.values
                .lock()
                .expect("values lock")
                .insert(key.to_owned(), value);
            Ok(())
        }

        async fn delete(&self, key: &str) -> Result<bool, CacheError> {
            self.deleted
                .lock()
                .expect("deleted lock")
                .push(key.to_owned());
            Ok(self
                .values
                .lock()
                .expect("values lock")
                .remove(key)
                .is_some())
        }

        async fn take_token_bucket(
            &self,
            _key: &str,
            _capacity: u32,
            _refill_per_second: u32,
            _requested: u32,
            _now_ms: u64,
        ) -> Result<RateLimitDecision, CacheError> {
            let (allowed, remaining, retry_after_ms) = self
                .script_results
                .lock()
                .expect("script results lock")
                .pop_front()
                .expect("script result queued");
            Ok(RateLimitDecision {
                allowed,
                remaining,
                retry_after: Duration::from_millis(retry_after_ms),
            })
        }
    }

    #[tokio::test]
    async fn typed_cache_round_trip_serializes_json() {
        let backend = MockBackend::default();
        let client = CacheClient::from_backend(backend.clone());
        let widget = Widget {
            id: 42,
            label: "cpi".into(),
        };

        client
            .set_json("widgets:42", &widget, Duration::from_secs(30))
            .await
            .expect("set json");

        let raw = backend
            .values
            .lock()
            .expect("values lock")
            .get("widgets:42")
            .cloned()
            .expect("cached raw value");
        assert_eq!(raw, r#"{"id":42,"label":"cpi"}"#);

        let got: Option<Widget> = client.get_json("widgets:42").await.expect("get json");
        assert_eq!(got, Some(widget));
    }

    #[tokio::test]
    async fn token_bucket_result_maps_backend_response() {
        let backend = MockBackend::default();
        backend
            .script_results
            .lock()
            .expect("script results lock")
            .push_back((false, 0, 250));
        let client = CacheClient::from_backend(backend);

        let decision = client
            .take_token_bucket("ratelimit:key", 3, 2, 1)
            .await
            .expect("rate limit");

        assert_eq!(
            decision,
            RateLimitDecision {
                allowed: false,
                remaining: 0,
                retry_after: Duration::from_millis(250),
            }
        );
    }

    #[tokio::test]
    async fn invalid_rate_limit_arguments_are_rejected() {
        let client = CacheClient::from_backend(MockBackend::default());
        let err = client
            .take_token_bucket("ratelimit:key", 0, 1, 1)
            .await
            .expect_err("zero capacity should fail");

        assert!(
            err.to_string().contains("capacity"),
            "expected validation message, got {err}"
        );
    }
}
