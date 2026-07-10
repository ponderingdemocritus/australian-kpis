//! Prometheus metrics for the production API and durable work state.

use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use au_kpis_source_register::{SourceStatus, load_source_register};
use axum::{
    body::Bytes,
    extract::{MatchedPath, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::AppState;

const HTTP_DURATION_BUCKETS: [f64; 10] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0, 30.0];
const STREAM_DURATION_BUCKETS: [f64; 8] = [0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HttpMetricKey {
    method: String,
    route: String,
    status: String,
    eligible: bool,
}

#[derive(Debug, Default)]
struct HistogramMetric {
    count: u64,
    sum: f64,
    buckets: Vec<u64>,
}

impl HistogramMetric {
    fn observe(&mut self, value: f64, buckets: &[f64]) {
        self.count = self.count.saturating_add(1);
        self.sum += value;
        if self.buckets.len() != buckets.len() {
            self.buckets.resize(buckets.len(), 0);
        }
        for (index, upper_bound) in buckets.iter().enumerate() {
            if value <= *upper_bound {
                self.buckets[index] = self.buckets[index].saturating_add(1);
            }
        }
    }
}

#[derive(Debug, Default)]
struct ApiMetrics {
    http: BTreeMap<HttpMetricKey, HistogramMetric>,
    streams: BTreeMap<String, StreamMetric>,
}

#[derive(Debug, Default)]
struct StreamMetric {
    active: u64,
    bytes: u64,
    duration: HistogramMetric,
}

static METRICS: OnceLock<Mutex<ApiMetrics>> = OnceLock::new();

fn metrics_registry() -> &'static Mutex<ApiMetrics> {
    METRICS.get_or_init(|| Mutex::new(ApiMetrics::default()))
}

/// Record route-level request counts and handler latency with bounded labels.
pub async fn record_http_metrics(request: Request, next: Next) -> Response {
    let started = Instant::now();
    let method = request.method().as_str().to_string();
    let route = request.extensions().get::<MatchedPath>().map_or_else(
        || fallback_route(request.uri().path()),
        |path| path.as_str().to_string(),
    );
    let traffic_class = request
        .headers()
        .get("x-au-kpis-traffic-class")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let response = next.run(request).await;
    let status = response.status();
    let eligible = method == "GET"
        && route.starts_with("/v1/")
        && !status.is_client_error()
        && !matches!(traffic_class.as_str(), "synthetic" | "operator-drill");
    let key = HttpMetricKey {
        method,
        route,
        status: status.as_u16().to_string(),
        eligible,
    };
    metrics_registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .http
        .entry(key)
        .or_default()
        .observe(started.elapsed().as_secs_f64(), &HTTP_DURATION_BUCKETS);
    response
}

/// Track a streaming response until it completes or the client disconnects.
#[derive(Debug)]
pub(crate) struct StreamGuard {
    format: &'static str,
    started: Instant,
    bytes: u64,
}

impl StreamGuard {
    pub(crate) fn start(format: &'static str) -> Self {
        let mut registry = metrics_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry
            .streams
            .entry(format.to_string())
            .or_default()
            .active += 1;
        drop(registry);
        Self {
            format,
            started: Instant::now(),
            bytes: 0,
        }
    }

    pub(crate) fn record_chunk(&mut self, chunk: &Result<Bytes, axum::Error>) {
        if let Ok(bytes) = chunk {
            self.bytes = self
                .bytes
                .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        }
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let mut registry = metrics_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let metric = registry.streams.entry(self.format.to_string()).or_default();
        metric.active = metric.active.saturating_sub(1);
        metric.bytes = metric.bytes.saturating_add(self.bytes);
        metric.duration.observe(
            self.started.elapsed().as_secs_f64(),
            &STREAM_DURATION_BUCKETS,
        );
    }
}

/// `GET /metrics`, protected by `AU_KPIS_METRICS_BEARER_TOKEN` when configured.
pub async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !metrics_authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let mut body = render_process_metrics(&state);
    let started = Instant::now();
    let (
        queue,
        generations,
        webhooks,
        subscriptions,
        freshness,
        aps,
        replication,
        operational,
        redis,
    ) = tokio::join!(
        load_group_counts(
            &state,
            "SELECT stage || ':' || status, count(*)::BIGINT FROM queue_jobs GROUP BY stage, status",
        ),
        load_group_counts(
            &state,
            "SELECT dataflow_id || ':' || status, count(*)::BIGINT FROM ingestion_generations GROUP BY dataflow_id, status",
        ),
        load_group_counts(
            &state,
            "SELECT status, count(*)::BIGINT FROM webhook_deliveries GROUP BY status",
        ),
        load_group_counts(
            &state,
            "SELECT status, count(*)::BIGINT FROM webhook_subscriptions GROUP BY status",
        ),
        load_freshness(&state),
        load_aps(&state),
        load_replication_lag(&state),
        load_operational_scalars(&state),
        state.cache.health_check(),
    );

    let mut collection_success = true;
    collection_success &= append_group_counts(
        &mut body,
        queue,
        "au_kpis_queue_depth",
        &["stage", "status"],
    );
    collection_success &= append_group_counts(
        &mut body,
        generations,
        "au_kpis_ingestion_generations",
        &["dataflow", "status"],
    );
    collection_success &= append_group_counts(
        &mut body,
        webhooks,
        "au_kpis_webhook_deliveries",
        &["status"],
    );
    collection_success &= append_group_counts(
        &mut body,
        subscriptions,
        "au_kpis_webhook_subscriptions",
        &["status"],
    );
    collection_success &= append_freshness(&mut body, freshness);
    collection_success &= append_aps(&mut body, aps);
    collection_success &=
        append_scalar(&mut body, replication, "au_kpis_db_replication_lag_seconds");
    collection_success &= append_operational_scalars(&mut body, operational);
    body.push_str(&format!(
        "# HELP au_kpis_redis_up Whether the API can reach Redis within its command budget.\n\
         # TYPE au_kpis_redis_up gauge\n\
         au_kpis_redis_up {}\n",
        u8::from(redis.is_ok())
    ));
    body.push_str(&format!(
        "# HELP au_kpis_metrics_collection_duration_seconds Time spent collecting live dependency metrics.\n\
         # TYPE au_kpis_metrics_collection_duration_seconds gauge\n\
         au_kpis_metrics_collection_duration_seconds {}\n\
         # HELP au_kpis_metrics_collection_success Whether all durable-state metric queries succeeded.\n\
         # TYPE au_kpis_metrics_collection_success gauge\n\
         au_kpis_metrics_collection_success {}\n",
        started.elapsed().as_secs_f64(),
        u8::from(collection_success)
    ));

    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

fn metrics_authorized(headers: &HeaderMap) -> bool {
    let Ok(expected) = std::env::var("AU_KPIS_METRICS_BEARER_TOKEN") else {
        return true;
    };
    expected.len() >= 32
        && headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|actual| constant_time_eq(expected.as_bytes(), actual.as_bytes()))
}

fn constant_time_eq(expected: &[u8], actual: &[u8]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    expected
        .iter()
        .zip(actual)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn fallback_route(path: &str) -> String {
    if matches!(path, "/livez" | "/readyz" | "/metrics") {
        return path.to_string();
    }
    "unmatched".to_string()
}

fn render_process_metrics(state: &AppState) -> String {
    let registry = metrics_registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut body = String::from(
        "# HELP au_kpis_http_requests_total HTTP requests completed by route, status, and availability eligibility.\n\
         # TYPE au_kpis_http_requests_total counter\n\
         # HELP au_kpis_http_request_duration_seconds HTTP handler duration by route, status, and availability eligibility.\n\
         # TYPE au_kpis_http_request_duration_seconds histogram\n",
    );
    for (key, metric) in &registry.http {
        let labels = format!(
            "method=\"{}\",route=\"{}\",status=\"{}\",eligible=\"{}\"",
            label_value(&key.method),
            label_value(&key.route),
            key.status,
            key.eligible
        );
        body.push_str(&format!(
            "au_kpis_http_requests_total{{{labels}}} {}\n",
            metric.count
        ));
        render_histogram(
            &mut body,
            "au_kpis_http_request_duration_seconds",
            &labels,
            metric,
            &HTTP_DURATION_BUCKETS,
        );
    }
    body.push_str(
        "# HELP au_kpis_stream_active Active observation streams by response format.\n\
         # TYPE au_kpis_stream_active gauge\n\
         # HELP au_kpis_stream_bytes_total Observation stream bytes sent by response format.\n\
         # TYPE au_kpis_stream_bytes_total counter\n\
         # HELP au_kpis_stream_duration_seconds Completed observation stream duration by response format.\n\
         # TYPE au_kpis_stream_duration_seconds histogram\n",
    );
    for (format, metric) in &registry.streams {
        let labels = format!("format=\"{}\"", label_value(format));
        body.push_str(&format!(
            "au_kpis_stream_active{{{labels}}} {}\n\
             au_kpis_stream_bytes_total{{{labels}}} {}\n",
            metric.active, metric.bytes
        ));
        render_histogram(
            &mut body,
            "au_kpis_stream_duration_seconds",
            &labels,
            &metric.duration,
            &STREAM_DURATION_BUCKETS,
        );
    }
    drop(registry);

    let size = state.db.size();
    let idle = u32::try_from(state.db.num_idle()).unwrap_or(u32::MAX);
    let maximum = state.db.options().get_max_connections();
    body.push_str(&format!(
        "# HELP au_kpis_db_pool_connections Database pool connections by state.\n\
         # TYPE au_kpis_db_pool_connections gauge\n\
         au_kpis_db_pool_connections{{state=\"open\"}} {size}\n\
         au_kpis_db_pool_connections{{state=\"idle\"}} {idle}\n\
         au_kpis_db_pool_connections{{state=\"in_use\"}} {}\n\
         au_kpis_db_pool_connections{{state=\"maximum\"}} {maximum}\n\
         # HELP au_kpis_admission_available_permits Available API admission permits by class.\n\
         # TYPE au_kpis_admission_available_permits gauge\n\
         au_kpis_admission_available_permits{{class=\"short\"}} {}\n\
         au_kpis_admission_available_permits{{class=\"bulk\"}} {}\n",
        size.saturating_sub(idle),
        state.short_admission.available_permits(),
        state.bulk_admission.available_permits(),
    ));
    if let Ok(register) = load_source_register() {
        body.push_str(
            "# HELP au_kpis_ingestion_freshness_budget_seconds Governed hard freshness budget for each active dataflow.\n\
             # TYPE au_kpis_ingestion_freshness_budget_seconds gauge\n",
        );
        for dataflow in register
            .dataflows
            .into_iter()
            .filter(|dataflow| dataflow.status == SourceStatus::Active)
        {
            if let Some(policy) = dataflow.freshness_policy {
                body.push_str(&format!(
                    "au_kpis_ingestion_freshness_budget_seconds{{dataflow=\"{}\"}} {}\n",
                    label_value(&dataflow.dataflow_id),
                    policy.hard_after_seconds
                ));
            }
        }
    }
    body
}

fn render_histogram(
    body: &mut String,
    name: &str,
    labels: &str,
    metric: &HistogramMetric,
    buckets: &[f64],
) {
    for (index, upper_bound) in buckets.iter().enumerate() {
        body.push_str(&format!(
            "{name}_bucket{{{labels},le=\"{upper_bound}\"}} {}\n",
            metric.buckets.get(index).copied().unwrap_or_default()
        ));
    }
    body.push_str(&format!(
        "{name}_bucket{{{labels},le=\"+Inf\"}} {}\n\
         {name}_sum{{{labels}}} {}\n\
         {name}_count{{{labels}}} {}\n",
        metric.count, metric.sum, metric.count
    ));
}

async fn load_group_counts(
    state: &AppState,
    sql: &'static str,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    tokio::time::timeout(
        Duration::from_millis(500),
        sqlx::query_as::<_, (String, i64)>(sql).fetch_all(&state.db),
    )
    .await
    .map_err(|_| sqlx::Error::PoolTimedOut)?
}

async fn load_freshness(state: &AppState) -> Result<Vec<(String, f64)>, sqlx::Error> {
    tokio::time::timeout(
        Duration::from_millis(500),
        sqlx::query_as::<_, (String, f64)>(
            "SELECT d.id,
                    COALESCE(
                        EXTRACT(EPOCH FROM (now() - max(g.published_at))),
                        1000000000000
                    )::DOUBLE PRECISION
             FROM dataflows d
             LEFT JOIN ingestion_generations g
               ON g.dataflow_id = d.id AND g.status = 'published'
             GROUP BY d.id",
        )
        .fetch_all(&state.db),
    )
    .await
    .map_err(|_| sqlx::Error::PoolTimedOut)?
}

async fn load_aps(state: &AppState) -> Result<Option<(String, f64, f64)>, sqlx::Error> {
    tokio::time::timeout(
        Duration::from_millis(500),
        sqlx::query_as::<_, (String, f64, f64)>(
            "SELECT publication_state,
                    overall_coverage_pct,
                    EXTRACT(EPOCH FROM (now() - published_at))::DOUBLE PRECISION
             FROM scorecard_snapshots_latest
             WHERE scorecard_id = 'aps'
             ORDER BY snapshot_date DESC, revision DESC
             LIMIT 1",
        )
        .fetch_optional(&state.db),
    )
    .await
    .map_err(|_| sqlx::Error::PoolTimedOut)?
}

async fn load_replication_lag(state: &AppState) -> Result<f64, sqlx::Error> {
    tokio::time::timeout(
        Duration::from_millis(500),
        sqlx::query_scalar::<_, f64>(
            "SELECT CASE WHEN pg_is_in_recovery()
                    THEN COALESCE(EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp()), 0)
                    ELSE 0 END::DOUBLE PRECISION",
        )
        .fetch_one(&state.db),
    )
    .await
    .map_err(|_| sqlx::Error::PoolTimedOut)?
}

#[derive(Debug)]
struct OperationalScalars {
    queue_oldest_pending_age: f64,
    generation_failures_recent: f64,
    webhook_oldest_due_age: f64,
    webhook_dead_letters_recent: f64,
}

async fn load_operational_scalars(state: &AppState) -> Result<OperationalScalars, sqlx::Error> {
    tokio::time::timeout(
        Duration::from_millis(500),
        sqlx::query_as::<_, (f64, f64, f64, f64)>(
            "SELECT
                COALESCE((SELECT EXTRACT(EPOCH FROM now() - min(run_at))
                          FROM queue_jobs WHERE status = 'pending'), 0)::DOUBLE PRECISION,
                (SELECT count(*) FROM ingestion_generations
                 WHERE status IN ('failed', 'rejected')
                   AND updated_at >= now() - INTERVAL '15 minutes')::DOUBLE PRECISION,
                COALESCE((SELECT EXTRACT(EPOCH FROM now() - min(next_attempt_at))
                          FROM webhook_deliveries
                          WHERE status = 'pending' AND next_attempt_at <= now()), 0)::DOUBLE PRECISION,
                (SELECT count(*) FROM webhook_deliveries
                 WHERE status IN ('dead_letter', 'failed')
                   AND updated_at >= now() - INTERVAL '15 minutes')::DOUBLE PRECISION",
        )
        .fetch_one(&state.db),
    )
    .await
    .map_err(|_| sqlx::Error::PoolTimedOut)?
    .map(
        |(
            queue_oldest_pending_age,
            generation_failures_recent,
            webhook_oldest_due_age,
            webhook_dead_letters_recent,
        )| OperationalScalars {
            queue_oldest_pending_age,
            generation_failures_recent,
            webhook_oldest_due_age,
            webhook_dead_letters_recent,
        },
    )
}

fn append_group_counts(
    body: &mut String,
    values: Result<Vec<(String, i64)>, sqlx::Error>,
    metric: &str,
    labels: &[&str],
) -> bool {
    let Ok(values) = values else {
        return false;
    };
    body.push_str(&format!("# TYPE {metric} gauge\n"));
    for (key, value) in values {
        let parts = key.split(':').collect::<Vec<_>>();
        if parts.len() != labels.len() {
            continue;
        }
        let rendered = labels
            .iter()
            .zip(parts)
            .map(|(name, value)| format!("{name}=\"{}\"", label_value(value)))
            .collect::<Vec<_>>()
            .join(",");
        body.push_str(&format!("{metric}{{{rendered}}} {value}\n"));
    }
    true
}

fn append_freshness(body: &mut String, values: Result<Vec<(String, f64)>, sqlx::Error>) -> bool {
    let Ok(values) = values else {
        return false;
    };
    body.push_str("# TYPE au_kpis_ingestion_lag_seconds gauge\n");
    for (dataflow, lag) in values {
        body.push_str(&format!(
            "au_kpis_ingestion_lag_seconds{{dataflow=\"{}\"}} {lag}\n",
            label_value(&dataflow)
        ));
    }
    true
}

fn append_aps(body: &mut String, value: Result<Option<(String, f64, f64)>, sqlx::Error>) -> bool {
    let Ok(value) = value else {
        return false;
    };
    let Some((state, coverage, age)) = value else {
        body.push_str(
            "# TYPE au_kpis_aps_snapshot_present gauge\n\
             au_kpis_aps_snapshot_present 0\n",
        );
        return true;
    };
    body.push_str(&format!(
        "# TYPE au_kpis_aps_snapshot_present gauge\n\
         au_kpis_aps_snapshot_present 1\n\
         # TYPE au_kpis_aps_snapshot_age_seconds gauge\n\
         au_kpis_aps_snapshot_age_seconds {age}\n\
         # TYPE au_kpis_aps_coverage_percent gauge\n\
         au_kpis_aps_coverage_percent {coverage}\n\
         # TYPE au_kpis_aps_publication_state gauge\n\
         au_kpis_aps_publication_state{{state=\"{}\"}} 1\n",
        label_value(&state)
    ));
    true
}

fn append_operational_scalars(
    body: &mut String,
    value: Result<OperationalScalars, sqlx::Error>,
) -> bool {
    let Ok(value) = value else {
        return false;
    };
    body.push_str(&format!(
        "# TYPE au_kpis_queue_oldest_pending_age_seconds gauge\n\
         au_kpis_queue_oldest_pending_age_seconds {}\n\
         # TYPE au_kpis_ingestion_generation_failures_recent gauge\n\
         au_kpis_ingestion_generation_failures_recent {}\n\
         # TYPE au_kpis_webhook_oldest_due_age_seconds gauge\n\
         au_kpis_webhook_oldest_due_age_seconds {}\n\
         # TYPE au_kpis_webhook_dead_letters_recent gauge\n\
         au_kpis_webhook_dead_letters_recent {}\n",
        value.queue_oldest_pending_age,
        value.generation_failures_recent,
        value.webhook_oldest_due_age,
        value.webhook_dead_letters_recent,
    ));
    true
}

fn append_scalar(body: &mut String, value: Result<f64, sqlx::Error>, metric: &str) -> bool {
    let Ok(value) = value else {
        return false;
    };
    body.push_str(&format!("# TYPE {metric} gauge\n{metric} {value}\n"));
    true
}

fn label_value(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('"', r#"\""#)
        .replace('\n', r"\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_observations_are_cumulative() {
        let mut metric = HistogramMetric::default();
        metric.observe(0.02, &[0.01, 0.1, 1.0]);
        metric.observe(0.5, &[0.01, 0.1, 1.0]);
        assert_eq!(metric.buckets, vec![0, 1, 2]);
        assert_eq!(metric.count, 2);
    }

    #[test]
    fn labels_escape_prometheus_control_characters() {
        assert_eq!(label_value("a\\\"b\nc"), "a\\\\\\\"b\\nc");
    }

    #[test]
    fn bearer_comparison_rejects_different_lengths() {
        assert!(constant_time_eq(b"same", b"same"));
        assert!(!constant_time_eq(b"same", b"different"));
    }
}
