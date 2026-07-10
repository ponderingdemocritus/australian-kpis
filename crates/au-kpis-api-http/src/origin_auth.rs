//! Trusted Cloudflare and web-BFF origin authentication.

use std::time::Duration;

use axum::{
    extract::{Request, State},
    http::{HeaderName, HeaderValue, Method},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::{ApiError, AppState};

const ORIGIN_ID: HeaderName = HeaderName::from_static("x-au-kpis-origin-id");
const CLIENT_IP: HeaderName = HeaderName::from_static("x-au-kpis-client-ip");
const ORIGIN_TIMESTAMP: HeaderName = HeaderName::from_static("x-au-kpis-origin-timestamp");
const ORIGIN_SIGNATURE: HeaderName = HeaderName::from_static("x-au-kpis-origin-signature");
const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const DEGRADED: HeaderName = HeaderName::from_static("x-au-kpis-degraded");
const MAX_CLOCK_SKEW_SECONDS: i64 = 30;
const REPLAY_TTL: Duration = Duration::from_secs(60);

type HmacSha256 = Hmac<Sha256>;

/// Reject requests that did not arrive through a configured trusted origin.
pub async fn require_trusted_origin(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    if !origin_auth_required() {
        return next.run(request).await;
    }

    let verified = match verify_headers(&request) {
        Ok(verified) => verified,
        Err(error) => return error.into_response(),
    };
    request.headers_mut().remove("x-forwarded-for");
    request.headers_mut().remove("x-real-ip");

    let replay_key = format!(
        "origin-replay:{}",
        hex::encode(Sha256::digest(
            format!(
                "{}:{}:{}",
                verified.origin_id, verified.request_id, verified.timestamp
            )
            .as_bytes()
        ))
    );
    match state.cache.claim_once(&replay_key, REPLAY_TTL).await {
        Ok(true) => next.run(request).await,
        Ok(false) => ApiError::Unauthorized("replayed origin request".into()).into_response(),
        Err(error) if request.method() == Method::GET => {
            tracing::warn!(%error, "origin replay cache unavailable; serving public read degraded");
            let mut response = next.run(request).await;
            response
                .headers_mut()
                .insert(DEGRADED, HeaderValue::from_static("redis"));
            response
        }
        Err(error) => {
            tracing::warn!(%error, "origin replay cache unavailable; rejecting protected write");
            ApiError::DependencyUnavailable("redis").into_response()
        }
    }
}

#[derive(Debug)]
struct VerifiedOrigin {
    origin_id: String,
    request_id: String,
    timestamp: i64,
}

fn verify_headers(request: &Request) -> Result<VerifiedOrigin, ApiError> {
    verify_headers_at(request, Utc::now().timestamp(), secret_for_origin)
}

fn verify_headers_at(
    request: &Request,
    now: i64,
    secret_lookup: impl Fn(&str) -> Result<String, ApiError>,
) -> Result<VerifiedOrigin, ApiError> {
    let origin_id = required_header(request, &ORIGIN_ID)?;
    let client_ip = required_header(request, &CLIENT_IP)?;
    let timestamp_raw = required_header(request, &ORIGIN_TIMESTAMP)?;
    let request_id = required_header(request, &REQUEST_ID)?;
    let signature = required_header(request, &ORIGIN_SIGNATURE)?;
    let timestamp = timestamp_raw
        .parse::<i64>()
        .map_err(|_| ApiError::Unauthorized("invalid origin timestamp".into()))?;
    if (now - timestamp).abs() > MAX_CLOCK_SKEW_SECONDS {
        return Err(ApiError::Unauthorized("expired origin timestamp".into()));
    }
    let secret = secret_lookup(&origin_id)?;
    let value = format!(
        "{origin_id}\n{client_ip}\n{timestamp}\n{request_id}\n{}\n{}",
        request.method(),
        request
            .uri()
            .path_and_query()
            .map_or(request.uri().path(), |value| value.as_str())
    );
    verify_origin_signature(secret.as_bytes(), value.as_bytes(), &signature)?;
    Ok(VerifiedOrigin {
        origin_id,
        request_id,
        timestamp,
    })
}

fn verify_origin_signature(secret: &[u8], value: &[u8], signature: &str) -> Result<(), ApiError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| ApiError::Unauthorized("invalid origin signature encoding".into()))?;
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|_| ApiError::Unauthorized("invalid origin secret".into()))?;
    mac.update(value);
    mac.verify_slice(&decoded)
        .map_err(|_| ApiError::Unauthorized("invalid origin signature".into()))
}

fn required_header(request: &Request, name: &HeaderName) -> Result<String, ApiError> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| ApiError::Unauthorized(format!("missing trusted-origin header `{name}`")))
}

fn secret_for_origin(origin_id: &str) -> Result<String, ApiError> {
    for (id_var, secret_var) in [
        (
            "AU_KPIS_CLOUDFLARE_ORIGIN_ID",
            "AU_KPIS_CLOUDFLARE_ORIGIN_SECRET",
        ),
        ("AU_KPIS_BFF_ORIGIN_ID", "AU_KPIS_BFF_ORIGIN_SECRET"),
    ] {
        if std::env::var(id_var).ok().as_deref() == Some(origin_id) {
            let secret = std::env::var(secret_var)
                .map_err(|_| ApiError::Unauthorized("origin secret is not configured".into()))?;
            if secret.len() < 32 {
                return Err(ApiError::Unauthorized("origin secret is too short".into()));
            }
            return Ok(secret);
        }
    }
    Err(ApiError::Unauthorized("untrusted origin id".into()))
}

fn origin_auth_required() -> bool {
    std::env::var("AU_KPIS_ORIGIN_AUTH_REQUIRED")
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes"))
}

#[cfg(test)]
pub(crate) fn sign_origin_for_test(
    secret: &str,
    origin_id: &str,
    client_ip: &str,
    timestamp: i64,
    request_id: &str,
    method: &str,
    path_and_query: &str,
) -> String {
    let value =
        format!("{origin_id}\n{client_ip}\n{timestamp}\n{request_id}\n{method}\n{path_and_query}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("test origin secret");
    mac.update(value.as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use axum::{body::Body, extract::Request};

    use crate::ApiError;

    use super::{sign_origin_for_test, verify_headers_at, verify_origin_signature};

    #[test]
    fn trusted_origin_signature_rejects_tampering() {
        let secret = "0123456789abcdef0123456789abcdef";
        let value = "cloudflare\n203.0.113.9\n1781000000\nrequest-1\nGET\n/v1/observations?dataflow=abs.cpi";
        let signature = sign_origin_for_test(
            secret,
            "cloudflare",
            "203.0.113.9",
            1_781_000_000,
            "request-1",
            "GET",
            "/v1/observations?dataflow=abs.cpi",
        );
        verify_origin_signature(secret.as_bytes(), value.as_bytes(), &signature)
            .expect("matching signature");
        assert!(
            verify_origin_signature(
                secret.as_bytes(),
                value.replace("abs.cpi", "abs.wpi").as_bytes(),
                &signature,
            )
            .is_err()
        );
    }

    #[test]
    fn trusted_origin_headers_reject_missing_expired_and_path_mismatch() {
        let secret = "0123456789abcdef0123456789abcdef";
        let now = 1_781_000_000;
        let signature = sign_origin_for_test(
            secret,
            "cloudflare",
            "203.0.113.9",
            now,
            "request-1",
            "GET",
            "/v1/sources?status=active",
        );
        let request = Request::builder()
            .uri("/v1/sources?status=active")
            .header("x-au-kpis-origin-id", "cloudflare")
            .header("x-au-kpis-client-ip", "203.0.113.9")
            .header("x-au-kpis-origin-timestamp", now.to_string())
            .header("x-au-kpis-origin-signature", &signature)
            .header("x-request-id", "request-1")
            .body(Body::empty())
            .unwrap();
        let lookup = |id: &str| {
            if id == "cloudflare" {
                Ok(secret.to_string())
            } else {
                Err(ApiError::Unauthorized("untrusted".into()))
            }
        };
        verify_headers_at(&request, now, lookup).expect("valid trusted origin");
        assert!(verify_headers_at(&request, now + 31, lookup).is_err());

        let mismatched = Request::builder()
            .uri("/v1/sources?status=inactive")
            .header("x-au-kpis-origin-id", "cloudflare")
            .header("x-au-kpis-client-ip", "203.0.113.9")
            .header("x-au-kpis-origin-timestamp", now.to_string())
            .header("x-au-kpis-origin-signature", signature)
            .header("x-request-id", "request-1")
            .body(Body::empty())
            .unwrap();
        assert!(verify_headers_at(&mismatched, now, lookup).is_err());

        let missing = Request::builder()
            .uri("/v1/sources")
            .body(Body::empty())
            .unwrap();
        assert!(verify_headers_at(&missing, now, lookup).is_err());
    }
}
