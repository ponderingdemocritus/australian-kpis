//! API-key middleware for protected routes.

use au_kpis_auth::{ApiKeyManager, AuthError, VerifiedApiKey};
use axum::{
    extract::{FromRequestParts, Request, State},
    http::{HeaderMap, request::Parts},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::{ApiError, AppState};

/// API-key identity required by protected handlers.
///
/// As a handler extractor this runs after axum has accepted the request method
/// for a route, so unsupported methods still receive axum's 405 response.
#[derive(Debug, Clone)]
pub struct RequiredApiKey {
    /// Verified API key identity.
    pub key: VerifiedApiKey,
}

#[axum::async_trait]
impl FromRequestParts<AppState> for RequiredApiKey {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if let Some(key) = parts.extensions.get::<VerifiedApiKey>() {
            return Ok(Self { key: key.clone() });
        }

        let key = verify_api_key_header(state, &parts.headers).await?;
        parts.extensions.insert(key.clone());
        Ok(Self { key })
    }
}

/// Validate the `X-API-Key` header and attach the verified key to extensions.
pub async fn require_api_key(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    if request.extensions().get::<VerifiedApiKey>().is_some() {
        return next.run(request).await;
    }

    match verify_api_key_header(&state, request.headers()).await {
        Ok(verified) => {
            request.extensions_mut().insert(verified);
            next.run(request).await
        }
        Err(err) => err.into_response(),
    }
}

/// Verify the `X-API-Key` header from a request.
pub async fn verify_api_key_header(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<VerifiedApiKey, ApiError> {
    let Some(header) = headers.get("x-api-key") else {
        return Err(unauthorized());
    };
    let Ok(plaintext) = header.to_str() else {
        return Err(unauthorized());
    };

    let manager = ApiKeyManager::new(state.db.clone(), state.cache.clone());
    match manager.verify(plaintext).await {
        Ok(verified) => Ok(verified),
        Err(AuthError::InvalidApiKey | AuthError::Validation(_)) => Err(unauthorized()),
        Err(err) => {
            tracing::error!(error = %err, "api key verification failed");
            Err(ApiError::Internal)
        }
    }
}

fn unauthorized() -> ApiError {
    ApiError::Unauthorized("missing or invalid API key".into())
}
