//! API-key middleware for protected routes.

use au_kpis_auth::{ApiKeyManager, AuthError};
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::{ApiError, AppState};

/// Validate the `X-API-Key` header and attach the verified key to extensions.
pub async fn require_api_key(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(header) = request.headers().get("x-api-key") else {
        return unauthorized().into_response();
    };
    let Ok(plaintext) = header.to_str() else {
        return unauthorized().into_response();
    };

    let manager = ApiKeyManager::new(state.db.clone(), state.cache.clone());
    match manager.verify(plaintext).await {
        Ok(verified) => {
            request.extensions_mut().insert(verified);
            next.run(request).await
        }
        Err(AuthError::InvalidApiKey | AuthError::Validation(_)) => unauthorized().into_response(),
        Err(err) => {
            tracing::error!(error = %err, "api key verification failed");
            ApiError::Internal.into_response()
        }
    }
}

fn unauthorized() -> ApiError {
    ApiError::Unauthorized("missing or invalid API key".into())
}
