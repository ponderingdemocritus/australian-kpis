use std::net::SocketAddr;

use au_kpis_api_http::{ApiDoc, router};
use axum::{Router, http::header::CONTENT_TYPE, response::IntoResponse, routing::get};
use utoipa::OpenApi;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let addr = std::env::var("AU_KPIS_CONTRACT_ADDR")
        .ok()
        .and_then(|raw| raw.parse::<SocketAddr>().ok())
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 38080)));

    let app = Router::new()
        .merge(router())
        .route("/v1/openapi.json", get(openapi));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind contract server listener");
    axum::serve(listener, app)
        .await
        .expect("serve contract server");
}

async fn openapi() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "application/json")],
        ApiDoc::openapi()
            .to_pretty_json()
            .expect("serialize OpenAPI document"),
    )
}
