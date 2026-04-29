use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use au_kpis_pdf_client::{
    ExtractRequest, ExtractionBackendKind, ExtractionStrategy, PdfClient, RetryPolicy,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn serve_responses(
    responses: Vec<&'static str>,
    statuses: Vec<u16>,
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let addr = listener.local_addr().expect("server address");
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_task = Arc::clone(&attempts);

    tokio::spawn(async move {
        for (body, status) in responses.into_iter().zip(statuses) {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let mut request = vec![0_u8; 4096];
            let read = stream.read(&mut request).await.expect("read request");
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("POST /extract HTTP/1.1"));
            assert!(request.contains("content-type: application/json"));
            assert!(request.contains(r#""s3_key":"artifacts/abc123""#));
            assert!(request.contains(r#""source_id":"treasury""#));
            assert!(request.contains(r#""artifact_date":"2026-05-12""#));
            assert!(request.contains(r#""strategy":"deterministic""#));
            attempts_for_task.fetch_add(1, Ordering::SeqCst);

            let response = format!(
                "HTTP/1.1 {status} test\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        }
    });

    (format!("http://{addr}"), attempts)
}

fn request() -> ExtractRequest {
    ExtractRequest::new("artifacts/abc123")
        .source_id("treasury")
        .artifact_date("2026-05-12")
        .strategy(ExtractionStrategy::Deterministic)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_posts_typed_request_and_parses_tables() {
    let body = r#"{
      "artifact_key": "artifacts/abc123",
      "backend": {
        "kind": "deterministic",
        "name": "camelot",
        "version": "1.0.0",
        "model_sha256": null
      },
      "tables": [
        {
          "page": 12,
          "bbox": [72.0, 120.0, 540.0, 720.0],
          "cells": [["Year", "Revenue"], ["2026-27", "123.4"]],
          "diagnostics": {"confidence": 0.98}
        }
      ]
    }"#;
    let (base_url, attempts) = serve_responses(vec![body], vec![200]).await;
    let client = PdfClient::builder()
        .base_url(base_url)
        .retry_policy(RetryPolicy::none())
        .build()
        .unwrap();

    let extracted = client.extract(request()).await.expect("extract");

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(extracted.artifact_key, "artifacts/abc123");
    assert_eq!(extracted.backend.kind, ExtractionBackendKind::Deterministic);
    assert_eq!(extracted.tables[0].page, 12);
    assert_eq!(extracted.tables[0].cells[1][1], "123.4");
    assert_eq!(extracted.tables[0].diagnostics["confidence"], 0.98);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_retries_5xx_with_backoff() {
    let success = r#"{
      "s3_key": "artifacts/abc123",
      "backend": {
        "kind": "deterministic",
        "name": "pdfplumber",
        "version": "0.11",
        "model_sha256": null
      },
      "tables": []
    }"#;
    let (base_url, attempts) =
        serve_responses(vec![r#"{"error":"busy"}"#, success], vec![503, 200]).await;
    let client = PdfClient::builder()
        .base_url(base_url)
        .retry_policy(RetryPolicy::new(2, Duration::ZERO).unwrap())
        .build()
        .unwrap();

    let extracted = client.extract(request()).await.expect("retry succeeds");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(extracted.artifact_key, "artifacts/abc123");
    assert!(extracted.tables.is_empty());
}
