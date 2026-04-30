use std::{
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use au_kpis_error::{Classify, ErrorClass};
use au_kpis_pdf_client::{
    ExtractRequest, ExtractionBackendKind, ExtractionStrategy, PdfClient, PdfClientError,
    RetryPolicy,
};
use futures::StreamExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tracing::{Id, Subscriber, span::Attributes};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};

#[derive(Debug, Clone)]
struct SpanCounter {
    name: &'static str,
    count: Arc<AtomicUsize>,
}

impl<S> Layer<S> for SpanCounter
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        if attrs.metadata().name() == self.name {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }
}

async fn read_http_request(stream: &mut TcpStream) -> String {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];

    loop {
        let read = stream.read(&mut buffer).await.expect("read request");
        assert_ne!(read, 0, "client closed connection before sending request");
        request.extend_from_slice(&buffer[..read]);

        let Some(header_end) = find_subslice(&request, b"\r\n\r\n") else {
            continue;
        };
        let body_start = header_end + 4;
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().expect("valid content-length"))
            })
            .unwrap_or(0);

        if request.len() >= body_start + content_length {
            return String::from_utf8(request).expect("request is utf-8");
        }
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

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
            let request = read_http_request(&mut stream).await;
            assert!(request.starts_with("POST /extract HTTP/1.1"));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("content-type: application/json")
            );
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

async fn serve_responses_with_headers(
    responses: Vec<&'static str>,
    statuses: Vec<u16>,
    response_headers: Vec<Vec<(&'static str, &'static str)>>,
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let addr = listener.local_addr().expect("server address");
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_task = Arc::clone(&attempts);

    tokio::spawn(async move {
        for ((body, status), headers) in responses.into_iter().zip(statuses).zip(response_headers) {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let request = read_http_request(&mut stream).await;
            assert!(request.starts_with("POST /extract HTTP/1.1"));
            attempts_for_task.fetch_add(1, Ordering::SeqCst);

            let headers = headers
                .into_iter()
                .fold(String::new(), |mut output, (name, value)| {
                    write!(output, "{name}: {value}\r\n").expect("write header");
                    output
                });
            let response = format!(
                "HTTP/1.1 {status} test\r\ncontent-type: application/json\r\n{headers}content-length: {}\r\n\r\n{}",
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
    ExtractRequest::new("artifacts/abc123", "treasury")
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
          "spans": [{"row": 0, "column": 0, "row_span": 1, "column_span": 2}],
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
    assert_eq!(extracted.tables[0].spans[0].column_span, 2);
    assert_eq!(extracted.tables[0].diagnostics["confidence"], 0.98);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_retries_5xx_with_backoff() {
    let success = r#"{
      "artifact_key": "artifacts/abc123",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_retries_429_with_retry_after() {
    let success = r#"{
      "artifact_key": "artifacts/abc123",
      "backend": {
        "kind": "deterministic",
        "name": "pdfplumber",
        "version": "0.11",
        "model_sha256": null
      },
      "tables": []
    }"#;
    let (base_url, attempts) = serve_responses_with_headers(
        vec![r#"{"error":"rate limited"}"#, success],
        vec![429, 200],
        vec![vec![("retry-after", "0")], vec![]],
    )
    .await;
    let client = PdfClient::builder()
        .base_url(base_url)
        .retry_policy(RetryPolicy::new(2, Duration::from_secs(5)).unwrap())
        .build()
        .unwrap();

    let extracted = client.extract(request()).await.expect("retry succeeds");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(extracted.artifact_key, "artifacts/abc123");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_rejects_response_missing_tables() {
    let body = r#"{
      "artifact_key": "artifacts/abc123",
      "backend": {
        "kind": "deterministic",
        "name": "camelot",
        "version": "1.0.0",
        "model_sha256": null
      }
    }"#;
    let (base_url, attempts) = serve_responses(vec![body], vec![200]).await;
    let client = PdfClient::builder()
        .base_url(base_url)
        .retry_policy(RetryPolicy::none())
        .build()
        .unwrap();

    let err = client
        .extract(request())
        .await
        .expect_err("missing tables should fail response decoding");

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(err.class(), ErrorClass::Permanent);
    assert!(matches!(err, PdfClientError::Json(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_stream_yields_body_before_full_response_arrives() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let addr = listener.local_addr().expect("server address");
    let (send_rest, receive_rest) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let request = read_http_request(&mut stream).await;
        assert!(request.starts_with("POST /extract HTTP/1.1"));

        let first = br#"{"artifact_key":"artifacts/abc123","#;
        let second = br#""backend":{"kind":"deterministic","name":"camelot","version":"1.0.0","model_sha256":null},"tables":[]}"#;
        let headers = format!(
            "HTTP/1.1 200 test\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            first.len() + second.len()
        );
        stream
            .write_all(headers.as_bytes())
            .await
            .expect("write headers");
        stream.write_all(first).await.expect("write first chunk");
        receive_rest.await.expect("test permits remaining body");
        stream.write_all(second).await.expect("write second chunk");
    });

    let client = PdfClient::builder()
        .base_url(format!("http://{addr}"))
        .retry_policy(RetryPolicy::none())
        .build()
        .unwrap();

    let mut stream = client
        .extract_stream(request())
        .await
        .expect("stream response");
    let first = stream
        .next()
        .await
        .expect("first body chunk")
        .expect("first body chunk succeeds");

    assert!(first.starts_with(br#"{"artifact_key""#));

    send_rest.send(()).expect("allow server to finish response");
    while let Some(chunk) = stream.next().await {
        chunk.expect("remaining body chunk succeeds");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_stream_timeout_bounds_incomplete_response_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let addr = listener.local_addr().expect("server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let request = read_http_request(&mut stream).await;
        assert!(request.starts_with("POST /extract HTTP/1.1"));

        let first = br#"{"artifact_key":"artifacts/abc123","#;
        let headers = format!(
            "HTTP/1.1 200 test\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            first.len() + 128
        );
        stream
            .write_all(headers.as_bytes())
            .await
            .expect("write headers");
        stream.write_all(first).await.expect("write first chunk");
        tokio::time::sleep(Duration::from_millis(250)).await;
    });

    let client = PdfClient::builder()
        .base_url(format!("http://{addr}"))
        .http_client(reqwest::Client::new())
        .timeout(Duration::from_millis(20))
        .retry_policy(RetryPolicy::none())
        .build()
        .unwrap();

    let mut stream = client
        .extract_stream(request())
        .await
        .expect("stream response");
    stream
        .next()
        .await
        .expect("first body chunk")
        .expect("first body chunk succeeds");
    let err = stream
        .next()
        .await
        .expect("timeout chunk")
        .expect_err("incomplete streamed body should time out");

    assert!(matches!(err, PdfClientError::Timeout { .. }));
    assert_eq!(err.class(), ErrorClass::Transient);
    assert!(stream.next().await.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn extract_emits_outbound_http_span() {
    let body = r#"{
      "artifact_key": "artifacts/abc123",
      "backend": {
        "kind": "deterministic",
        "name": "camelot",
        "version": "1.0.0",
        "model_sha256": null
      },
      "tables": []
    }"#;
    let (base_url, _attempts) = serve_responses(vec![body], vec![200]).await;
    let client = PdfClient::builder()
        .base_url(base_url)
        .retry_policy(RetryPolicy::none())
        .build()
        .unwrap();
    let span_count = Arc::new(AtomicUsize::new(0));
    let subscriber = tracing_subscriber::registry().with(SpanCounter {
        name: "HTTP request",
        count: Arc::clone(&span_count),
    });
    let _guard = tracing::subscriber::set_default(subscriber);

    client.extract(request()).await.expect("extract");

    assert_eq!(span_count.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_timeout_bounds_hung_sidecar_calls() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let addr = listener.local_addr().expect("server address");
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_task = Arc::clone(&attempts);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let request = read_http_request(&mut stream).await;
        assert!(request.starts_with("POST /extract HTTP/1.1"));
        attempts_for_task.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(250)).await;
    });

    let client = PdfClient::builder()
        .base_url(format!("http://{addr}"))
        .timeout(Duration::from_millis(20))
        .retry_policy(RetryPolicy::none())
        .build()
        .unwrap();

    let err = client
        .extract(request())
        .await
        .expect_err("hung sidecar should time out");

    assert_eq!(err.class(), ErrorClass::Transient);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extract_timeout_bounds_incomplete_response_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let addr = listener.local_addr().expect("server address");
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_task = Arc::clone(&attempts);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let request = read_http_request(&mut stream).await;
        assert!(request.starts_with("POST /extract HTTP/1.1"));
        attempts_for_task.fetch_add(1, Ordering::SeqCst);

        let body = br#"{"artifact_key":"artifacts/abc123","#;
        let response = format!(
            "HTTP/1.1 200 test\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len() + 128
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");
        stream.write_all(body).await.expect("write partial body");
        tokio::time::sleep(Duration::from_millis(250)).await;
    });

    let client = PdfClient::builder()
        .base_url(format!("http://{addr}"))
        .http_client(reqwest::Client::new())
        .timeout(Duration::from_millis(20))
        .retry_policy(RetryPolicy::none())
        .build()
        .unwrap();

    let err = client
        .extract(request())
        .await
        .expect_err("incomplete body should time out");

    assert!(matches!(err, PdfClientError::Timeout { .. }));
    assert_eq!(err.class(), ErrorClass::Transient);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_http_client_still_enforces_request_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let addr = listener.local_addr().expect("server address");
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_task = Arc::clone(&attempts);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let request = read_http_request(&mut stream).await;
        assert!(request.starts_with("POST /extract HTTP/1.1"));
        attempts_for_task.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(250)).await;
    });

    let client = PdfClient::builder()
        .base_url(format!("http://{addr}"))
        .http_client(reqwest::Client::new())
        .timeout(Duration::from_millis(20))
        .retry_policy(RetryPolicy::none())
        .build()
        .unwrap();

    let err = client
        .extract(request())
        .await
        .expect_err("hung sidecar should time out");

    assert!(matches!(err, PdfClientError::Timeout { .. }));
    assert_eq!(err.class(), ErrorClass::Transient);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}
