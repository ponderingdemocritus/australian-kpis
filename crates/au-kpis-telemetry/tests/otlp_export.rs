use std::time::Duration;

use au_kpis_config::{LogFormat, TelemetryConfig};
use au_kpis_telemetry::init;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_flushes_emitted_spans_to_otlp() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buffer = vec![0_u8; 16 * 1024];
        let bytes_read = stream.read(&mut buffer).await.expect("read");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
            .await
            .expect("respond");
        let _ = tx.send(buffer[..bytes_read].to_vec());
    });

    let telemetry = init(&TelemetryConfig {
        service_name: "au-kpis-test".into(),
        log_format: LogFormat::Json,
        log_level: "info".into(),
        otlp_endpoint: Some(endpoint),
    })
    .expect("telemetry init");

    let span = tracing::info_span!("integration_span", request_id = "req-123");
    let _entered = span.enter();
    tracing::info!("sending integration span");
    drop(_entered);
    drop(span);
    drop(telemetry);

    let request = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("request timeout")
        .expect("request bytes");
    let request = String::from_utf8_lossy(&request);

    assert!(request.starts_with("POST "), "expected OTLP POST request");
    assert!(
        request.contains("\r\ncontent-type:"),
        "expected content-type header"
    );
}
