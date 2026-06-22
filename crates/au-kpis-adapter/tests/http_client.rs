use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use au_kpis_adapter::{AdapterHttpClient, RateLimit};

#[tokio::test]
async fn execute_retries_transient_send_errors_for_cloneable_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");
    let attempts = Arc::new(AtomicUsize::new(0));
    let server_attempts = Arc::clone(&attempts);

    thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("accept request");
            let attempt = server_attempts.fetch_add(1, Ordering::SeqCst);
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).expect("read request");
            if attempt == 0 {
                continue;
            }

            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                .expect("write response");
        }
    });

    let client = AdapterHttpClient::new(RateLimit::new(10_000, Duration::from_secs(1)).unwrap());
    let response = client
        .execute(client.raw().get(format!("http://{addr}/artifact.xlsx")))
        .await
        .expect("retry should recover from the first dropped connection");

    assert!(response.status().is_success());
    assert_eq!(response.text().await.expect("response body"), "ok");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}
