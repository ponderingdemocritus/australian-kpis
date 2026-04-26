use std::{
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use assert_cmd::cargo::cargo_bin;

#[test]
fn api_binary_honors_sigterm() {
    let addr = reserve_local_addr();
    let binary = cargo_bin("au-kpis-api");

    let mut child = Command::new(binary)
        .env("AU_KPIS_HTTP__BIND", &addr)
        .env(
            "AU_KPIS_DATABASE__URL",
            "postgres://postgres:postgres@localhost/au_kpis",
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn au-kpis-api");

    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        if std::net::TcpStream::connect(&addr).is_ok() {
            break;
        }
        if child
            .try_wait()
            .expect("poll child during startup")
            .is_some()
        {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    assert!(
        std::net::TcpStream::connect(&addr).is_ok(),
        "au-kpis-api never became ready on {addr}"
    );

    let kill = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(kill.success(), "SIGTERM failed: {kill:?}");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            assert!(
                status.success(),
                "au-kpis-api exited unsuccessfully: {status}"
            );
            break;
        }

        assert!(
            Instant::now() < deadline,
            "au-kpis-api did not exit within 5s of SIGTERM"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn reserve_local_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr.to_string()
}
