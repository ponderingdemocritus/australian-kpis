use std::{
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use assert_cmd::cargo::cargo_bin;
use au_kpis_testing::redis::start_redis;

#[tokio::test(flavor = "multi_thread")]
async fn api_binary_honors_sigterm() {
    let binary = cargo_bin("au-kpis-api");
    let startup_file = unique_startup_file();
    let redis = start_redis().await.expect("start redis test container");

    let mut child = Command::new(binary)
        .env("AU_KPIS_HTTP__BIND", "127.0.0.1:0")
        .env(
            "AU_KPIS_DATABASE__URL",
            "postgres://postgres:postgres@localhost/au_kpis",
        )
        .env("AU_KPIS_CACHE__URL", redis.url())
        .env("AU_KPIS_STARTUP_NOTIFY_FILE", &startup_file)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn au-kpis-api");

    let addr = wait_for_startup_file(&startup_file, &mut child);

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

fn unique_startup_file() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "au-kpis-api-startup-{}-{}.txt",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    path
}

fn wait_for_startup_file(path: &PathBuf, child: &mut std::process::Child) -> String {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        if let Ok(addr) = std::fs::read_to_string(path) {
            let addr = addr.trim().to_string();
            if !addr.is_empty() {
                let _ = std::fs::remove_file(path);
                return addr;
            }
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

    panic!("au-kpis-api never reported its bound address");
}
