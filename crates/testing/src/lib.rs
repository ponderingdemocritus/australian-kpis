#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]

use std::{future::Future, time::Duration};

use testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
};

pub const MINIO_ACCESS_KEY: &str = "minioadmin";
pub const MINIO_API_PORT: u16 = 9000;
pub const MINIO_IMAGE: &str = "minio/minio";
pub const MINIO_SECRET_KEY: &str = "minioadmin";
pub const MINIO_TAG: &str = "RELEASE.2024-10-02T17-50-41Z";
pub const TIMESCALE_IMAGE: &str = "timescale/timescaledb";
pub const TIMESCALE_PORT: u16 = 5432;
pub const TIMESCALE_TAG: &str = "latest-pg16";

pub async fn endpoint_for(container: &ContainerAsync<GenericImage>, port: u16) -> String {
    let host = container.get_host().await.expect("container host");
    let port = container.get_host_port_ipv4(port).await.expect("host port");
    format!("http://{host}:{port}")
}

pub fn minio_image(bucket: &str) -> ContainerRequest<GenericImage> {
    let start_script = format!(
        "mkdir -p /data/{bucket} && exec minio server /data --address :{MINIO_API_PORT} --console-address :9001"
    );
    GenericImage::new(MINIO_IMAGE, MINIO_TAG)
        .with_exposed_port(ContainerPort::Tcp(MINIO_API_PORT))
        .with_wait_for(WaitFor::message_on_stderr("API:"))
        .with_entrypoint("sh")
        .with_env_var("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
        .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
        .with_cmd(vec!["-c".to_string(), start_script])
}

pub async fn postgres_connection_string(
    container: &ContainerAsync<GenericImage>,
    database: &str,
) -> String {
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(TIMESCALE_PORT)
        .await
        .expect("host port");
    format!("postgres://postgres:postgres@{host}:{port}/{database}")
}

pub async fn retry_async<T, E, F, Fut>(attempts: usize, delay: Duration, mut op: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    assert!(attempts > 0, "retry_async requires at least one attempt");

    for attempt in 0..attempts {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt + 1 == attempts => return Err(err),
            Err(_) => tokio::time::sleep(delay).await,
        }
    }

    unreachable!("attempts > 0 guarantees a return")
}

pub fn timescale_image(database: &str) -> ContainerRequest<GenericImage> {
    GenericImage::new(TIMESCALE_IMAGE, TIMESCALE_TAG)
        .with_exposed_port(ContainerPort::Tcp(TIMESCALE_PORT))
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", database)
}
