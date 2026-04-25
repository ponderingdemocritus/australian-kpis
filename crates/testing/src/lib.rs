//! Shared test harnesses for Docker-backed integration tests.
//!
//! The repository leans on `testcontainers` for real Postgres/Timescale,
//! Redis, and MinIO coverage. This crate centralizes container startup so
//! tests share the same image tags and connection-string conventions.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::fmt;

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, TestcontainersError,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
};

/// Redis helpers.
pub mod redis {
    use super::*;

    const REDIS_IMAGE: &str = "redis";
    const REDIS_TAG: &str = "7.2-alpine";
    const REDIS_PORT: u16 = 6379;

    /// Running Redis test container.
    pub struct RedisHarness {
        _container: ContainerAsync<GenericImage>,
        url: String,
    }

    impl fmt::Debug for RedisHarness {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("RedisHarness")
                .field("url", &self.url)
                .finish_non_exhaustive()
        }
    }

    impl RedisHarness {
        /// Borrow the Redis connection URL.
        pub fn url(&self) -> &str {
            &self.url
        }
    }

    /// Start a disposable Redis container and return its connection URL.
    pub async fn start_redis() -> Result<RedisHarness, TestcontainersError> {
        let container = GenericImage::new(REDIS_IMAGE, REDIS_TAG)
            .with_exposed_port(ContainerPort::Tcp(REDIS_PORT))
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await?;

        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(REDIS_PORT).await?;
        let url = format!("redis://{host}:{port}");

        Ok(RedisHarness {
            _container: container,
            url,
        })
    }
}

/// Timescale/Postgres helpers.
pub mod timescale {
    use super::*;

    const IMAGE_NAME: &str = "timescale/timescaledb";
    const IMAGE_TAG: &str = "latest-pg16";
    const POSTGRES_PORT: u16 = 5432;
    const POSTGRES_USER: &str = "postgres";
    const POSTGRES_PASSWORD: &str = "postgres";

    /// Running TimescaleDB test container.
    pub struct TimescaleHarness {
        _container: ContainerAsync<GenericImage>,
        url: String,
    }

    impl fmt::Debug for TimescaleHarness {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("TimescaleHarness")
                .field("url", &self.url)
                .finish_non_exhaustive()
        }
    }

    impl TimescaleHarness {
        /// Borrow the Postgres connection URL.
        pub fn url(&self) -> &str {
            &self.url
        }
    }

    /// Start a disposable TimescaleDB container for the provided database name.
    pub async fn start_timescale(database: &str) -> Result<TimescaleHarness, TestcontainersError> {
        let container = GenericImage::new(IMAGE_NAME, IMAGE_TAG)
            .with_exposed_port(ContainerPort::Tcp(POSTGRES_PORT))
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
            .with_env_var("POSTGRES_DB", database)
            .start()
            .await?;

        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(POSTGRES_PORT).await?;
        let url =
            format!("postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host}:{port}/{database}");

        Ok(TimescaleHarness {
            _container: container,
            url,
        })
    }
}

/// MinIO helpers.
pub mod minio {
    use super::*;

    const MINIO_IMAGE: &str = "minio/minio";
    const MINIO_TAG: &str = "RELEASE.2024-10-02T17-50-41Z";
    const MINIO_PORT: u16 = 9000;
    const ACCESS_KEY: &str = "minioadmin";
    const SECRET_KEY: &str = "minioadmin";

    /// Running MinIO test container.
    pub struct MinioHarness {
        _container: ContainerAsync<GenericImage>,
        endpoint: String,
        bucket: String,
    }

    impl fmt::Debug for MinioHarness {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MinioHarness")
                .field("endpoint", &self.endpoint)
                .field("bucket", &self.bucket)
                .finish_non_exhaustive()
        }
    }

    impl MinioHarness {
        /// Borrow the base HTTP endpoint.
        pub fn endpoint(&self) -> &str {
            &self.endpoint
        }

        /// Borrow the pre-created bucket name.
        pub fn bucket(&self) -> &str {
            &self.bucket
        }

        /// Borrow the access key.
        pub fn access_key(&self) -> &str {
            ACCESS_KEY
        }

        /// Borrow the secret key.
        pub fn secret_key(&self) -> &str {
            SECRET_KEY
        }
    }

    /// Start a disposable MinIO container and pre-create the requested bucket.
    pub async fn start_minio(
        bucket: impl Into<String>,
    ) -> Result<MinioHarness, TestcontainersError> {
        let bucket = bucket.into();
        let start_script = format!(
            "mkdir -p /data/{bucket} && exec minio server /data --address :{MINIO_PORT} --console-address :9001"
        );

        let container = GenericImage::new(MINIO_IMAGE, MINIO_TAG)
            .with_exposed_port(ContainerPort::Tcp(MINIO_PORT))
            .with_wait_for(WaitFor::message_on_stderr("API:"))
            .with_entrypoint("sh")
            .with_env_var("MINIO_ROOT_USER", ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .with_cmd(["-c", start_script.as_str()])
            .start()
            .await?;

        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(MINIO_PORT).await?;
        let endpoint = format!("http://{host}:{port}");

        Ok(MinioHarness {
            _container: container,
            endpoint,
            bucket,
        })
    }
}
