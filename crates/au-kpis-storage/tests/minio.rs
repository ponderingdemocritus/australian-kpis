//! Integration tests for `au-kpis-storage` against a real MinIO backend.
//!
//! The spec calls for content-addressed, idempotent artifact writes and
//! streaming reads (`Spec.md § Ingestion pipeline`, issue #8). Asserting
//! those contracts against the real `AmazonS3` client — pointed at a
//! MinIO container — catches SigV4 / path-style / conditional-write
//! regressions that a pure in-memory stub would not.
//!
//! Requires a working Docker daemon. In CI the job runs against the
//! default socket; locally `colima start` or Docker Desktop is enough.

use std::{sync::Arc, time::Duration};

use au_kpis_storage::{BlobStore, Sha256Key, StorageError};
use bytes::Bytes;
use futures::StreamExt;
use object_store::{ObjectStore, aws::AmazonS3Builder};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
};

const MINIO_IMAGE: &str = "minio/minio";
const MINIO_TAG: &str = "RELEASE.2024-10-02T17-50-41Z";
const MINIO_PORT: u16 = 9000;
const BUCKET: &str = "au-kpis-test";
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";

struct Harness {
    // Holds the container alive for the lifetime of the test.
    _container: ContainerAsync<GenericImage>,
    store: Arc<dyn ObjectStore>,
}

async fn start_minio() -> Harness {
    // MinIO's official image does not offer an env-var bucket
    // bootstrap; overriding the entrypoint to `sh -c` lets us
    // pre-create the bucket directory (MinIO treats top-level dirs
    // under the data path as buckets) before starting the server.
    let start_script = format!(
        "mkdir -p /data/{BUCKET} && exec minio server /data --address :{MINIO_PORT} --console-address :9001"
    );
    let container = GenericImage::new(MINIO_IMAGE, MINIO_TAG)
        .with_exposed_port(ContainerPort::Tcp(MINIO_PORT))
        // MinIO writes all its startup banner to stderr (including the
        // "API:" line), so that's the only stream worth watching.
        .with_wait_for(WaitFor::message_on_stderr("API:"))
        .with_entrypoint("sh")
        .with_env_var("MINIO_ROOT_USER", ACCESS_KEY)
        .with_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
        .with_cmd(["-c", start_script.as_str()])
        .start()
        .await
        .expect("start minio container");

    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(MINIO_PORT)
        .await
        .expect("host port");
    let endpoint = format!("http://{host}:{port}");

    let store = build_store(&endpoint);
    let store: Arc<dyn ObjectStore> = Arc::new(store);

    // MinIO is ready the moment it logs the API line, but the first
    // SigV4 request still races against the HTTP listener binding.
    // A short probe loop avoids the flake.
    wait_for_ready(&store).await;

    Harness {
        _container: container,
        store,
    }
}

fn build_store(endpoint: &str) -> object_store::aws::AmazonS3 {
    AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_region("us-east-1")
        .with_bucket_name(BUCKET)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("build AmazonS3")
}

async fn wait_for_ready(store: &Arc<dyn ObjectStore>) {
    let probe = object_store::path::Path::from("readiness-probe");
    for _ in 0..20 {
        // A list() call is cheap and succeeds as soon as the bucket is
        // reachable; NotFound on an individual key would also be fine,
        // but the list form also validates credentials.
        let mut stream = store.list(Some(&probe));
        if stream.next().await.is_none() || stream.next().await.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("minio did not become ready within the retry window");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_artifact_is_content_addressed() {
    let h = start_minio().await;
    let blob = BlobStore::from_arc(Arc::clone(&h.store));

    let content = Bytes::from_static(b"the quick brown fox");
    let key = blob.put_artifact(content.clone()).await.expect("put");

    // Key matches the sha256 of the bytes we wrote, computed without
    // going through the blob store.
    let expected = Sha256Key::from_content(&content);
    assert_eq!(key, expected);

    // Distinct content → distinct key.
    let other = Bytes::from_static(b"the quick brown foz");
    let other_key = blob.put_artifact(other).await.expect("put other");
    assert_ne!(key, other_key);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_artifact_is_idempotent_and_does_not_rewrite() {
    let h = start_minio().await;
    let blob = BlobStore::from_arc(Arc::clone(&h.store));

    let content = Bytes::from_static(b"idempotent payload");
    let key = blob.put_artifact(content.clone()).await.expect("put 1");

    let meta_first = h
        .store
        .head(&key.to_object_path())
        .await
        .expect("head after first put");

    // Second put of identical content: same key, same backend object.
    let key_again = blob.put_artifact(content.clone()).await.expect("put 2");
    assert_eq!(key, key_again);

    let meta_second = h
        .store
        .head(&key.to_object_path())
        .await
        .expect("head after second put");

    // `last_modified` and `e_tag` would both change if the backend
    // actually received a second PUT — the conditional-write guard
    // keeps them stable, which is the "no rewrite" contract.
    assert_eq!(
        meta_first.last_modified, meta_second.last_modified,
        "last_modified changed — backend was rewritten",
    );
    assert_eq!(
        meta_first.e_tag, meta_second.e_tag,
        "e_tag changed — backend was rewritten",
    );
    assert_eq!(meta_first.size, meta_second.size);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_artifact_streams_full_payload() {
    let h = start_minio().await;
    let blob = BlobStore::from_arc(Arc::clone(&h.store));

    // A payload comfortably larger than a single chunk so the stream
    // actually yields multiple `Bytes` values on the happy path.
    let content = Bytes::from_iter((0u8..=255).cycle().take(64 * 1024));
    let key = blob.put_artifact(content.clone()).await.expect("put");

    let mut stream = blob.get_artifact(&key).await.expect("get stream");
    let mut buf = Vec::with_capacity(content.len());
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk.expect("chunk"));
    }
    assert_eq!(buf.len(), content.len());
    assert_eq!(Bytes::from(buf), content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_artifact_surfaces_not_found() {
    let h = start_minio().await;
    let blob = BlobStore::from_arc(Arc::clone(&h.store));

    let ghost = Sha256Key::from_content(b"never written");
    // `ByteStream` intentionally does not implement `Debug`, so we
    // inspect the `Result` by hand rather than going through
    // `.expect_err(...)`.
    match blob.get_artifact(&ghost).await {
        Ok(_) => panic!("unwritten key should not resolve"),
        Err(StorageError::NotFound(_)) => {}
        Err(other) => panic!("expected NotFound, got {other:?}"),
    }

    assert!(
        !blob.contains(&ghost).await.expect("contains probe"),
        "contains() on an unwritten key should return false",
    );
}
