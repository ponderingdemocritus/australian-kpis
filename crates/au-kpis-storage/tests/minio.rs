//! Integration tests for `au-kpis-storage` against a real MinIO backend.
//!
//! The spec calls for content-addressed, idempotent artifact writes and
//! streaming reads and writes (`Spec.md § Ingestion pipeline`, issue
//! #8). Asserting those contracts against the real `AmazonS3` client —
//! pointed at a MinIO container — catches SigV4 / path-style /
//! conditional-write / multipart regressions that a pure in-memory stub
//! would not.
//!
//! Requires a working Docker daemon. In CI the job runs against the
//! default socket; locally `colima start` or Docker Desktop is enough.

use std::{sync::Arc, time::Duration};

use au_kpis_domain::ids::ArtifactId;
use au_kpis_storage::{BlobStore, StorageError, StorageKey};
use bytes::Bytes;
use futures::{StreamExt, stream};
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
    let id = blob.put_artifact(content.clone()).await.expect("put");

    // Id matches the sha256 of the bytes we wrote, computed without
    // going through the blob store.
    assert_eq!(id, ArtifactId::of_content(&content));

    // Distinct content → distinct id.
    let other = Bytes::from_static(b"the quick brown foz");
    let other_id = blob.put_artifact(other).await.expect("put other");
    assert_ne!(id, other_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_artifact_is_idempotent_and_does_not_rewrite() {
    let h = start_minio().await;
    let blob = BlobStore::from_arc(Arc::clone(&h.store));

    let content = Bytes::from_static(b"idempotent payload");
    let id = blob.put_artifact(content.clone()).await.expect("put 1");

    let key = StorageKey::canonical_for(&id);
    let canonical = object_store::path::Path::from(key.as_str());
    let meta_first = h
        .store
        .head(&canonical)
        .await
        .expect("head after first put");

    // Second put of identical content: same id, same backend object.
    let id_again = blob.put_artifact(content.clone()).await.expect("put 2");
    assert_eq!(id, id_again);

    let meta_second = h
        .store
        .head(&canonical)
        .await
        .expect("head after second put");

    // `last_modified` and `e_tag` would both change if the backend
    // actually received a second PUT — skipping the re-write keeps
    // them stable, which is the "no rewrite" contract.
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
    let id = blob.put_artifact(content.clone()).await.expect("put");

    let key = StorageKey::canonical_for(&id);
    let mut out = blob.get(&key).await.expect("get stream");
    let mut buf = Vec::with_capacity(content.len());
    while let Some(chunk) = out.next().await {
        buf.extend_from_slice(&chunk.expect("chunk"));
    }
    assert_eq!(buf.len(), content.len());
    assert_eq!(Bytes::from(buf), content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_artifact_surfaces_not_found() {
    let h = start_minio().await;
    let blob = BlobStore::from_arc(Arc::clone(&h.store));

    let ghost_id = ArtifactId::of_content(b"never written");
    let ghost = StorageKey::canonical_for(&ghost_id);
    // `ByteStream` intentionally does not implement `Debug`, so we
    // inspect the `Result` by hand rather than going through
    // `.expect_err(...)`.
    match blob.get(&ghost).await {
        Ok(_) => panic!("unwritten key should not resolve"),
        Err(StorageError::NotFound(_)) => {}
        Err(other) => panic!("expected NotFound, got {other:?}"),
    }

    assert!(
        !blob.exists(&ghost).await.expect("exists probe"),
        "exists() on an unwritten key should return false",
    );

    // A persisted key that points outside `artifacts/` (e.g. a cold-tier
    // move) must also round-trip cleanly through `exists`/`get` — this
    // is the P1 from PR #74 about retention-tier rewrites.
    let moved = StorageKey::from_persisted("cold/never-existed");
    assert!(
        !blob.exists(&moved).await.expect("exists moved"),
        "unmoved/moved keys must both be reachable via StorageKey",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_artifact_stream_matches_single_shot_id() {
    let h = start_minio().await;
    let blob = BlobStore::from_arc(Arc::clone(&h.store));

    // Payload sized to genuinely cross S3's multipart boundary:
    // `WriteMultipart` only dispatches parts once its internal buffer
    // fills to 5 MiB, so anything smaller stays in one part and never
    // exercises `put_part` / `complete` at all. 12 MiB spans two full
    // 5 MiB parts plus a ~2 MiB trailer, matching what the adapter
    // fetch stage will see for any non-trivial upstream file.
    let total: Vec<u8> = (0u8..=255).cycle().take(12 * 1024 * 1024).collect();
    // Small producer chunks (256 KiB) also prove the internal buffer
    // coalesces sub-5 MiB writes; a naive per-chunk `put_part` would
    // trip `EntityTooSmall` here.
    let chunk_size = 256 * 1024;
    let chunks: Vec<Bytes> = total
        .chunks(chunk_size)
        .map(Bytes::copy_from_slice)
        .collect();
    let stream_chunks = stream::iter(
        chunks
            .into_iter()
            .map(Ok::<_, std::io::Error>)
            .collect::<Vec<_>>(),
    );

    let streamed = blob
        .put_artifact_stream(stream_chunks)
        .await
        .expect("streaming put");
    let direct = ArtifactId::of_content(&total);
    assert_eq!(
        streamed, direct,
        "streaming digest must match a one-shot hash of the same bytes",
    );

    // The bytes under the canonical key must match what we streamed.
    let key = StorageKey::canonical_for(&streamed);
    let mut got = Vec::with_capacity(total.len());
    let mut body = blob.get(&key).await.expect("get");
    while let Some(chunk) = body.next().await {
        got.extend_from_slice(&chunk.expect("chunk"));
    }
    assert_eq!(got, total, "stored bytes differ from the streamed input");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_artifact_stream_is_idempotent_and_cleans_staging() {
    use object_store::path::Path;

    let h = start_minio().await;
    let blob = BlobStore::from_arc(Arc::clone(&h.store));

    let content = b"streamed-idempotent".to_vec();
    let build = || {
        stream::iter(vec![Ok::<_, std::io::Error>(Bytes::copy_from_slice(
            &content,
        ))])
    };

    let first = blob.put_artifact_stream(build()).await.expect("stream 1");
    let canonical = object_store::path::Path::from(StorageKey::canonical_for(&first).as_str());
    let meta_first = h.store.head(&canonical).await.expect("head 1");

    let second = blob.put_artifact_stream(build()).await.expect("stream 2");
    assert_eq!(first, second);
    let meta_second = h.store.head(&canonical).await.expect("head 2");

    // The second streaming put discovered the canonical key already
    // existed and deleted its staged copy without overwriting the
    // canonical object. `last_modified` / `e_tag` stability proves no
    // rewrite even though the upload-and-copy path ran a second time.
    assert_eq!(meta_first.last_modified, meta_second.last_modified);
    assert_eq!(meta_first.e_tag, meta_second.e_tag);

    // Staging must be clean — no dangling `artifacts-staging/*` objects
    // after either put settles.
    let staging_prefix = Path::from("artifacts-staging");
    let mut staged = h.store.list(Some(&staging_prefix));
    let leaked: Vec<_> = std::iter::from_fn(|| {
        futures::executor::block_on(staged.next())
            .map(|r| r.expect("list staging").location.to_string())
    })
    .collect();
    assert!(
        leaked.is_empty(),
        "streaming put left staged objects behind: {leaked:?}",
    );
}
