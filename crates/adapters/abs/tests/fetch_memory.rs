#![cfg(feature = "dhat-heap")]

use std::{collections::BTreeMap, fmt, io, sync::Arc};

use au_kpis_adapter::{
    AdapterHttpClient, DiscoveredJob, FetchCtx, NoopArtifactRecorder, SourceAdapter,
};
use au_kpis_adapter_abs::AbsAdapter;
use au_kpis_domain::{DataflowId, SourceId};
use au_kpis_storage::BlobStore;
use chrono::Utc;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use object_store::{
    Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    UploadPart, path::Path,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const PAYLOAD_BYTES: usize = 500 * 1024 * 1024;
const CHUNK_BYTES: usize = 256 * 1024;
const MAX_HEAP_BYTES: usize = 50 * 1024 * 1024;

async fn serve_large_artifact_once() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).await.expect("read request");
        assert!(String::from_utf8_lossy(&request[..read]).starts_with(
            "GET /rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD HTTP/1.1"
        ));

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/vnd.sdmx.data+json\r\ncontent-length: {PAYLOAD_BYTES}\r\n\r\n",
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");

        let chunk = vec![b'x'; CHUNK_BYTES];
        let mut remaining = PAYLOAD_BYTES;
        while remaining > 0 {
            let take = remaining.min(chunk.len());
            stream
                .write_all(&chunk[..take])
                .await
                .expect("write response chunk");
            remaining -= take;
        }
    });

    format!("http://{addr}/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "500 MB DHAT memory profile for issue #25"]
async fn fetch_500mb_stays_below_50mb_peak_heap_under_dhat() {
    let profiler = dhat::Profiler::builder().testing().build();
    let source_url = serve_large_artifact_once().await;
    let base_url = source_url
        .split_once("/data/")
        .map(|(base, _)| base)
        .expect("fixture URL has ABS data path");
    let adapter = AbsAdapter::builder().base_url(base_url).build();
    let job = DiscoveredJob {
        id: "abs:CPI:2.0.0:memory".into(),
        source_id: SourceId::new("abs").unwrap(),
        dataflow_id: DataflowId::new("abs.cpi").unwrap(),
        source_url,
        metadata: BTreeMap::from([
            ("abs_dataflow_id".into(), "CPI".into()),
            ("agency_id".into(), "ABS".into()),
            ("version".into(), "2.0.0".into()),
        ]),
    };

    let artifact = adapter
        .fetch(
            job,
            &FetchCtx::new(
                AdapterHttpClient::new(adapter.manifest().rate_limit),
                BlobStore::new(NullObjectStore),
                Utc::now(),
                Arc::new(NoopArtifactRecorder),
            ),
        )
        .await
        .expect("fetch large artifact");

    let stats = dhat::HeapStats::get();
    println!(
        "dhat abs fetch: payload_bytes={} max_bytes={} total_bytes={} artifact_size={}",
        PAYLOAD_BYTES, stats.max_bytes, stats.total_bytes, artifact.size_bytes
    );
    assert_eq!(artifact.size_bytes, PAYLOAD_BYTES as u64);
    assert!(
        stats.max_bytes < MAX_HEAP_BYTES,
        "peak heap {} bytes exceeded {} byte budget",
        stats.max_bytes,
        MAX_HEAP_BYTES
    );
    drop(profiler);
}

#[derive(Debug, Default)]
struct NullObjectStore;

impl fmt::Display for NullObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("null-object-store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for NullObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        Ok(put_result())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        Ok(Box::new(NullMultipartUpload))
    }

    async fn get_opts(
        &self,
        _location: &Path,
        _options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        Err(unsupported())
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        Err(ObjectStoreError::NotFound {
            path: location.to_string(),
            source: "not persisted in null store".into(),
        })
    }

    async fn delete(&self, _location: &Path) -> ObjectStoreResult<()> {
        Ok(())
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        futures::stream::empty().boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        Ok(ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        Ok(())
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct NullMultipartUpload;

#[async_trait::async_trait]
impl MultipartUpload for NullMultipartUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        async { Ok(()) }.boxed()
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        Ok(put_result())
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        Ok(())
    }
}

fn put_result() -> PutResult {
    PutResult {
        e_tag: None,
        version: None,
    }
}

fn unsupported() -> ObjectStoreError {
    ObjectStoreError::NotSupported {
        source: io::Error::other("unused by ABS fetch memory profile").into(),
    }
}
