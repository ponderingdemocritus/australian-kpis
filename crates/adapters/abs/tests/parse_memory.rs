#![cfg(feature = "dhat-heap")]

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::Path,
};

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_abs::AbsAdapter;
use au_kpis_domain::{ArtifactId, Sha256Digest, SourceId};
use au_kpis_storage::BlobStore;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use sha2::{Digest, Sha256};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const PAYLOAD_BYTES: usize = 500 * 1024 * 1024;
const MAX_HEAP_BYTES: usize = 100 * 1024 * 1024;
const VALUE_FRACTION_BYTES: usize = 16 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "500 MB DHAT memory profile for issue #26"]
async fn parse_500mb_stays_below_100mb_peak_heap_under_dhat() {
    let root =
        std::env::temp_dir().join(format!("au-kpis-abs-parse-memory-{}", std::process::id()));
    let source_path = root.join("parse-large.json");
    let artifact_id =
        write_large_sdmx_fixture(&source_path, PAYLOAD_BYTES).expect("write large fixture");
    let storage_key = format!("artifacts/{}", artifact_id.to_hex());
    let artifact_path = root.join(&storage_key);
    fs::create_dir_all(
        artifact_path
            .parent()
            .expect("canonical artifact path has parent"),
    )
    .expect("create artifact dir");
    fs::rename(&source_path, &artifact_path).expect("move fixture to canonical storage key");

    let adapter = AbsAdapter::default();
    let blob_store = BlobStore::new(LocalFileSystem::new_with_prefix(&root).expect("local store"));
    let payload_bytes = fs::metadata(&artifact_path)
        .expect("large fixture metadata")
        .len();
    let artifact = ArtifactRef {
        id: artifact_id,
        source_id: SourceId::new("abs").expect("static source id is valid"),
        source_url: "https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD".into(),
        content_type: "application/vnd.sdmx.data+json".into(),
        response_headers: BTreeMap::new(),
        storage_key,
        size_bytes: payload_bytes,
        fetched_at: DateTime::parse_from_rfc3339("2024-04-24T00:00:00Z")
            .expect("valid fixture date")
            .with_timezone(&Utc),
    };
    let ctx = ParseCtx::new(
        AdapterHttpClient::new(adapter.manifest().rate_limit),
        blob_store,
        DateTime::parse_from_rfc3339("2024-04-30T00:00:00Z")
            .expect("valid fixture date")
            .with_timezone(&Utc),
    );

    let profiler = dhat::Profiler::builder().testing().build();
    let mut stream = adapter.parse(artifact, &ctx);
    let mut observations = 0_u64;
    while let Some(next) = stream.next().await {
        next.expect("parse large fixture row");
        observations += 1;
    }

    let stats = dhat::HeapStats::get();
    println!(
        "dhat abs parse: payload_bytes={} max_bytes={} total_bytes={} observations={}",
        payload_bytes, stats.max_bytes, stats.total_bytes, observations
    );
    assert!(payload_bytes >= PAYLOAD_BYTES as u64);
    assert!(observations > 1_000);
    assert!(
        stats.max_bytes < MAX_HEAP_BYTES,
        "peak heap {} bytes exceeded {} byte budget",
        stats.max_bytes,
        MAX_HEAP_BYTES
    );
    drop(profiler);
    fs::remove_dir_all(root).expect("remove large fixture dir");
}

fn write_large_sdmx_fixture(path: &Path, target_bytes: usize) -> io::Result<ArtifactId> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let mut writer = CountingWriter::new(BufWriter::new(file));

    let observation_count = (target_bytes / (VALUE_FRACTION_BYTES + 32)).max(1_001) + 16;
    let fractional_digits = vec![b'0'; VALUE_FRACTION_BYTES];

    writer.write_all(br#"{"data":{"structure":{"dimensions":{"series":[{"id":"REGION","values":[{"id":"AUS"}]},{"id":"MEASURE","values":[{"id":"INDEX"}]}],"observation":[{"id":"TIME_PERIOD","values":["#)?;
    for index in 0..observation_count {
        if index > 0 {
            writer.write_all(b",")?;
        }
        write!(writer, r#"{{"id":"2024-Q{}"}}"#, (index % 4) + 1)?;
    }
    writer.write_all(br#"]}]},"attributes":{"observation":[{"id":"OBS_STATUS","values":[{"id":"A"}]}]}},"dataSets":[{"series":{"0:0":{"observations":{"#)?;
    for index in 0..observation_count {
        if index > 0 {
            writer.write_all(b",")?;
        }
        write!(writer, r#""{index}":[1."#)?;
        writer.write_all(&fractional_digits)?;
        writer.write_all(b",0]")?;
    }
    writer.write_all(br#"}}}}]}}"#)?;
    writer.flush()?;
    Ok(writer.artifact_id())
}

struct CountingWriter<W> {
    inner: W,
    bytes_written: usize,
    hasher: Sha256,
}

impl<W> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            bytes_written: 0,
            hasher: Sha256::new(),
        }
    }

    fn artifact_id(self) -> ArtifactId {
        ArtifactId::from_digest(Sha256Digest::from_bytes(self.hasher.finalize().into()))
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.bytes_written += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
