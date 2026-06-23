use std::collections::BTreeMap;

use au_kpis_adapter::{AdapterHttpClient, ArtifactRef, ParseCtx, SourceAdapter};
use au_kpis_adapter_asx::AsxAdapter;
use au_kpis_domain::{ArtifactId, SourceId};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use object_store::memory::InMemory;
use serde::Serialize;

const MARKET_STATS_HTML: &str = r#"
<h3><a id="end"></a>End of month values</h3>
<div role="tabpanel" data-cmp-data-layer="{&#34;dc:title&#34;:&#34;2026&#34;}">
  <table>
    <tbody>
      <tr><td><b>Month</b></td><td><b>All Ords price index</b></td><td><b>S&amp;P/ASX 200 price index</b></td><td><b>Total end of month market cap ($m)</b></td></tr>
      <tr><td>May</td><td>8965.0</td><td>8731.7</td><td>$3,251,220</td></tr>
      <tr><td>April</td><td>8887.6</td><td>8665.8</td><td>$3,227,932</td></tr>
    </tbody>
  </table>
</div>
<div role="tabpanel" data-cmp-data-layer="{&#34;dc:title&#34;:&#34;2025&#34;}">
  <table>
    <tbody>
      <tr><td><b>Month</b></td><td><b>All Ords price index</b></td><td><b>S&amp;P/ASX 200 price index</b></td><td><b>Total end of month market cap ($m)</b></td></tr>
      <tr><td>December</td><td>9018.8</td><td>8714.3</td><td>$3,282,379</td></tr>
    </tbody>
  </table>
</div>
<div role="tabpanel" data-cmp-data-layer="{&#34;dc:title&#34;:&#34;2020&#34;}">
  <table>
    <tbody>
      <tr><td><b>Month</b></td><td><b>All Ords Price Index</b></td><td><b>S&amp;P/ASX 200 Price Index</b></td><td><b>Dom. Equity Mkt cap $m</b></td></tr>
      <tr><td>Dec&#39;20</td><td>6850.6</td><td>6587.1</td><td>2,236,723.04</td></tr>
    </tbody>
  </table>
</div>
<div role="tabpanel" data-cmp-data-layer="{&#34;dc:title&#34;:&#34;2018&#34;}">
  <table>
    <tbody>
      <tr><th>Month</th><th>All Ords Price Index</th><th>S&amp;P/ASX 200 Price Index</th><th>Dom. Equity Mkt cap $m</th></tr>
      <tr><td>Nov18</td><td>5749.3</td><td>5667.2</td><td>1,814,596</td></tr>
    </tbody>
  </table>
</div>
<div role="tabpanel" data-cmp-data-layer="{&#34;dc:title&#34;:&#34;2017&#34;}">
  <table>
    <tbody>
      <tr><th>Month</th><th>All Ords Price Index</th><th>S&amp;P/ASX 200 Price Index</th><th>Dom. Equity Mkt cap $m</th></tr>
      <tr><td>Nov-17</td><td>6057.2</td><td>5969.9</td><td>1,893,560</td></tr>
    </tbody>
  </table>
</div>
<h3><a id="number"></a>Number of companies and securities listed on ASX</h3>
<div role="tabpanel" data-cmp-data-layer="{&#34;dc:title&#34;:&#34;2026&#34;}">
  <table>
    <tbody>
      <tr><td><b>Month</b></td><td><b>Total*</b></td><td><b>All listed entities**</b></td></tr>
      <tr><td>May</td><td>1,898</td><td>2,040</td></tr>
      <tr><td>April</td><td>1,900</td><td>2,040</td></tr>
    </tbody>
  </table>
</div>
"#;

const ANNOUNCEMENTS_RSS: &str = r#"
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:asx="https://www.asx.com.au/rss">
  <channel>
    <title>ASX announcements</title>
    <item>
      <title>BHP - Change in substantial holding</title>
      <link>https://www.asx.com.au/asxpdf/20260619/pdf/027xyz.pdf</link>
      <guid>027xyz</guid>
      <pubDate>Fri, 19 Jun 2026 05:15:00 +1000</pubDate>
      <category>Market Sensitive</category>
      <asx:code>BHP</asx:code>
    </item>
    <item>
      <title>WBC - Appendix 3Y</title>
      <link>https://www.asx.com.au/asxpdf/20260619/pdf/027abc.pdf</link>
      <guid>027abc</guid>
      <pubDate>Fri, 19 Jun 2026 06:30:00 +1000</pubDate>
      <category>Director Interest Notice</category>
      <asx:code>WBC</asx:code>
    </item>
  </channel>
</rss>
"#;

const EOD_CSV: &str = r#"date,symbol,open,high,low,close,volume
2026-06-19,BHP,42.10,42.80,41.90,42.50,12345678
2026-06-19,WBC,28.00,28.35,27.95,28.20,8765432
"#;

#[derive(Debug, Serialize)]
struct SnapshotRow {
    dataflow_id: String,
    measure_id: String,
    dimensions: BTreeMap<String, String>,
    unit: String,
    time: String,
    time_precision: String,
    value: Option<f64>,
    status: String,
    attributes: BTreeMap<String, String>,
    source_artifact_id: String,
}

async fn artifact_for(
    blob_store: &BlobStore,
    bytes: &'static [u8],
    source_url: &str,
    content_type: &str,
) -> ArtifactRef {
    let id = blob_store
        .put_artifact(Bytes::from_static(bytes))
        .await
        .expect("store fixture artifact");
    ArtifactRef {
        id,
        fetch_id: None,
        source_id: SourceId::new("asx").unwrap(),
        source_url: source_url.into(),
        content_type: content_type.into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 6, 19, 0, 0, 0).unwrap(),
    }
}

#[tokio::test]
async fn parses_asx_market_statistics_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        MARKET_STATS_HTML.as_bytes(),
        "https://www.asx.com.au/about/market-statistics/historical-market-statistics",
        "text/html",
    )
    .await;
    let adapter = AsxAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 0, 1, 0).unwrap(),
    );

    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse ASX fixture");

    let snapshot = rows
        .into_iter()
        .map(|(series, observation)| SnapshotRow {
            dataflow_id: series.dataflow_id.as_str().to_string(),
            measure_id: series.measure_id.as_str().to_string(),
            dimensions: series
                .dimensions
                .into_iter()
                .map(|(key, value)| (key.as_str().to_string(), value.as_str().to_string()))
                .collect(),
            unit: series.unit,
            time: observation.time.to_rfc3339(),
            time_precision: format!("{:?}", observation.time_precision),
            value: observation.value,
            status: format!("{:?}", observation.status),
            attributes: observation.attributes,
            source_artifact_id: observation.source_artifact_id.to_hex(),
        })
        .collect::<Vec<_>>();

    insta::assert_json_snapshot!(snapshot);
}

#[tokio::test]
async fn parses_asx_announcements_rss_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        ANNOUNCEMENTS_RSS.as_bytes(),
        "https://www.asx.com.au/asx/rss/announcements.xml",
        "application/rss+xml",
    )
    .await;
    let adapter = AsxAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 0, 0).unwrap(),
    );

    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse ASX announcements fixture");

    let snapshot = rows
        .into_iter()
        .map(|(series, observation)| SnapshotRow {
            dataflow_id: series.dataflow_id.as_str().to_string(),
            measure_id: series.measure_id.as_str().to_string(),
            dimensions: series
                .dimensions
                .into_iter()
                .map(|(key, value)| (key.as_str().to_string(), value.as_str().to_string()))
                .collect(),
            unit: series.unit,
            time: observation.time.to_rfc3339(),
            time_precision: format!("{:?}", observation.time_precision),
            value: observation.value,
            status: format!("{:?}", observation.status),
            attributes: observation.attributes,
            source_artifact_id: observation.source_artifact_id.to_hex(),
        })
        .collect::<Vec<_>>();

    insta::assert_json_snapshot!(snapshot);
}

#[tokio::test]
async fn parses_asx_eod_csv_fixture() {
    let blob_store = BlobStore::new(InMemory::new());
    let artifact = artifact_for(
        &blob_store,
        EOD_CSV.as_bytes(),
        "https://www.asx.com.au/data/eod/eod.csv",
        "text/csv",
    )
    .await;
    let adapter = AsxAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 7, 0, 0).unwrap(),
    );

    let rows = adapter
        .parse(artifact, &ctx)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse ASX EOD fixture");

    let snapshot = rows
        .into_iter()
        .map(|(series, observation)| SnapshotRow {
            dataflow_id: series.dataflow_id.as_str().to_string(),
            measure_id: series.measure_id.as_str().to_string(),
            dimensions: series
                .dimensions
                .into_iter()
                .map(|(key, value)| (key.as_str().to_string(), value.as_str().to_string()))
                .collect(),
            unit: series.unit,
            time: observation.time.to_rfc3339(),
            time_precision: format!("{:?}", observation.time_precision),
            value: observation.value,
            status: format!("{:?}", observation.status),
            attributes: observation.attributes,
            source_artifact_id: observation.source_artifact_id.to_hex(),
        })
        .collect::<Vec<_>>();

    insta::assert_json_snapshot!(snapshot);
}

#[tokio::test]
async fn parse_rejects_ambiguous_asx_provenance() {
    let blob_store = BlobStore::new(InMemory::new());
    let mut artifact = artifact_for(
        &blob_store,
        MARKET_STATS_HTML.as_bytes(),
        "https://www.asx.com.au/about/market-statistics/historical-market-statistics",
        "text/html",
    )
    .await;
    artifact.source_id = SourceId::new("aemo").unwrap();

    let adapter = AsxAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 0, 1, 0).unwrap(),
    );

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("invalid provenance should fail");

    assert!(
        err.to_string()
            .contains("ASX parse received artifact for source")
    );
}

#[tokio::test]
async fn parse_rejects_artifact_id_storage_key_mismatch() {
    let blob_store = BlobStore::new(InMemory::new());
    let actual_id = blob_store
        .put_artifact(Bytes::from_static(MARKET_STATS_HTML.as_bytes()))
        .await
        .expect("store fixture artifact");
    let wrong_id = ArtifactId::of_content(b"different ASX artifact");
    assert_ne!(actual_id, wrong_id);

    let artifact = ArtifactRef {
        id: wrong_id,
        fetch_id: None,
        source_id: SourceId::new("asx").unwrap(),
        source_url: "https://www.asx.com.au/about/market-statistics/historical-market-statistics"
            .into(),
        content_type: "text/html".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&actual_id).to_string(),
        size_bytes: MARKET_STATS_HTML.len() as u64,
        fetched_at: Utc.with_ymd_and_hms(2026, 6, 19, 0, 0, 0).unwrap(),
    };
    let adapter = AsxAdapter::default();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let ctx = ParseCtx::new(
        http,
        blob_store,
        Utc.with_ymd_and_hms(2026, 6, 19, 0, 1, 0).unwrap(),
    );

    let err = adapter
        .parse(artifact, &ctx)
        .next()
        .await
        .expect("one parse result")
        .expect_err("mismatched storage key should fail");

    assert!(
        err.to_string().contains("does not match artifact id"),
        "{err}"
    );
}
