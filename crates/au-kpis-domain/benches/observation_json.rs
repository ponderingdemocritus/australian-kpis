use std::{collections::BTreeMap, time::Duration};

use au_kpis_domain::{
    ArtifactId, DataflowId, Observation, ObservationStatus, SeriesKey, TimePrecision,
};
use chrono::{TimeZone, Utc};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn sample_observations() -> Vec<Observation> {
    let dataflow = DataflowId::new("abs.cpi").expect("valid dataflow id");
    let artifact = ArtifactId::of_content(b"benchmark-artifact");
    let series_key = SeriesKey::derive(
        &dataflow,
        [("region", "AUS"), ("measure", "all-groups-cpi")],
    );
    let ingested_at = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("valid timestamp");

    (0..10_000)
        .map(|idx| {
            let time = Utc
                .with_ymd_and_hms(2000 + i32::from((idx % 24) as u8), 3, 1, 0, 0, 0)
                .single()
                .expect("valid timestamp");

            Observation {
                series_key,
                time,
                time_precision: TimePrecision::Quarter,
                value: Some(100.0 + f64::from(idx) / 10.0),
                status: ObservationStatus::Normal,
                revision_no: 0,
                attributes: BTreeMap::new(),
                ingested_at,
                source_artifact_id: artifact,
            }
        })
        .collect()
}

fn bench_observation_json(c: &mut Criterion) {
    let observations = sample_observations();

    c.bench_function("serialize_10k_observations_json", |b| {
        b.iter(|| serde_json::to_vec(black_box(&observations)).expect("serialize observations"));
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(40);
    targets = bench_observation_json
}
criterion_main!(benches);
