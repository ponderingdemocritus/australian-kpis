use std::{io::Cursor, time::Duration};

use au_kpis_adapter_abs::parse_sdmx_json_observation_count_for_benchmark;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};

const SERIES_COUNT: usize = 100;
const PERIOD_COUNT: usize = 1_000;
const OBSERVATION_COUNT: usize = SERIES_COUNT * PERIOD_COUNT;

fn synthetic_sdmx_fixture() -> Vec<u8> {
    let mut json = String::with_capacity(OBSERVATION_COUNT * 18);
    json.push_str(r#"{"data":{"structure":{"dimensions":{"series":[{"id":"REGION","values":["#);
    for region in 0..SERIES_COUNT {
        if region > 0 {
            json.push(',');
        }
        json.push_str(&format!(r#"{{"id":"R{region:03}"}}"#));
    }
    json.push_str(
        r#"]},{"id":"MEASURE","values":[{"id":"INDEX"}]}],"observation":[{"id":"TIME_PERIOD","values":["#,
    );
    for period in 0..PERIOD_COUNT {
        if period > 0 {
            json.push(',');
        }
        let year = 2000 + period / 12;
        let month = 1 + period % 12;
        json.push_str(&format!(r#"{{"id":"{year:04}-{month:02}"}}"#));
    }
    json.push_str(r#"]}]}},"dataSets":[{"series":{"#);
    for region in 0..SERIES_COUNT {
        if region > 0 {
            json.push(',');
        }
        json.push_str(&format!(r#""{region}:0":{{"observations":{{"#));
        for period in 0..PERIOD_COUNT {
            if period > 0 {
                json.push(',');
            }
            let value = 100.0 + f64::from(region as u32) + f64::from(period as u32) / 100.0;
            json.push_str(&format!(r#""{period}":[{value:.2}]"#));
        }
        json.push_str("}}");
    }
    json.push_str("}}]}}");
    json.into_bytes()
}

fn bench_sdmx_parse(c: &mut Criterion) {
    let fixture = synthetic_sdmx_fixture();
    let parsed = parse_sdmx_json_observation_count_for_benchmark(Cursor::new(&fixture))
        .expect("parse benchmark fixture");
    assert_eq!(parsed, OBSERVATION_COUNT);

    let mut group = c.benchmark_group("abs_sdmx_parse");
    group.throughput(Throughput::Elements(OBSERVATION_COUNT as u64));
    group.bench_function("sdmx_parse_100k_observations_over_500k_obs_per_sec", |b| {
        b.iter(|| {
            let parsed = parse_sdmx_json_observation_count_for_benchmark(Cursor::new(&fixture))
                .expect("parse benchmark fixture");
            black_box(parsed);
            assert_eq!(parsed, OBSERVATION_COUNT);
        });
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20);
    targets = bench_sdmx_parse
}
criterion_main!(benches);
