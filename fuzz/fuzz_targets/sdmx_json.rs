#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = au_kpis_adapter_abs::parse_sdmx_json_observation_count_for_benchmark(Cursor::new(data));
});
