#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = au_kpis_adapter_rba::parse_xls_bytes_for_fuzz(data);
    let _ = au_kpis_adapter_apra::parse_xls_bytes_for_fuzz(data);
});
