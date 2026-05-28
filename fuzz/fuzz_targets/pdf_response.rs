#![no_main]

use au_kpis_pdf_client::ExtractionResponse;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<ExtractionResponse>(data);
});
