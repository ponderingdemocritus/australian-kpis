#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use tokio::runtime::{Builder, Runtime};

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_current_thread()
            .build()
            .expect("build current-thread fuzz runtime")
    })
}

fuzz_target!(|data: &[u8]| {
    let _ = runtime().block_on(au_kpis_adapter_rba::parse_csv_bytes_for_fuzz(data));
});
