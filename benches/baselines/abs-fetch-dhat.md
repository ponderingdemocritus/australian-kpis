# ABS Fetch DHAT Profile

Issue: #25

Command:

```bash
cargo test -p au-kpis-adapter-abs --features dhat-heap --test fetch_memory -- --ignored --nocapture
```

Result captured on 2026-04-30:

```text
dhat abs fetch: payload_bytes=524288000 max_bytes=6688192 total_bytes=525807653 artifact_size=524288000
test fetch_500mb_stays_below_50mb_peak_heap_under_dhat ... ok
```

The profile streams a 500 MiB local HTTP response through the real
`AbsAdapter::fetch` path into an `object_store` test backend that discards
completed parts. This isolates fetch-stage buffering from backend retention.

Budget: peak heap <50 MiB. Measured peak heap: 6,688,192 bytes.
