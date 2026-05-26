# Issue #37 Criterion Baselines

Captured: 2026-05-26 on the local Codex workstation with Docker-backed
TimescaleDB testcontainers.

## SDMX parse bench

Command:

```bash
cargo bench -p au-kpis-adapter-abs --bench sdmx_parse --locked -- --save-baseline issue37
```

Budget: >500k observations/s.

Measured:

```text
abs_sdmx_parse/sdmx_parse_100k_observations_over_500k_obs_per_sec
time:  [77.361 ms 80.055 ms 83.359 ms]
thrpt: [1.1996 Melem/s 1.2491 Melem/s 1.2926 Melem/s]
```

## Loader COPY bench

Command:

```bash
cargo bench -p au-kpis-loader --bench copy_upsert --locked -- --save-baseline issue37
```

Budget: 10k rows <500 ms.

Measured:

```text
loader_copy/copy_upsert_10k_rows_under_500ms
time:  [240.13 ms 250.32 ms 261.08 ms]
thrpt: [38.302 Kelem/s 39.949 Kelem/s 41.645 Kelem/s]
```

## API handler overhead

Command:

```bash
cargo bench -p au-kpis-api-http --bench observations_handler --locked -- --save-baseline issue37
```

Budget: <5 ms above DB.

Measured:

```text
api handler overhead estimate above direct DB: 128.458 us

api_observations/direct_db_observations_page
time: [2.5920 ms 2.6942 ms 2.8008 ms]

api_observations/handler_observations_page_under_5ms_above_db
time: [2.6788 ms 2.7963 ms 2.9310 ms]
```

The handler median is 102 us above the direct DB benchmark; the explicit
20-sample overhead estimate printed by the bench was 128.458 us.

## CI regression gate

The PR workflow now runs Criterion on `pull_request` and `merge_group`, captures
workspace `main` and `pr` baselines, and runs:

```bash
critcmp main pr --threshold 5
```

The benchmark job is blocking through the aggregate `CI OK` gate.
