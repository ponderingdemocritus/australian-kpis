# Full-Stack Verification Notes - 2026-06-19

## Scope

Goal under test:

- run the local application stack;
- test implemented ingestion sources;
- verify API data flow from ingestion/storage/database to HTTP and web UI;
- migrate the web app to Next.js 16, Tailwind 4, and shadcn-compatible setup.

## Verified Working

- Local Postgres, Redis, and MinIO start with
  `docker compose -f infra/compose/docker-compose.yml up -d --no-build --wait postgres redis minio`.
  The current local default sets `GOMEMLIMIT=64MiB` and `GOGC=50` after the
  Docker VM OOM-killed uncapped and 256 MiB MinIO attempts under the 1.9 GiB
  memory limit. The embedded MinIO console is disabled with `MINIO_BROWSER=off`,
  and the background scanner is slowed with `MINIO_SCANNER_SPEED=slowest`; the
  app only needs the S3 API locally. A later MinIO restart failed again with
  exit 137 because 16 orphaned `org.testcontainers.managed-by=testcontainers`
  containers left only about 61 MiB available in the Docker VM. Removing only
  those disposable testcontainers raised available memory to about 476 MiB, and
  MinIO then started healthy. After the latest restart and S3 smoke, MinIO stayed
  healthy with `OOMKilled=false` and roughly 218 MiB RSS.
- Local compose now uses `au-kpis-artifacts` as the canonical object-store
  bucket for ingestion and the PDF sidecar. The previous compose default used
  `au-kpis-local`, while the live-loaded ABS/AEMO/state-budget/Treasury
  artifacts and demo docs used `au-kpis-artifacts`; the database stores only
  storage keys, not bucket names, so mixed buckets made local artifact
  resolution inconsistent.
- Current local object-store consistency proof:
  - all 12 database `artifacts.storage_key` rows resolve in
    `au-kpis-artifacts`;
  - S3 `head-object` byte sizes match the database `artifacts.size_bytes`
    values for ABS, AEMO, APRA, ASX, state budgets, and Treasury artifacts;
  - 120 repeated `head-object` probes across all 12 artifact keys completed
    successfully;
  - a 2026-06-20 rerun verified all 12 artifact objects again after cleaning up
    orphaned testcontainers;
  - MinIO health stayed ready after the probe.
- Database migrations apply with
  `DATABASE_URL=postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis sqlx migrate run --source infra/migrations`.
- Live ABS CPI ingestion now completes against local Postgres and MinIO after
  updating the ABS media type, live CPI catalog seed, and staged loader commit
  path. The clean run loaded:
  - 1 artifact;
  - 8,467 series;
  - 319,944 observations;
  - 0 parse errors.
- Live APRA ingestion now discovers the current `/system/files/...xlsx` release
  links, parses current and historical workbooks, and loaded committed APRA
  observations before the local low-memory database killed the final cleanup
  statement. Persisted APRA counts after restart:
  - 4 artifacts;
  - 6,943 series;
  - 194,776 observations;
  - first observation `2004-01-01`;
  - last observation `2025-10-01`.
- Live ASX market-statistics ingestion now completes against local Postgres and
  MinIO:
  - 2 artifacts from the live verification attempts;
  - 5 series;
  - 625 observations;
  - first observation `2016-01-01`;
  - last observation `2026-05-01`.
- Live AEMO dispatch ingestion now completes for an official NEMWeb DispatchIS
  ZIP selected from a one-file local listing:
  - 1 artifact;
  - 10 series;
  - 10 observations;
  - observation time `2026-06-19 17:05:00+00`.
    A rerun after the MinIO memory fix wrote the ZIP artifact to
    `au-kpis-artifacts` and the API returned 10 minute-precision rows across
    `NSW1`, `QLD1`, `SA1`, `TAS1`, and `VIC1`.
- The API serves live ABS CPI data for:
  - `GET /v1/health`;
  - `GET /v1/dataflows`;
  - `GET /v1/dataflows/abs.cpi/codelists/region`;
  - `GET /v1/observations?dataflow=abs.cpi&limit=3`;
  - `GET /v1/observations?dataflow=abs.cpi&dimensions[measure]=1&dimensions[index]=10001&dimensions[tsest]=10&dimensions[region]=50&dimensions[freq]=Q&limit=5`;
  - `GET /v1/search?q=cpi&limit=5`.
- API catalog search now treats dataflow-name acronyms as direct matches. A
  current `GET /v1/search?q=CPI&limit=8` returns `abs.cpi` /
  `Consumer Price Index` as the first result, ahead of CPI-related measure
  matches. Regression coverage:
  `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-api-http --test search search_catalog_matches_dataflow_acronyms -- --nocapture`.
- The TypeScript SDK exposes `client.search.catalog()` for `/v1/search`, and
  `pnpm --filter @au-kpis/sdk test` covers URL construction and response return.
- The API serves live APRA data for:
  - `GET /v1/dataflows/apra.quarterly_statistics`;
  - `GET /v1/observations?dataflow=apra.quarterly_statistics&limit=3`;
  - `GET /v1/observations?dataflow=apra.quarterly_statistics&since=2025-01-01&limit=3`;
  - `GET /v1/series/apra.quarterly_statistics/{series_key}`.
- The API serves live ASX and AEMO data for:
  - `GET /v1/dataflows/asx.market_statistics`;
  - `GET /v1/dataflows/aemo.dispatch`;
  - `GET /v1/observations?dataflow=asx.market_statistics&limit=3`;
  - `GET /v1/observations?dataflow=aemo.dispatch&limit=3`;
  - `GET /v1/search?q=ASX&limit=5`;
  - `GET /v1/search?q=AEMO&limit=5`.
- The API serves live state-budget data for:
  - `GET /v1/dataflows/state_budgets.nsw_budget`;
  - `GET /v1/dataflows/state_budgets.vic_budget`;
  - `GET /v1/dataflows/state_budgets.qld_budget`;
  - `GET /v1/observations?dataflow=state_budgets.nsw_budget&limit=3`;
  - `GET /v1/observations?dataflow=state_budgets.vic_budget&limit=3`;
  - `GET /v1/observations?dataflow=state_budgets.qld_budget&limit=3`.
- A post-MinIO-fix API smoke on `127.0.0.1:3013` returned:
  - health `{"status":"ok"}`;
  - 9 dataflows;
  - 3 filtered ABS CPI observations;
  - 3 APRA observations since `2025-01-01`;
  - 3 ASX market-statistics observations;
  - 10 AEMO dispatch observations, all `minute` precision;
  - search results for `market` including `asx.market_statistics` and
    `aemo.dispatch`.
- A later full-service API/object-store smoke with Postgres, Redis, and MinIO
  healthy verified all currently loaded sources:
  - `abs.cpi`, `aemo.dispatch`, `apra.quarterly_statistics`,
    `asx.market_statistics`, `state_budgets.nsw_budget`,
    `state_budgets.qld_budget`, `state_budgets.vic_budget`, and
    `treasury.budget_papers` each returned 3 API observations with
    `source_artifact_id`, units, and time precision populated.
  - `rba.statistical_tables` remains catalog-only with 0 series because the
    current environment still receives upstream RBA HTTP 403 responses.
  - The eight sampled `source_artifact_id` values all resolved to database
    artifact rows with `artifacts/<sha256>` storage keys.
  - The same eight storage keys were present in MinIO via S3 `head-object`;
    object byte sizes matched the database `size_bytes` values.
- Latest observation reads no longer use the global `observations_latest` view
  for the API hot path. The handler first counts matching series, rejects
  high-cardinality latest reads at 512 or more matching series, then loads the
  bounded candidate series metadata and reads observations by concrete
  `series_key` values. Exact one-series requests use `obs.series_key = $1`,
  which avoids the low-memory query plans that killed Postgres during broad CPI
  and filtered ABS probes. Latest reads now fetch a bounded raw window ordered by
  time, series key, and descending revision, then deduplicate adjacent revisions
  in Rust. If older revisions consume the raw overfetch window, the handler
  advances an internal fetch cursor and continues until it has enough unique
  observations or exhausts the result set. This preserves API cursor semantics
  without introducing the expensive `DISTINCT ON` plan that destabilized the
  low-memory local Timescale/Postgres VM.
- High-cardinality latest reads now fail fast with a user-actionable 400. The
  current broad CPI smoke returned:
  - request: `GET /v1/observations?dataflow=abs.cpi&limit=500`;
  - HTTP status: `400`;
  - detail: observations query for dataflow `abs.cpi` matches 8467 series; add
    more `dimensions[]` filters before reading observations.
    This preserves service health instead of admitting a request shape that can
    exhaust the local Timescale/Postgres VM.
- Two database migrations support the safer local read path:
  - `0009_observation_read_indexes` adds a lightweight partial series index for
    observed dataflow reads without building another large index on the
    Timescale hypertable.
  - `0010_delay_rollup_refresh_jobs` moves Timescale continuous-aggregate
    refresh jobs out by one hour after migration so fresh local/test stacks do
    not immediately race rollback verification or compete with startup smoke.
- Local migration state is now at version 10. Verified with:
  - `DATABASE_URL=postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis sqlx migrate run --source infra/migrations`;
  - `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-db --test migrations -- --nocapture`.
- Latest exact-series live API smoke against `127.0.0.1:3013` returned:
  - `abs.cpi`: HTTP 200, 2 rows, first artifact
    `8522beab11c052753b39dc558f9c03eace6079a17d9d4e2fbac864908f38d3ac`;
  - `aemo.dispatch`: HTTP 200, 1 row, first artifact
    `9f746b2f6a20327beb1e88d312c28aade3c82ffce605d0e37c34a58d7ca5dae8`;
  - `apra.quarterly_statistics`: HTTP 200, 1 row, first artifact
    `82da02f9d51049d45383dfbb771138bd62308afe604fc9b0b9c2b2448e2a2c0d`;
  - `asx.market_statistics`: HTTP 200, 3 rows, first artifact
    `cd233346758cc59769c9e3220e1b608ef76f52152786878a02394e1aa9b4dc79`;
  - `state_budgets.nsw_budget`: HTTP 200, 3 rows, first artifact
    `e9155d5c9de596675b24ae7477c497ff22fd74cc55df3c3115eea73fa157a2ee`;
  - `state_budgets.qld_budget`: HTTP 200, 3 rows, first artifact
    `9cf28ec0851ce38e3e3afb216a41132c2225c13af187679fac015d8e93dab699`;
  - `state_budgets.vic_budget`: HTTP 200, 3 rows, first artifact
    `e2f3245a52d6ff11cf6d4f5db31a6637af6e4858e7dec626498d837fd22afbbc`;
  - `treasury.budget_papers`: HTTP 200, 2 rows, first artifact
    `ca5e9b60c3c215eadcf64861ea16a8217261a0c858297acffce834698bde52ed`.
    Later Docker log checks still showed local resource pressure outside the
    request shape itself: a Postgres backend was killed by signal 9 at
    `2026-06-19T23:33:03Z` during broader concurrent verification, and a
    Timescale continuous-aggregate background worker was killed by signal 9 at
    `2026-06-20T02:31:44Z` while the API was left running. The fixed
    exact-series API shape and serial observations integration suite pass, but
    full-stack local stability still depends on the Docker VM memory budget,
    background job scheduling, and test concurrency.
- A 2026-06-20 bounded per-source API rerun selected one `series` row per
  dataflow without scanning the `observations` hypertable, converted the series
  dimensions into public query parameters, and returned:
  - `abs.cpi`: HTTP 200, 2 rows, quarter precision, first artifact
    `8522beab11c052753b39dc558f9c03eace6079a17d9d4e2fbac864908f38d3ac`;
  - `aemo.dispatch`: HTTP 200, 1 row, minute precision, first artifact
    `9f746b2f6a20327beb1e88d312c28aade3c82ffce605d0e37c34a58d7ca5dae8`;
  - `apra.quarterly_statistics`: HTTP 200, 1 row, quarter precision, first
    artifact `82da02f9d51049d45383dfbb771138bd62308afe604fc9b0b9c2b2448e2a2c0d`;
  - `asx.market_statistics`: HTTP 200, 3 rows, month precision, first artifact
    `cd233346758cc59769c9e3220e1b608ef76f52152786878a02394e1aa9b4dc79`;
  - `rba.statistical_tables`: HTTP 200, 0 rows, catalog-only;
  - `state_budgets.nsw_budget`: HTTP 200, 3 rows, year precision, first artifact
    `e9155d5c9de596675b24ae7477c497ff22fd74cc55df3c3115eea73fa157a2ee`;
  - `state_budgets.qld_budget`: HTTP 200, 3 rows, year precision, first artifact
    `9cf28ec0851ce38e3e3afb216a41132c2225c13af187679fac015d8e93dab699`;
  - `state_budgets.vic_budget`: HTTP 200, 3 rows, year precision, first artifact
    `e2f3245a52d6ff11cf6d4f5db31a6637af6e4858e7dec626498d837fd22afbbc`;
  - `treasury.budget_papers`: HTTP 200, 2 rows, year precision, first artifact
    `ca5e9b60c3c215eadcf64861ea16a8217261a0c858297acffce834698bde52ed`.
    Broad ad hoc verification queries that joined or probed `observations` without
    concrete series bounds still killed Postgres backends at `2026-06-20T02:40:49Z`
    and `2026-06-20T02:43:28Z`. After switching back to bounded API probes, no new
    kill, recovery, or fatal Postgres log entries appeared after
    `2026-06-20T02:44:00Z`.
- The exact filtered ABS request that previously destabilized the local
  database now returns HTTP 200 with 4 rows:
  `GET /v1/observations?dataflow=abs.cpi&dimensions[]=measure=1&dimensions[]=index=10001&dimensions[]=tsest=10&dimensions[]=region=50&dimensions[]=freq=Q&limit=4&since=2024-03-01`.
- CSV observation export works for live ABS data and preserves attribution in
  the header comment.
- Observation JSON cache validation now fingerprints matching `series` metadata
  instead of aggregating every matching observation row. This keeps the ETag path
  off the largest hot table for live CPI queries while still changing when loader
  metadata updates the affected series.
- The Next.js web app passes:
  - `pnpm run lint`;
  - `pnpm --filter @au-kpis/web typecheck`;
  - `NEXT_PUBLIC_AU_KPIS_API_BASE_URL=http://127.0.0.1:3001 pnpm --filter @au-kpis/web build`;
  - `NEXT_PUBLIC_AU_KPIS_API_BASE_URL=http://127.0.0.1:3001 pnpm --filter @au-kpis/web test:e2e`.
- The web E2E run covers Explorer, Compare, Playground, and axe checks.
- The web app now includes a Search surface in the Next.js client shell. The
  Search flow queries the SDK, shows ranked catalog results, and opens a
  selected dataflow in Explorer. The E2E suite covers
  `Search -> CPI -> Consumer Price Index -> Open in Explore`.
- A fresh rerun against a dedicated API/web pair also passes:
  - API on `127.0.0.1:3013` with CORS allowing `http://127.0.0.1:3014`
    and local anonymous rate limits raised to avoid browser-test bursts
    exhausting the default 200-request hour bucket;
  - web on `127.0.0.1:3014`;
  - `AU_KPIS_WEB_PORT=3014 NEXT_PUBLIC_AU_KPIS_API_BASE_URL=http://127.0.0.1:3013 pnpm --filter @au-kpis/web test:e2e`.
- Latest focused frontend verification after adding Search:
  - `pnpm --filter @au-kpis/web typecheck`;
  - `pnpm --filter @au-kpis/sdk test`;
  - `AU_KPIS_WEB_PORT=3014 NEXT_PUBLIC_AU_KPIS_API_BASE_URL=http://127.0.0.1:3013 pnpm --filter @au-kpis/web test:e2e` with 6 passing Chromium tests.
- Latest current-stack frontend verification:
  - `pnpm dlx shadcn@latest info --json` identifies `apps/web` as Next.js
    `16.2.9`, App Router, Tailwind v4, RSC enabled, lucide icons, and installed
    components `button`, `card`, `native-select`, and `table`;
  - `pnpm --filter @au-kpis/sdk test`;
  - `pnpm --filter @au-kpis/web typecheck`;
  - `AU_KPIS_API_BASE_URL=http://127.0.0.1:3013 pnpm --filter @au-kpis/web build`;
  - `AU_KPIS_WEB_PORT=3014 AU_KPIS_API_BASE_URL=http://127.0.0.1:3013 pnpm --filter @au-kpis/web exec playwright test --workers=1`
    with 6 passing Chromium tests.
  - The same SDK, web typecheck, shadcn info, production build, and serial
    Playwright checks passed again on 2026-06-20 after MinIO/testcontainer
    cleanup and the bounded per-source API rerun.
  - A two-worker Playwright run hit local startup pressure in the first Explorer
    and Compare tests, where the dataflow select stayed empty for the default
    5-second assertion window. The same suite passed serially against the same
    API and web code.
- Latest broad workspace verification:
  - `cargo check --workspace` passed.
  - `cargo fmt --all --check` passed.
  - `cargo clippy --workspace --all-targets -- -D warnings` passed after
    replacing two off-by-one-style `observations.len() >= query.limit + 1`
    checks with `observations.len() > query.limit`.
  - A parallel `cargo nextest run --workspace` exposed two real test drift
    issues and local Docker memory pressure. The real drift was fixed by
    refreshing the OpenAPI snapshot for `TimePrecision::Minute` and making
    webhook delivery tests read the persisted `next_attempt_at` instead of
    comparing against a fixed May 2026 timestamp.
  - `cargo nextest run --workspace -j 1` printed
    `499 tests run: 499 passed, 1 skipped` in 356.399 seconds. The wrapper
    command then exited with a shell-script error from assigning to zsh's
    read-only `status` variable after the passing test summary, so the nextest
    log is the verification evidence.
  - `pnpm run lint` passed.
  - `pnpm turbo run typecheck test` passed all 10 TypeScript tasks.
- Browser validation against API `127.0.0.1:3013` and web
  `127.0.0.1:3014` verified:
  - Explorer renders real CPI data (`101.7` for Jan 2026), state comparison, and
    25 table rows with no console warnings/errors;
  - Search starts with `CPI`, returns Consumer Price Index, and opens it in
    Explorer with the latest CPI card and table still loaded;
  - Compare shows 3 active CPI lines and a latest-values table for Australia,
    Sydney, and Melbourne;
  - Playground runs a live query and returns JSON containing `observations` and
    `source_artifact_id`, plus curl and SDK snippets;
  - a 390px-wide viewport loads without app errors or loading deadlock.
  - Desktop and mobile screenshots were captured for Explorer, Search, Compare,
    and Playground during the UI/UX review.
- The broader ingestion-core integration/failure-path suite now passes after
  setting the Colima Docker socket:
  - `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-ingestion-core --tests -- --test-threads=1`.
  - The earlier failure without `DOCKER_HOST` was
    `Connection refused` while testcontainers attempted to create a TimescaleDB
    container, not an assertion failure.
  - The 2026-06-20 serial rerun left no `org.testcontainers.managed-by=testcontainers`
    containers behind.
- Artifact-load completion and rerun-skip verification now passes:
  - `cargo fmt --all --check`;
  - `cargo check -p au-kpis-db -p au-kpis-ingestion-core -p au-kpis-ingestion -p au-kpis-loader`;
  - `cargo clippy -p au-kpis-db -p au-kpis-ingestion-core -p au-kpis-ingestion -p au-kpis-loader --all-targets -- -D warnings`;
  - `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock cargo test -p au-kpis-db --test migrations -- --nocapture`;
  - `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-ingestion-core --test pipeline_failures -- --nocapture`;
  - `cargo test -p au-kpis-loader --test load_batch -- --nocapture`.
  - The first default-parallel ingestion-core run exposed pool-acquire timeouts
    after the parse stage started probing the new completion table. The fix keeps
    the probe best-effort only for pool-acquire unavailability while preserving
    hard failures for schema/query errors; the full suite now passes in both
    serial and default parallel mode.
- The source-adapter test set passes on the current worktree:
  - `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-adapter-abs -p au-kpis-adapter-apra -p au-kpis-adapter-asx -p au-kpis-adapter-aemo -p au-kpis-adapter-rba -p au-kpis-adapter-treasury -p au-kpis-adapter-state-budgets --tests`.
  - This rechecks source-specific discovery, fetch, parse, provenance rejection,
    schema-hash drift, and current PDF/table-shape fixture coverage.
- Latest observation API verification after the bounded latest-read fix:
  - `cargo fmt --all --check`;
  - `cargo test -p au-kpis-api-http --lib observations::tests -- --nocapture`;
  - `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock cargo test -p au-kpis-api-http --test observations paginated_observations_concatenate_to_the_full_result -- --nocapture --exact`;
  - `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock cargo test -p au-kpis-api-http --test observations -- --nocapture --test-threads=1`;
  - `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-db --test migrations -- --nocapture`.
  - A parallel run of the observations integration file started several
    Timescale test containers at once and one test hit `StartupTimeout`; the
    serial rerun above completed all 5 observations integration tests.
  - The 2026-06-20 rerun of `cargo fmt --all --check`,
    `cargo test -p au-kpis-api-http --lib observations::tests -- --nocapture`,
    and
    `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock cargo test -p au-kpis-api-http --test observations -- --nocapture --test-threads=1`
    passed.

## Ingestion Findings

### ABS

Status: end-to-end clean for the live CPI dataflow.

The first live ABS run failed before parsing observations because the adapter
requested generic `application/vnd.sdmx.data+json`. ABS currently negotiates
SDMX-JSON `version=2`, whose data wrapper uses `structures` instead of the
parser-supported `structure` key.

Fix applied:

- `au-kpis-adapter-abs` now requests
  `application/vnd.sdmx.data+json;version=1.0.0-wd`.
- Regression coverage added in `crates/adapters/abs/tests/fetch.rs`.
- The demo ABS CPI seed now matches the live SDMX dimensions and measures:
  `measure`, `index`, `tsest`, `region`, `freq`, with numeric ABS codes.
- The staged loader commit path now promotes large accepted artifacts in bounded
  chunks using an indexed temp staging row id. This avoids the Postgres backend
  kill seen when promoting the full live CPI artifact in one unindexed
  statement/transaction.
- Regression coverage added in `crates/au-kpis-loader/tests/load_batch.rs`.
- `cargo test -p au-kpis-adapter-abs --tests` passes.
- `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock cargo test -p au-kpis-loader --test load_batch`
  passes.
- `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock cargo test -p au-kpis-api-http --test observations`
  passes for observation JSON/CSV/Parquet responses, pagination, ETag headers,
  and monthly rollups. The rollup test removes the background monthly refresh
  policy in its disposable database before manually refreshing the fixture
  window, avoiding a Timescale refresh race.
- The API and web UI query the live national All groups CPI slice using
  `measure=1`, `index=10001`, `tsest=10`, `region=50`, `freq=Q`.

### APRA

Status: live observations are loaded and queryable through the API. The local
database still cannot complete the largest APRA one-shot cleanup reliably under
the current Docker memory limit.

Fix applied:

- `SourceAdapter` now exposes optional source metadata and dataflow metadata.
- The ingestion binary syncs adapter-owned source/dataflow/measure/dimension
  catalog rows after migrations and before `--once` or worker mode runs.
- APRA, RBA, Treasury, and state-budget static dataflows now publish their
  catalog metadata through the adapter trait.
- APRA discovery now accepts current release links under `/system/files/...` as
  well as the older `/sites/default/files/...` path.
- APRA parsing now treats inline unit-marker cells such as
  `(thousands of loans)` as metadata/missing values instead of fatal numeric
  drift.
- The shared adapter HTTP client retries cloneable transient send failures. This
  covers APRA TLS EOF/close-notify interruptions observed during live workbook
  fetches.
- `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock cargo test -p au-kpis-ingestion --test cli run_mode_syncs_adapter_catalog_before_worker_loop`
  passes and verifies APRA plus the other static dataflows are present before
  the worker loop accepts jobs.
- `cargo test -p au-kpis-adapter-apra --tests` passes.
- `cargo test -p au-kpis-adapter --tests` passes.
- Live API proof:
  - `GET /v1/observations?dataflow=apra.quarterly_statistics&limit=3` returns
    APRA rows with `source_artifact_id`, APRA workbook `source_url`, dimensions,
    attributes, and cursor metadata.
  - `GET /v1/series/apra.quarterly_statistics/{series_key}` returns the latest
    APRA observation for a loaded series.

Remaining issue:

- The APRA one-shot live run still hit the local resource ceiling during final
  temp-table cleanup: the Postgres container was killed by signal 9 under a
  Docker memory limit of roughly 1.9 GiB. Data committed before the cleanup kill
  remained queryable after restart. This is a local capacity/scalability issue to
  address with smaller staging/promote units, increased local Docker resources,
  or Timescale job tuning.

### RBA

Status: blocked by upstream/network access from the current environment.

Evidence:

- `https://www.rba.gov.au/statistics/tables/` returns HTTP 403 to curl.
- Direct CSV/XLS URLs under `https://www.rba.gov.au/statistics/tables/` also
  return HTTP 403.
- Browser-like request headers and Python `urllib` receive the same 403.

This is not currently proven to be an adapter header bug.

### Treasury And State Budgets

Status: Treasury and NSW/VIC/QLD state budgets are now live-loaded and
queryable through the API. The local state-budget dataflow checks use filtered
or exact-series probes where needed because broad ad hoc joins can still exceed
the local Docker VM memory limit.

Treasury fetched and stored a PDF artifact after source metadata was seeded, but
the local Python sidecar extraction exceeded the Rust PDF client's 30 second
request timeout. The sidecar continued CPU-bound after the client timed out.

Fixes applied:

- The PDF sidecar request contract now accepts optional `pages`, a non-empty
  list of 1-indexed PDF pages.
- The deterministic sidecar passes request-level page windows into both Camelot
  and pdfplumber. If `pages` is absent, the existing
  `AU_KPIS_PDF_EXTRACTOR__MAX_PAGES` smoke-test cap still applies.
- Treasury extraction requests are bounded to pages 1-85, matching the current
  2026-27 Budget Paper No. 4 agency-resourcing PDF inspected locally.
- State-budget extraction requests use source-specific bounded windows:
  - NSW pages 1-80;
  - VIC pages 1-80;
  - QLD pages 1-125. Local inspection found QLD Table 8.1 on page 113 of the
    official 2025-26 Budget Paper No. 2 PDF.
- `AU_KPIS_PDF_REQUEST_TIMEOUT_SECS` now lets the ingestion binary raise the
  Rust PDF client's per-attempt sidecar timeout when `AU_KPIS_PDF_BASE_URL` is
  set. This preserves the 30 second default while allowing live Treasury runs to
  use a longer bounded extraction window.
- The sidecar and adapter contract docs were updated in `Spec.md` and
  `docs/pdf-extractor.md`.

Focused verification:

- `cargo fmt --all --check`
- `pnpm exec markdownlint-cli2 Spec.md docs/pdf-extractor.md docs/full-stack-verification-2026-06-19.md docs/ui-ux-approachability-prd.md`
- `cd apps/pdf-extractor && uv run pytest -p no:capture tests/test_app.py`
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-pdf-client extraction_request_requires_source_id_and_omits_optional_fields_until_set --lib`
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-adapter-treasury --test parse parses_treasury_budget_pdf_fixtures_through_sidecar_contract -- --nocapture`
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-adapter-state-budgets --test parse -- --nocapture`
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo check -p au-kpis-ingestion`

Live Treasury rerun after the bounded request change:

- Started the local sidecar with `uv run uvicorn` on `127.0.0.1:8010`, pointed
  at MinIO.
- Ran one Treasury ingestion pass with `AU_KPIS_PDF_BASE_URL=http://127.0.0.1:8010`
  and `AU_KPIS_PDF_REQUEST_TIMEOUT_SECS=300`.
- The run fetched and stored the official Budget Paper No. 4 PDF:
  - artifact id
    `ca5e9b60c3c215eadcf64861ea16a8217261a0c858297acffce834698bde52ed`;
  - storage key
    `artifacts/ca5e9b60c3c215eadcf64861ea16a8217261a0c858297acffce834698bde52ed`;
  - source URL
    `https://budget.gov.au/content/bp4/download/bp4_05_agency_resourcing_tables.pdf`;
  - size `2,070,210` bytes.
- The sidecar request completed with HTTP 200, so the previous 30 second timeout
  was removed from this path.
- The first rerun after the bounded request change still failed with
  `format drift: Treasury PDF sidecar returned no recognised budget tables`.
- Direct local deterministic extraction over pages 1-85 returned 85 Camelot
  stream tables. The first table is page 1 with 30 rows and 12 columns, headed
  by `PARLIAMENT` and `Agency Resourcing - 2026-2027`, proving the remaining
  gap was adapter table recognition/mapping for the real BP4 shape.
- The Treasury parser now supports the real Camelot stream BP4 shape:
  - current/prior fiscal periods are read from the title rows;
  - `Total` column values are parsed as the source-supported value measure;
  - department/outcome context is preserved in `line_item`;
  - `$'000` is emitted as `$ thousand`;
  - row-label `Total` cells are no longer mistaken for the total-value column.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-adapter-treasury --test parse -- --nocapture`
  now covers the existing simple fixture shape and the real BP4 Camelot stream
  shape.

Live Treasury proof after the parser fix:

- `cargo run --bin au-kpis-ingestion -- --once --source treasury --dataflow budget-papers`
  exited successfully against local Postgres, Redis, MinIO, and the local
  `uvicorn` PDF sidecar.
- Treasury series count after the run: 156 series for
  `treasury.budget_papers`.
- `GET /v1/observations?dataflow=treasury.budget_papers&limit=5` through the
  local API returned 5 observations. The first returned row included:
  - `source_artifact_id`
    `ca5e9b60c3c215eadcf64861ea16a8217261a0c858297acffce834698bde52ed`;
  - `unit` `$ thousand`;
  - `time_precision` `year`;
  - `status` `estimated`;
  - `treasury_line_item` `National Competition Council / Outcome 1`;
  - `treasury_period_label` `2025-26`;
  - source URL
    `https://budget.gov.au/content/bp4/download/bp4_05_agency_resourcing_tables.pdf`.
- Existing Treasury `parse_errors` rows remain from the two failed attempts
  before the parser fix, but no new Treasury parse error was recorded by the
  successful rerun.

Live state-budget proof after the parser fixes:

- `cargo run --bin au-kpis-ingestion -- --once --source state-budgets --dataflow nsw-budget`
  exits successfully against local Postgres, Redis, MinIO, and the local
  `uvicorn` PDF sidecar.
- NSW now uses the current official PDF URL
  `https://www.nsw.gov.au/sites/default/files/noindex/2026-03/bp1-budget-statement-nsw-budget-2025-26.pdf`
  and parses `Table 1.2: Key budget aggregates for the general government
sector`.
- The current NSW PDF path contains `/2026-03/`, but the adapter now preserves
  the correct budget-year dimension `2025-26`. The bad local `2026-03` slice
  produced by the pre-fix run was removed from the verification database.
- NSW database proof:
  - 13 series for `state_budgets.nsw_budget`;
  - only budget-year dimension `2025-26`;
  - first API row from
    `GET /v1/observations?dataflow=state_budgets.nsw_budget&limit=3`
    includes table page `17`, schema hash
    `8682d8d86a4591880d0f82cf85acc0eabb8608b2e04fd4dc0934eb8317f7c385`,
    and source artifact
    `e9155d5c9de596675b24ae7477c497ff22fd74cc55df3c3115eea73fa157a2ee`.
- `cargo run --bin au-kpis-ingestion -- --once --source state-budgets --dataflow vic-budget`
  exits successfully after narrowing VIC extraction to page 14 and accepting
  the current `COMPREHENSIVE OPERATING STATEMENT` table shape.
- VIC database/API proof:
  - 31 series for `state_budgets.vic_budget`;
  - observations span `2026-07-01` to `2029-07-01`;
  - exact-series probe for grants returns `56,366`, `55,679`, `57,030`,
    `58,946` with forecast status;
  - API rows include table page `14`, schema hash
    `a3a19b2782eee5343c3bc530b81ce9468e814bd22e5d007155b4df02d92bc706`,
    and source artifact
    `e2f3245a52d6ff11cf6d4f5db31a6637af6e4858e7dec626498d837fd22afbbc`.
- `cargo run --bin au-kpis-ingestion -- --once --source state-budgets --dataflow qld-budget`
  exits successfully after narrowing QLD extraction to page 113 and accepting
  the current split `Table 8.1` title shape.
- QLD database/API proof:
  - 28 series for `state_budgets.qld_budget`;
  - observations span `2023-07-01` to `2028-07-01`;
  - exact-series probe for taxation revenue returns one row per fiscal year and
    keeps `2024-25 Est.Actual` as `25,015` with estimated status, instead of
    emitting both the `2024-25 Budget` and `2024-25 Est.Actual` columns for the
    same timestamp;
  - API rows include table page `113`, schema hash
    `3dc097502c47f72793ba9bff6b62b77d022d00a69ac6e7e7d81349bb11c8e7ef`,
    and source artifact
    `9cf28ec0851ce38e3e3afb216a41132c2225c13af187679fac015d8e93dab699`.
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-C debuginfo=0' cargo test -p au-kpis-adapter-state-budgets --tests -- --nocapture`
  passes with current-shape coverage for NSW, VIC, and QLD.

Local Python note:

- Plain `uv run pytest tests/test_app.py` crashes in this environment while
  pytest imports Python's `readline` module during capture setup. Re-running
  with pytest capture disabled avoids that local native-code crash; the same
  sidecar API test file passes with `-p no:capture`.

Remaining issues:

- Ad hoc broad SQL counts over `observations` still crash this low-memory local
  Postgres VM. Use the API or bounded/indexed series-key reads for verification.
  The Treasury API query succeeded but logged a 1.65 second slow-query warning,
  so this path needs a later performance pass.
- The Docker PDF image cache was damaged during an earlier disk-full event; the
  sidecar was verified locally with `uv run`.

### ASX

Status: implemented for the public ASX historical market-statistics page and
live-loaded locally. This is a public market-statistics path, not a licensed
ASX EOD/ComNews feed.

Fix applied:

- `crates/adapters/asx` now implements `SourceAdapter` for
  `asx.market_statistics`.
- Discovery emits the public historical market-statistics page as a monthly
  HTML artifact.
- Fetch persists the raw HTML artifact with response headers and
  content-addressed storage provenance.
- Parse extracts monthly All Ords, S&P/ASX 200, market capitalisation, listed
  company count, and all-listed-entity count observations.
- Parser regression coverage includes older ASX archive month labels:
  `Dec&#39;20`, `Nov18`, and `Nov-17`.
- Dataflow metadata keeps the official ASX source URL even when a local page URL
  override is used for testing.
- `cargo test -p au-kpis-adapter-asx --tests` passes.

Live proof:

- `cargo run -p au-kpis-ingestion -- --once --source asx --dataflow market-statistics`
  exited successfully against local Postgres and MinIO.
- Database counts after the run:
  - 5 series;
  - 625 observations;
  - first observation `2016-01-01`;
  - last observation `2026-05-01`.
- API proof:
  - `GET /v1/dataflows/asx.market_statistics` returns the ASX dataflow and
    `metric` dimension metadata.
  - `GET /v1/observations?dataflow=asx.market_statistics&limit=3` returns
    source-attributed ASX observations.
  - `GET /v1/search?q=ASX&limit=5` returns `asx.market_statistics`.

Note:

- A stale `parse_errors` row remains from the pre-fix live attempt that exposed
  the `Dec&#39;20` parser gap. A subsequent live ASX run succeeded after the
  regression fix.

### AEMO

Status: implemented for NEMWeb DispatchIS ZIP artifacts and live-loaded locally
for one official five-minute interval.

Fix applied:

- `crates/adapters/aemo` now implements `SourceAdapter` for `aemo.dispatch`.
- Discovery parses NEMWeb directory listings, including uppercase `HREF`
  directory markup from the live IIS-style listing.
- Fetch persists raw DispatchIS ZIP artifacts with response headers and
  content-addressed storage provenance.
- Parse extracts regional reference price (`RRP`) and regional total demand
  (`TOTALDEMAND`) from the CSV inside each ZIP.
- Dispatch observations now use `minute` time precision. The domain enum,
  loader mapping, API query parsing/CSV labels, and schema migration support the
  value end to end.
- Parser and discovery regression coverage rejects ambiguous source/storage
  provenance and verifies uppercase NEMWeb links.
- Dataflow metadata keeps the official NEMWeb source URL even when a local
  listing URL override is used for testing.
- `cargo test -p au-kpis-adapter-aemo --tests` passes.

Live proof:

- The full live NEMWeb directory contained 500+ current DispatchIS ZIPs, so the
  smoke run used a one-file local listing pointing at an official NEMWeb ZIP to
  avoid spending minutes fetching the whole rolling window.
- `cargo run -p au-kpis-ingestion -- --once --source aemo --dataflow dispatch`
  exited successfully against local Postgres and MinIO with
  `AU_KPIS_AEMO_DISPATCH_LISTING_URL` pointed at that one-file listing.
- After adding the local MinIO memory cap, the same command was rerun against
  `http://127.0.0.1:8765/au-kpis-aemo-listing.html` and exited successfully.
  The object store contained `artifacts/9f746b...5dae8` at 18 KiB.
- Database counts after the run:
  - 1 artifact;
  - 10 series;
  - 10 observations;
  - observation time `2026-06-19 17:05:00+00`.
- Narrow indexed database proof for series
  `29eb7029e161e45e086bf376f08e01de679eeab3075ce727ac34bc507c8d37e1`
  returns one row at `2026-06-19 17:05:00+00` with `{minute}` precision.
- API proof:
  - `GET /v1/dataflows/aemo.dispatch` returns the AEMO dataflow with `region`
    and `metric` dimensions.
  - `GET /v1/observations?dataflow=aemo.dispatch&limit=3` returns live NEMWeb
    source-attributed price and demand rows.
  - `GET /v1/observations?dataflow=aemo.dispatch&limit=1` returns
    `"time_precision": "minute"` for the verified dispatch timestamp after
    applying `0008_add_minute_time_precision`.
  - `GET /v1/observations?dataflow=aemo.dispatch&limit=10` returns 10 rows, all
    with `"time_precision": "minute"`.
  - `GET /v1/search?q=AEMO&limit=5` returns `aemo.dispatch`.

Remaining issue:

- AEMO is still only smoke-tested against one official five-minute interval. It
  now preserves the exact dispatch timestamp and labels dispatch observations
  with `minute` time precision end to end.
- Broad ad hoc joins from `series` to the large `observations` hypertable can
  still exceed the local Docker VM memory limit. Use API paths or indexed
  `series_key` probes for local verification until the query plan is hardened.

## 2026-06-20 Verification Addendum

### RBA Current-Shape Smoke

Status: representative official RBA statistical-table ingestion now passes
locally.

Fix applied:

- RBA CSV parsing now handles current upstream table shape with a title row
  before the `Title,...` header, no explicit `Date` header, blank trailing
  series columns, and UTF-8 or Windows-1252 encoded CSV content.
- Non-numeric operation detail columns are preserved as context attributes
  instead of being treated as numeric observation series.
- Scalar range cells such as `-0.50 to -1.00` are treated as missing numeric
  values, while mixed numeric/non-numeric values in an otherwise numeric series
  still fail as format drift.
- Parser regression tests cover current A1, A2, A3, and long-dated open-market
  operation table shapes.

Live proof:

- A local index served four official RBA CSV URLs for A1, A2, A3 daily open
  market operations, and A3 long-dated open-market operations.
- `cargo run -p au-kpis-ingestion -- --once --source rba --dataflow statistical-tables`
  exited successfully with serial fetch/parse settings and one-row load chunks.
- Database counts after the smoke:
  - 12 RBA artifacts;
  - 45 RBA statistical-table series;
  - 0 new RBA parse errors after the parser fixes.

Remaining issue:

- Running the full current RBA catalog still exceeds the small local Docker
  Postgres memory budget during large staged observation promotion. The parser
  drift exposed by live RBA was fixed; the remaining failure is local bulk-load
  capacity, not a parser error.

### AEMO Current Smoke

Status: AEMO dispatch ingestion was rerun against a current official NEMWeb ZIP
selected from the live listing.

Live proof:

- The latest official DispatchIS artifact selected for smoke verification was
  `PUBLIC_DISPATCHIS_202606201350_0000000523397811.zip`.
- A local one-file listing pointed ingestion at that official ZIP.
- `cargo run -p au-kpis-ingestion -- --once --source aemo --dataflow dispatch`
  exited successfully.
- Database counts after the rerun:
  - 2 AEMO artifacts;
  - 10 AEMO dispatch series;
  - 0 new AEMO parse errors after the smoke.

### Artifact Load Completion Markers

Status: completed artifact/dataflow loads are now persisted and skipped on
rerun, reducing conflict-heavy reprocessing for already-ingested artifacts.

Live proof:

- Migration `0011_artifact_loads` was applied to the local compose database; all
  11 sqlx migrations are present and `artifact_loads` resolves in `public`.
- A live AEMO one-shot run against the current listing reached an upstream
  `403 Forbidden` after successfully loading and marking 117 artifacts. A rerun
  emitted `skipping previously completed artifact load` for those artifacts
  instead of parsing/loading them again.
- The rerun was stopped after proving the skip path and after six additional
  clean artifacts were marked. The local marker table then held 123 AEMO
  `aemo.dispatch` completions, with 1,230 parsed and 1,230 loaded observations.

Remaining issue:

- This does not backfill markers for historical artifacts loaded before
  migration 0011. Existing dirty local databases may still reprocess old
  unmarked artifacts until they complete one clean post-migration run or a
  deliberate, indexed backfill is designed.

### Exact-Series API And Provenance

Status: exact-series HTTP reads pass for all nine locally loaded dataflows and
the returned provenance resolves through database and object storage.

Verified dataflows:

- `abs.cpi`
- `aemo.dispatch`
- `apra.quarterly_statistics`
- `asx.market_statistics`
- `rba.statistical_tables`
- `state_budgets.nsw_budget`
- `state_budgets.qld_budget`
- `state_budgets.vic_budget`
- `treasury.budget_papers`

Proof:

- `GET /v1/health` returned `{"status":"ok"}` from `127.0.0.1:3200`.
- `GET /v1/dataflows` returned all loaded dataflows.
- Exact-series `GET /v1/observations` requests returned HTTP 200 for all nine
  dataflows with populated `source_artifact_id`, unit, and time precision.
- Broad raw `GET /v1/observations` requests now fail fast with HTTP 400 when
  they match more than 16 series, instead of admitting query plans that can
  exhaust the small local Timescale/Postgres VM.
- Live broad-read matrix after the guard:
  - `abs.cpi`: HTTP 400, matched 8,467 series;
  - `apra.quarterly_statistics`: HTTP 400, matched 6,943 series;
  - `aemo.dispatch`: HTTP 200, 10 rows;
  - `asx.market_statistics`: HTTP 200, 3 rows;
  - `rba.statistical_tables`: HTTP 400, matched 45 series;
  - `state_budgets.nsw_budget`: HTTP 200, 3 rows;
  - `state_budgets.qld_budget`: HTTP 400, matched 28 series;
  - `state_budgets.vic_budget`: HTTP 400, matched 31 series;
  - `treasury.budget_papers`: HTTP 400, matched 156 series.
- Exact-series live matrix after the guard returned HTTP 200 for all nine loaded
  dataflows:
  - `abs.cpi`: 2 rows, first `2025-10-01T00:00:00Z`;
  - `aemo.dispatch`: 2 rows, first `2026-06-19T17:05:00Z`;
  - `apra.quarterly_statistics`: 1 row, first `2025-10-01T00:00:00Z`;
  - `asx.market_statistics`: 3 rows, first `2016-01-01T00:00:00Z`;
  - `rba.statistical_tables`: 3 rows, first `2013-11-11T00:00:00Z`;
  - `state_budgets.nsw_budget`: 3 rows, first `2023-07-01T00:00:00Z`;
  - `state_budgets.qld_budget`: 3 rows, first `2023-07-01T00:00:00Z`;
  - `state_budgets.vic_budget`: 3 rows, first `2026-07-01T00:00:00Z`;
  - `treasury.budget_papers`: 2 rows, first `2025-07-01T00:00:00Z`.
- A 2026-06-20 rerun on `127.0.0.1:3000` verified the `/v1/series/{dataflow}/{series_key}`
  lookup path after replacing the hypertable lateral join:
  - all nine representative series lookups returned HTTP 200 in one parallel
    batch;
  - total wall time for the batch was 145 ms;
  - eight of the nine representative samples returned latest-observation
    provenance directly; the sampled RBA series returned metadata only, while
    the exact-dimension RBA `/v1/observations` probe still returned rows with
    `source_artifact_id`.
- Every sampled `source_artifact_id` existed in the `artifacts` table with a
  canonical `artifacts/<sha256>` storage key.
- S3 `head-object` against local MinIO confirmed all sampled artifact objects
  exist and byte sizes match the database.

Implementation note:

- `/v1/observations` no longer uses a multi-series `IN (...) ORDER BY time` plan
  for raw latest reads. It validates candidate-series count first, reads bounded
  single-series windows with observed time bounds, and merges the small result
  set in Rust.
- `/v1/series/{dataflow}/{series_key}` no longer joins `series` directly to the
  observations hypertable or expands `observations_latest`. It reads series
  metadata first, then performs a point lookup at `(series_key, last_observed)`
  and chooses the highest revision for that timestamp. This removed the
  `out of shared memory` failures reproduced by parallel series lookups on the
  local TimescaleDB container.
- Postgres stayed healthy with no new recovery logs after the guarded live API
  matrix.

Remaining local caveat:

- Broad ad hoc SQL joins from `series` to `observations` can still exceed the
  small local Docker VM's Timescale lock budget. Use API endpoints or exact
  point-lookups for verification; do not validate provenance with unbounded
  exploratory joins on the local compose database.

### Frontend Build And Browser Review

Status: the upgraded reference client builds and loads live API data through the
Next.js app proxy.

Verified:

- `pnpm --filter @au-kpis/web typecheck`
- `pnpm run lint`
- `pnpm --filter @au-kpis/web build`
- `cargo fmt --all --check`
- `cargo check -p au-kpis-api-http -p au-kpis-api -p au-kpis-ingestion -p au-kpis-adapter-rba`
- `cargo test -p au-kpis-api-http observations::tests:: -- --nocapture`
- `cargo test -p au-kpis-api-http --test observations -- --nocapture`
- Browser load at `http://127.0.0.1:3210/`
- Search navigation with `CPI` result rendering
- Desktop screenshot after CPI data loaded
- 390px mobile screenshot after CPI data loaded
- `/favicon.ico` returns `200` after adding the favicon route

Browser findings that feed the UI/UX PRD:

- The current app is credible and data-focused, but first use still leads with
  internal vocabulary and controls instead of the latest answer.
- Search and Explore work with live CPI data.
- Desktop and mobile first viewports show no obvious clipping or overlap.
- Mobile places controls before the answer, which is the main product usability
  issue to fix next.
- Route-backed navigation, CSV export, snippet copy, stronger error states, and
  a docs gateway remain the key gaps against the spec-critical CPI journey.

## Next Implementation Slice

1. Extend catalog sync with source-owned code values where upstream metadata or
   parsed series can safely populate codelists.
2. Harden local Timescale query plans and cleanup paths that still exceed the
   small Docker VM memory budget during broad ad hoc observation operations.
3. Expand AEMO beyond smoke ingestion with broader fixture coverage across
   multiple dispatch intervals and regions.
4. Keep broad local Rust verification serial (`cargo nextest run --workspace -j 1`)
   unless the Docker VM memory budget is increased; the default parallel run can
   OOM local MinIO and delay testcontainer startup.
5. Decide whether Timescale continuous aggregate refresh policies should be
   disabled or narrowed in local smoke profiles; current startup refreshes can
   exceed the small local Docker memory budget after large live ingestion runs.
