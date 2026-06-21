# PDF extractor service

The PDF extractor is a stateless FastAPI sidecar for turning stored PDFs into raw table candidates. It does not emit final economic observations; Rust adapters still own source-specific validation and mapping.

## Runtime

- `GET /health` returns `{"status":"ok"}` for liveness and readiness probes.
- `POST /extract` accepts `s3_key`, `source_id`, optional `artifact_date`, optional `strategy`, and optional `pages`.
- `pages` is a non-empty list of 1-indexed pages. Source-specific adapters use it to keep deterministic extraction bounded to the expected table window.
- The sidecar fetches the PDF directly from the configured S3/R2-compatible bucket and writes it to an ephemeral local temp file for extraction.
- The deterministic backend uses `pdfplumber` and `camelot-py[cv]`; model fallback requests currently return `501` until a pinned local model backend is configured.
- Responses match the `au-kpis-pdf-client` contract: `artifact_key`, `backend`, and a list of table candidates with page, bounding box, raw cells, spans, and diagnostics.
- The compose service runs as `linux/amd64` because Camelot's CV extra depends on a
  `pdftopng` wheel that is not published for Linux ARM.

## Configuration

The service uses the same object-store environment names as the ingestion stack:

- `AU_KPIS_OBJECT_STORE__ENDPOINT`
- `AU_KPIS_OBJECT_STORE__BUCKET`
- `AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID`
- `AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY`
- `AU_KPIS_OBJECT_STORE__REGION`
- `AU_KPIS_PDF_EXTRACTOR__MAX_PAGES` optionally limits deterministic extraction to
  the first N pages for smoke tests when a request does not include `pages`.
- `AU_KPIS_PDF_REQUEST_TIMEOUT_SECS` optionally raises the Rust ingestion
  client's per-attempt sidecar timeout when `AU_KPIS_PDF_BASE_URL` is set.

Local compose points these at MinIO. Staging and production should point them at the private R2/S3 endpoint used for raw artifacts.

## Horizontal scaling

The sidecar is CPU-bound and stateless. Scale it horizontally behind an internal load balancer rather than sharing local state.

- Run at least two replicas in staging/production.
- Keep the service internal-only; adapters call it through `au-kpis-pdf-client`.
- Give each replica isolated `/tmp` storage sized for the largest expected PDF plus extraction scratch space.
- Start with one worker process per vCPU. Increase replicas before increasing per-process concurrency because Camelot/OpenCV work is CPU-heavy.
- Use `/health` as the readiness probe and remove a replica from rotation before terminating it.
- Store all raw artifacts in S3/R2. Do not mount shared volumes for PDF input.

## Smoke test

`apps/bench/pdf-extractor-smoke.js` verifies `/health` and one deterministic extraction against a preloaded S3 key. In CI the key is seeded into MinIO from the committed Budget Paper No. 4 agency resourcing table fixture.
