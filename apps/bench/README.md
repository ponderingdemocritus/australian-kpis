# k6 Bench Scaffolds

`smoke.js` is the Phase 1 placeholder for the PR smoke scenario described in
`Spec.md § Benchmarking`. It targets a locally running API and keeps thresholds
close to the production budget while the endpoint set is still small.

```bash
AU_KPIS_BASE_URL=http://127.0.0.1:3000 k6 run apps/bench/smoke.js
```

Later issues add sustained and burst scenarios here without changing the local
entrypoint convention.
