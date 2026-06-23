# Merge Queue And Main Branch Protection

`Spec.md § CI/CD pipeline -> Merge-queue flow` is the source of truth. This
note records the repository settings that make `.github/workflows/merge.yml`
blocking for `main`.

## Required GitHub Settings

Configure `main` with a branch protection rule or repository ruleset that has:

- [ ] Branch target is exactly `main`; do not use a wildcard pattern for merge queue.
- [ ] Pull requests are required before merging.
- [ ] CODEOWNERS review is required before a pull request can merge.
- [ ] Signed commits are required.
- [ ] Status checks are required before merging.
- [ ] Required check `CI OK` is selected for pull request heads.
- [ ] Required check `Merge Queue OK` is selected for merge groups.
- [ ] Merge queue is required.
- [ ] Merge queue uses squash merging.
- [ ] Build concurrency is `1` until the suite is consistently under the
      `Spec.md` wall-time budget.
- [ ] Minimum and maximum merge group size are both `1` until the staging smoke,
      contract, and benchmark gates have stable historical results.
- [ ] Only non-failing pull requests can enter the queue.
- [ ] Status check timeout is no more than `60` minutes.

## Required Repository Variables And Secrets

- [ ] `AU_KPIS_STAGING_BASE_URL` repository variable points at the staging API.
- [ ] `AU_KPIS_CONTRACT_DATAFLOW` repository variable is set when staging does
      not contain the default `abs.cpi` fixture.
- [ ] `AU_KPIS_CONTRACT_DIMENSION` repository variable is set when staging does
      not contain the default `region` dimension.
- [ ] `AU_KPIS_CONTRACT_SERIES_KEY` repository variable is set when staging does
      not contain the default all-`a` fixture series key.
- [ ] `K6_INFLUXDB_ADDR` repository variable points at shared k6 trend storage,
      or the workflow will use the local compose InfluxDB target where supported.
- [ ] `AU_KPIS_SMOKE_API_KEY` repository secret is set when staging requires an
      API key for k6 or Schemathesis volume.

## Failure Semantics

When `Merge Queue OK` fails or times out, GitHub removes the merge group from
the queue. Because `Merge Queue OK` depends on the full reusable PR flow and the
deep Schemathesis gate, no queued batch can partially land after a failed
merge-group build.

## Proof Required Before Closing Issue #15

- [ ] `.github/workflows/merge.yml` exists on the branch and is visible in the
      repository Actions tab after push.
- [ ] A `merge_group` run reports `Merge Queue OK`.
- [ ] Repository settings show merge queue required on `main`.
- [ ] Repository settings show required CODEOWNERS review on `main`.
- [ ] Repository settings show signed commits required on `main`.
- [ ] A failed merge-group check removes the group from the queue rather than
      merging any member PR.
