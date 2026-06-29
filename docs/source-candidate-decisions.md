# Source Candidate Decisions

This note tracks weak APS/source candidates that are intentionally not made
score-ready in v1. The default policy is conservative: official/core sources can
score when provenance and measure mapping are reviewed; weak proxies remain
`manual_pending`, `coverage_gap`, or `visible_unscored`.

## Decisions

| Current dataflow | Status | Decision |
| --- | --- | --- |
| `compute.au_datacentre_capacity_mw` | `manual_pending` | Do not score the curated `example.test` placeholder. Replacement candidate is `aemo.data_centre_demand`, sourced from AEMO IASR/ESOO data-centre electricity-demand forecasts. Review units, region coverage, forecast vintage, and whether demand is an acceptable proxy before adding a scored adapter. |
| `home_affairs.skillselect_talent_proxy` | `coverage_gap` | No score-ready replacement yet. Current SkillSelect pages do not publish AI-related invitation counts, so keep this as a visible coverage gap until a reviewed labour/talent source is selected. |
| `curated.surveillance_intensity` | `visible_unscored` | No current scored replacement. Keep as context only until a current, Australia-specific surveillance taxonomy and source cadence are reviewed. |
| `pc.productivity_bulletin` | score-ready current source | Keep the Productivity Commission source for v1. ABS productivity tables are a future primary-source candidate if the APS mapping needs a more direct official statistical series. |

## Follow-up Gate

Before any candidate becomes scored, add or update the adapter/dataflow,
document provenance and mapping in `Spec.md` or source docs, add fixture-backed
parser tests, and update APS config review metadata.
