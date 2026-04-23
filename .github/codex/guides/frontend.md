## Frontend Review Guide

Applies to:
- `apps/web/**`

Primary review goal:
- protect correctness and accessibility of the reference client

Focus on:
- broken data assumptions from API changes
- missing Playwright or axe coverage
- regressions in key explorer/compare flows

Ask:
- does this UI change depend on an API contract that is not covered elsewhere?
- should there be E2E or accessibility coverage?
- is the change hiding a deeper contract mismatch rather than just a UI issue?
