## CI And Spec Review Guide

Applies to:
- `.github/workflows/**`
- `Spec.md`
- `AGENTS.md`
- `CONTRIBUTING.md`

Primary review goal:
- keep repository policy, CI behavior, and documented contracts aligned

Focus on:
- weakened gates
- silent skips
- workflow behavior diverging from spec
- missing `Spec.md` updates when architecture or CI contracts change

Ask:
- does this change weaken a stated gate or silently bypass one?
- is there a workflow/contract change without a matching spec update?
- is the review signal advisory vs blocking behavior still explicit?
