# On-call

## Ownership

| Surface | Primary | Secondary |
|---|---|---|
| API, edge, Railway, Redis, deploy | Platform | API |
| Timescale, queues, migrations, restore | Platform | Data |
| Adapters, freshness, schema drift, data quality | Data | Platform |
| APS publication and manual inputs | Product/Methodology | Data |
| Web/BFF and accessibility | Web | API |
| Subscriptions, webhook delivery, SSRF events | API | Security |

The current weekly rota and PagerDuty escalation policy are the source of truth
for named people. Never put personal phone numbers or provider tokens in this
repository.

## Page Triage

1. Acknowledge the page, open the production operations dashboard, and record
   the alert start, deployment SHA, affected route/dataflow, and trace ID.
2. Check `/livez` and `/readyz`. A failed `/livez` is a process incident; a
   database failure in `/readyz` is a state-plane incident; Redis degradation
   leaves public GETs available but blocks protected writes.
3. Freeze risky changes. Do not restart every replica simultaneously and do not
   mutate artifacts or published APS snapshots.
4. Mitigate from the relevant runbook: deploy rollback, source pause/replay,
   webhook DLQ, or database/object restore.
5. Verify the alert clears, synthetics recover, queue age falls, and no partial
   generation became visible. Attach dashboard links and command output to the
   incident record.

## Severity

- **SEV-1:** public outage, data corruption, security boundary failure, or no
  viable database recovery. Page Platform, Data, Security, and incident lead.
- **SEV-2:** SLO burn, stale active dataflow, missing APS publication, webhook
  backlog, or single dependency degradation. Page the owning team and Platform.
- **SEV-3:** non-page warning, one consumer issue, or expected manual coverage
  gap. Track in the issue queue during business hours.

Escalate a still-burning page after 15 minutes or any uncertain integrity event
immediately. Customer-facing updates state observed impact and verified facts;
they do not speculate about root cause.

## Closeout

An incident closes only after metrics and synthetics are healthy, durable
backlogs are drained, source/R2 reconciliation passes when relevant, and a
follow-up owner is assigned. Retain alert, trace, deploy, and operator-audit
evidence for the post-incident review.
