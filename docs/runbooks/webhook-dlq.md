# Webhook DLQ

Page conditions are terminal deliveries, a due-delivery age above five minutes,
or a subscription paused after five consecutive failures.

1. Identify the subscription and stable event ID in Grafana traces. Confirm the
   destination still belongs to the API-key owner.
2. Inspect status, attempts, last response code/error, lease expiry, next
   attempt, and payload hash using read-only database access. Never print the
   encrypted signing secret or full customer payload into chat or an issue.
3. Classify the cause. Retry 408/409/425/429/5xx and network failures after the
   receiver recovers. Do not retry permanent 4xx, revoked subscriptions, SSRF
   validation failures, or a destination whose DNS/IP is no longer public.
4. For DNS changes, rerun HTTPS-only URL, redirect, mixed-DNS, rebinding, and
   resolved-IP validation before reactivation.

DLQ replay is an audited database operation executed by the Platform database
operator. In one transaction, lock the delivery and subscription, retain the
same event ID and payload, set an eligible terminal delivery to `pending`, clear
lease ownership/expiry, set `next_attempt_at=now()`, reset the subscription's
consecutive failures only after an explicit receiver canary succeeds, and write
`operator_audit_log`. Never insert a duplicate delivery row.

Start the webhook worker canary and verify one signed challenge/delivery. Then
watch delivery age, retry count, terminal failures, and subscription state until
the backlog drains. A successful delivery resets consecutive failures; it does
not erase prior attempt history.

Close with the subscription ID, event ID, actor/reason, receiver confirmation,
trace, SQL audit ID, and alert fire/clear timestamps. Escalate suspected secret
or ownership compromise to Security and revoke rather than replay.
