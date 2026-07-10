# Grafana Cloud production configuration

This Terraform module provisions the production folder, operations dashboard,
all page-level rules from `../prometheus/rules/slo-burn-rates.yml`, and API/web
HTTP checks at a one-minute interval. It does not create credentials, contact
points, or state storage.

Use a remote Terraform backend approved by Platform and inject variables from
the protected production environment. Do not commit `.tfvars`, state, access
tokens, or probe credentials.

```bash
terraform -chdir=infra/observability/grafana-cloud init \
  -backend-config=/secure/path/backend.hcl
terraform -chdir=infra/observability/grafana-cloud plan \
  -var-file=/secure/path/production.tfvars
terraform -chdir=infra/observability/grafana-cloud apply \
  -var-file=/secure/path/production.tfvars
```

The Grafana service account needs folder, dashboard, and alert-rule write
access. Synthetic Monitoring uses its own access token. Select at least two
public probes from different locations. Notification policies and escalation
destinations remain organization-owned and must route `severity=page` to the
production on-call rotation before launch.
