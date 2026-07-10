output "dashboard_url" {
  description = "Provisioned production operations dashboard URL."
  value       = grafana_dashboard.production_operations.url
}
output "synthetic_check_ids" {
  description = "Provisioned one-minute production synthetic checks."
  value = {
    api = grafana_synthetic_monitoring_check.api.id
    web = grafana_synthetic_monitoring_check.web.id
  }
}
