variable "grafana_url" {
  description = "Grafana Cloud stack URL."
  type        = string
}
variable "grafana_service_account_token" {
  description = "Grafana service-account token with folder, dashboard, and alert provisioning access."
  type        = string
  sensitive   = true
}

variable "prometheus_datasource_uid" {
  description = "UID of the Grafana Cloud Prometheus datasource receiving AU KPIs metrics."
  type        = string
}

variable "synthetic_monitoring_url" {
  description = "Synthetic Monitoring API URL shown in the Grafana Cloud Synthetics configuration."
  type        = string
}

variable "synthetic_monitoring_token" {
  description = "Synthetic Monitoring access token."
  type        = string
  sensitive   = true
}

variable "synthetic_probe_ids" {
  description = "Public probe IDs selected for the production HTTP checks."
  type        = set(number)

  validation {
    condition     = length(var.synthetic_probe_ids) >= 2
    error_message = "At least two independent public probes are required."
  }
}

variable "api_base_url" {
  description = "Public production API origin, without a trailing slash."
  type        = string

  validation {
    condition     = startswith(var.api_base_url, "https://") && !endswith(var.api_base_url, "/")
    error_message = "api_base_url must use HTTPS and omit the trailing slash."
  }
}

variable "web_base_url" {
  description = "Public production web origin, without a trailing slash."
  type        = string

  validation {
    condition     = startswith(var.web_base_url, "https://") && !endswith(var.web_base_url, "/")
    error_message = "web_base_url must use HTTPS and omit the trailing slash."
  }
}
