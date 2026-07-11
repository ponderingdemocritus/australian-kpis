provider "grafana" {
  url             = var.grafana_url
  auth            = var.grafana_service_account_token
  sm_url          = var.synthetic_monitoring_url
  sm_access_token = var.synthetic_monitoring_token
}

locals {
  prometheus_rules = yamldecode(file("${path.module}/../prometheus/rules/slo-burn-rates.yml"))
  alert_rules = {
    for rule in local.prometheus_rules.groups[0].rules : rule.alert => rule
  }
}

resource "grafana_folder" "production" {
  title = "Australian KPIs Production"
  uid   = "au-kpis-production"
}

resource "grafana_dashboard" "production_operations" {
  folder = grafana_folder.production.uid
  config_json = replace(
    file("${path.module}/../grafana/dashboards/production-operations.json"),
    "au-kpis-prometheus",
    var.prometheus_datasource_uid,
  )
  overwrite = true
}

resource "grafana_rule_group" "production" {
  name             = "AU KPIs production pages"
  folder_uid       = grafana_folder.production.uid
  interval_seconds = 30

  dynamic "rule" {
    for_each = local.alert_rules
    content {
      name           = rule.key
      uid            = substr(md5(rule.key), 0, 16)
      condition      = "A"
      for            = rule.value.for
      no_data_state  = "Alerting"
      exec_err_state = "Alerting"
      annotations    = rule.value.annotations
      labels         = rule.value.labels

      data {
        ref_id         = "A"
        datasource_uid = var.prometheus_datasource_uid
        model = jsonencode({
          datasource = {
            type = "prometheus"
            uid  = var.prometheus_datasource_uid
          }
          editorMode    = "code"
          expr          = rule.value.expr
          format        = "table"
          instant       = true
          intervalMs    = 1000
          legendFormat  = "__auto"
          maxDataPoints = 43200
          range         = false
          refId         = "A"
        })
        relative_time_range {
          from = 21600
          to   = 0
        }
      }
    }
  }
}

resource "grafana_synthetic_monitoring_check" "api" {
  job       = "au-kpis-api-production"
  target    = "${var.api_base_url}/readyz"
  probes    = var.synthetic_probe_ids
  enabled   = true
  frequency = 60000
  timeout   = 10000

  settings {
    http {
      method = "GET"
    }
  }
}

resource "grafana_synthetic_monitoring_check" "web" {
  job       = "au-kpis-web-production"
  target    = var.web_base_url
  probes    = var.synthetic_probe_ids
  enabled   = true
  frequency = 60000
  timeout   = 10000

  settings {
    http {
      method = "GET"
    }
  }
}
