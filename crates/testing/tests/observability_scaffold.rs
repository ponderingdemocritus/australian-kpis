use std::{fs, path::Path};

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("testing crate lives under crates/testing")
}

#[test]
fn issue_47_observability_stack_contract_is_wired() {
    let root = repo_root();

    for path in [
        "infra/observability/otel-collector.yml",
        "infra/observability/prometheus/prometheus.yml",
        "infra/observability/prometheus/rules/slo-burn-rates.yml",
        "infra/observability/prometheus/rules/chaos-drill.test.yml",
        "infra/observability/alertmanager.yml",
        "infra/observability/loki.yml",
        "infra/observability/promtail.yml",
        "infra/observability/tempo.yml",
        "infra/observability/grafana/provisioning/datasources/datasources.yml",
        "infra/observability/grafana/provisioning/dashboards/dashboards.yml",
        "infra/observability/grafana/dashboards/freshness-heatmap.json",
        "infra/observability/grafana/dashboards/api-latency.json",
        "infra/observability/grafana/dashboards/error-rate.json",
        "infra/observability/grafana/dashboards/queue-db.json",
        "infra/observability/grafana/dashboards/slo-burn-rates.json",
        "tools/observability/chaos-drill.sh",
        "docs/observability.md",
    ] {
        assert!(
            root.join(path).is_file(),
            "issue #47 should provide observability asset `{path}`"
        );
    }

    let compose =
        fs::read_to_string(root.join("infra/compose/docker-compose.yml")).expect("read compose");
    for expected in [
        "otel-collector:",
        "prometheus:",
        "alertmanager:",
        "tempo:",
        "loki:",
        "promtail:",
        "grafana:",
        "pushgateway:",
        "OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector:4318/v1/traces",
        "AU_KPIS_TELEMETRY__LOG_FORMAT: json",
    ] {
        assert!(
            compose.contains(expected),
            "compose observability stack should include `{expected}`"
        );
    }

    let collector = fs::read_to_string(root.join("infra/observability/otel-collector.yml"))
        .expect("read collector config");
    for expected in ["otlp:", "tempo:4317", "prometheus:", "debug:"] {
        assert!(
            collector.contains(expected),
            "OTel collector should route traces/metrics/logs through `{expected}`"
        );
    }

    let dashboards = fs::read_to_string(
        root.join("infra/observability/grafana/provisioning/dashboards/dashboards.yml"),
    )
    .expect("read dashboard provisioning");
    assert!(
        dashboards.contains("path: /var/lib/grafana/dashboards"),
        "Grafana should provision the dashboard directory mounted by compose"
    );

    let slo_rules =
        fs::read_to_string(root.join("infra/observability/prometheus/rules/slo-burn-rates.yml"))
            .expect("read SLO rules");
    for expected in [
        "AuKpisApiAvailabilityFastBurn",
        "AuKpisApiAvailabilitySlowBurn",
        "AuKpisApiLatencyFastBurn",
        "AuKpisApiLatencySlowBurn",
        "AuKpisFreshnessFastBurn",
        "AuKpisFreshnessSlowBurn",
        "aemo.dispatch",
        "900",
        "AuKpisIngestionErrorFastBurn",
        "AuKpisIngestionErrorSlowBurn",
        "AuKpisSchemaHashDrift",
        "au_kpis_schema_hash_drifts_total",
        "AuKpisChaosDrillCanaryFiring",
        "severity: page",
        "team: data-platform",
    ] {
        assert!(
            slo_rules.contains(expected),
            "SLO rules should include `{expected}`"
        );
    }

    let alertmanager = fs::read_to_string(root.join("infra/observability/alertmanager.yml"))
        .expect("read alertmanager config");
    for expected in [
        "slack_configs:",
        "pagerduty_configs:",
        "__SLACK_WEBHOOK_URL__",
        "__PAGERDUTY_ROUTING_KEY__",
    ] {
        assert!(
            alertmanager.contains(expected),
            "Alertmanager should wire `{expected}`"
        );
    }

    let drill = fs::read_to_string(root.join("tools/observability/chaos-drill.sh"))
        .expect("read chaos drill script");
    for expected in [
        "promtool test rules",
        "au_kpis_chaos_error_ratio",
        "/api/v1/alerts",
        "AuKpisChaosDrillCanaryFiring",
    ] {
        assert!(
            drill.contains(expected),
            "chaos drill should exercise `{expected}`"
        );
    }
}
