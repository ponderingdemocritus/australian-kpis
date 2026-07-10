#!/usr/bin/env python3
"""Generate a promtool fire-and-clear test for every page-level alert."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Alert:
    name: str
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)


DYNAMIC_LABELS = {
    "AuKpisFreshnessFastBurn": {"dataflow": "abs.cpi"},
    "AuKpisFreshnessSlowBurn": {"dataflow": "abs.cpi"},
    "AuKpisSchemaHashDrift": {
        "source": "treasury",
        "dataflow": "treasury.budget_papers",
    },
    "AuKpisSyntheticApiUnavailable": {"job": "au-kpis-api-production"},
    "AuKpisSyntheticWebUnavailable": {"job": "au-kpis-web-production"},
    "AuKpisChaosDrillCanaryFiring": {"service": "chaos-drill"},
}

INPUT_SERIES = [
    (
        'au_kpis_http_requests_total{eligible="true",status="500"}',
        "0+100x120 12000+0x360",
    ),
    ('au_kpis_http_requests_total{eligible="true",status="200"}', "0+0x120 0+100x360"),
    (
        'au_kpis_http_request_duration_seconds_bucket{eligible="true",le="0.1"}',
        "0+0x120 0+100x360",
    ),
    (
        'au_kpis_http_request_duration_seconds_bucket{eligible="true",le="0.5"}',
        "0+0x120 0+100x360",
    ),
    (
        'au_kpis_http_request_duration_seconds_bucket{eligible="true",le="1"}',
        "0+100x480",
    ),
    (
        'au_kpis_http_request_duration_seconds_bucket{eligible="true",le="+Inf"}',
        "0+100x480",
    ),
    ('au_kpis_ingestion_lag_seconds{dataflow="abs.cpi"}', "100x120 0x360"),
    ('au_kpis_ingestion_freshness_budget_seconds{dataflow="abs.cpi"}', "10x480"),
    ("au_kpis_ingestion_generation_failures_recent", "1x120 0x360"),
    ("au_kpis_queue_oldest_pending_age_seconds", "1000x120 0x360"),
    (
        'au_kpis_schema_hash_drifts_total{source="treasury",dataflow="treasury.budget_papers"}',
        "0+1x120 120+0x360",
    ),
    (
        'au_kpis_stream_duration_seconds_bucket{format="parquet",le="10"}',
        "0+0x120 0+100x360",
    ),
    (
        'au_kpis_stream_duration_seconds_bucket{format="parquet",le="30"}',
        "0+0x120 0+100x360",
    ),
    ('au_kpis_stream_duration_seconds_bucket{format="parquet",le="60"}', "0+100x480"),
    ('au_kpis_stream_duration_seconds_bucket{format="parquet",le="+Inf"}', "0+100x480"),
    ('au_kpis_db_pool_connections{state="in_use"}', "9x120 1x360"),
    ('au_kpis_db_pool_connections{state="maximum"}', "10x480"),
    ("au_kpis_db_replication_lag_seconds", "301x120 0x360"),
    ("au_kpis_redis_up", "0x120 1x360"),
    ("au_kpis_metrics_collection_success", "0x120 1x360"),
    ('au_kpis_queue_depth{status="dead"}', "1x120 0x360"),
    ("au_kpis_aps_snapshot_present", "0x120 1x360"),
    ("au_kpis_aps_snapshot_age_seconds", "129601x120 0x360"),
    ("au_kpis_aps_coverage_percent", "69x120 100x360"),
    ("au_kpis_webhook_oldest_due_age_seconds", "301x120 0x360"),
    ("au_kpis_webhook_dead_letters_recent", "1x120 0x360"),
    ("au_kpis_scheduler_leader_active", "0x120 1x360"),
    ('probe_success{job="au-kpis-api-production"}', "0x120 1x360"),
    ('probe_success{job="au-kpis-web-production"}', "0x120 1x360"),
    ('au_kpis_chaos_error_ratio{service="chaos-drill"}', "0.02x120 0x360"),
]


def parse_alerts(path: Path) -> list[Alert]:
    alerts: list[Alert] = []
    current: Alert | None = None
    section: str | None = None
    for raw in path.read_text().splitlines():
        match = re.match(r"^\s{6}- alert: (\S+)\s*$", raw)
        if match:
            current = Alert(match.group(1))
            alerts.append(current)
            section = None
            continue
        if current is None:
            continue
        if re.match(r"^\s{8}labels:\s*$", raw):
            section = "labels"
            continue
        if re.match(r"^\s{8}annotations:\s*$", raw):
            section = "annotations"
            continue
        entry = re.match(r"^\s{10}([a-zA-Z_][a-zA-Z0-9_]*):\s*(.*?)\s*$", raw)
        if entry and section:
            value = entry.group(2).strip('"')
            getattr(current, section)[entry.group(1)] = value
        elif raw.strip() and len(raw) - len(raw.lstrip()) <= 8:
            section = None
    page_alerts = [alert for alert in alerts if alert.labels.get("severity") == "page"]
    if not page_alerts:
        raise SystemExit("no page-level alerts found")
    return page_alerts


def quote(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def rendered_annotations(alert: Alert, labels: dict[str, str]) -> dict[str, str]:
    output = {}
    for key, value in alert.annotations.items():
        for label, replacement in labels.items():
            value = value.replace(f"{{{{ $labels.{label} }}}}", replacement)
        output[key] = value
    return output


def generate_fixture(alerts: list[Alert]) -> str:
    lines = [
        "rule_files:",
        "  - slo-burn-rates.yml",
        "",
        "evaluation_interval: 1m",
        "",
        "tests:",
        "  - interval: 1m",
        "    input_series:",
    ]
    for series, values in INPUT_SERIES:
        lines.extend(
            [f"      - series: {quote(series)}", f"        values: {quote(values)}"]
        )
    lines.append("    alert_rule_test:")
    for alert in alerts:
        dynamic = DYNAMIC_LABELS.get(alert.name, {})
        labels = {"alertname": alert.name, **dynamic, **alert.labels}
        annotations = rendered_annotations(alert, labels)
        lines.extend(
            [
                "      - eval_time: 120m",
                f"        alertname: {alert.name}",
                "        exp_alerts:",
                "          - exp_labels:",
            ]
        )
        for key, value in sorted(labels.items()):
            lines.append(f"              {key}: {quote(value)}")
        lines.append("            exp_annotations:")
        for key, value in sorted(annotations.items()):
            lines.append(f"              {key}: {quote(value)}")
        lines.extend(
            [
                "      - eval_time: 480m",
                f"        alertname: {alert.name}",
                "        exp_alerts: []",
            ]
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rules", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    args = parser.parse_args()

    alerts = parse_alerts(args.rules)
    args.output.write_text(generate_fixture(alerts))
    args.inventory.write_text(
        json.dumps(
            {
                "status": "fixture_generated",
                "page_alert_count": len(alerts),
                "fire_evaluation": "120m",
                "clear_evaluation": "480m",
                "alerts": [alert.name for alert in alerts],
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
