#!/usr/bin/env python3
"""Build a PR comment comparing k6 load summaries against the last nightly run."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


SCENARIOS: dict[str, list[tuple[str, str, str]]] = {
    "sustained": [
        ("HTTP p95", "http_req_duration", "p(95)"),
        ("HTTP p99", "http_req_duration", "p(99)"),
        ("Failure rate", "http_req_failed", "rate"),
    ],
    "burst": [
        ("Rate-limit ratio", "rate_limit_ratio", "rate"),
        ("Server-error ratio", "server_error_ratio", "rate"),
        ("Rate-limit seen", "rate_limit_seen", "rate"),
    ],
    "full-load": [
        ("HTTP p99", "http_req_duration{endpoint:observations}", "p(99)"),
        ("Failure rate", "http_req_failed", "rate"),
        ("Dropped iterations", "dropped_iterations", "count"),
    ],
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pr", default="")
    args = parser.parse_args()

    lines = [
        "<!-- k6 load comparison -->",
        "## k6 load comparison",
        "",
        "Triggered by the `perf:regression` PR label or manual dispatch.",
    ]
    if args.pr:
        lines.append(f"PR: #{args.pr}")
    lines.extend(
        [
            "",
            "| Scenario | Metric | Current | Nightly baseline | Delta |",
            "|---|---:|---:|---:|---:|",
        ]
    )

    for scenario, metrics in SCENARIOS.items():
        current = read_summary(args.current, scenario)
        baseline = read_summary(args.baseline, scenario)
        for label, metric, value_name in metrics:
            current_value = metric_value(current, metric, value_name)
            baseline_value = metric_value(baseline, metric, value_name)
            lines.append(
                "| {scenario} | {label} | {current} | {baseline} | {delta} |".format(
                    scenario=scenario,
                    label=label,
                    current=format_value(current_value, value_name),
                    baseline=format_value(baseline_value, value_name),
                    delta=format_delta(current_value, baseline_value),
                )
            )

    lines.extend(
        [
            "",
            "Threshold failures keep this workflow red; the table shows the current run next to the latest successful scheduled baseline when that artifact is available.",
        ]
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_summary(directory: Path, scenario: str) -> dict[str, Any] | None:
    if not directory.exists():
        return None
    for path in [directory / f"{scenario}-summary.json", *directory.rglob(f"{scenario}-summary.json")]:
        if path.is_file():
            with path.open(encoding="utf-8") as handle:
                return json.load(handle)
    return None


def metric_value(summary: dict[str, Any] | None, metric: str, value_name: str) -> float | None:
    if summary is None:
        return None
    metric_data = summary.get("metrics", {}).get(metric, {})
    value = metric_data.get(value_name)
    if not isinstance(value, (int, float)) and value_name == "rate":
        value = metric_data.get("value")
    if not isinstance(value, (int, float)):
        values = metric_data.get("values", {})
        if isinstance(values, dict):
            value = values.get(value_name)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def format_value(value: float | None, value_name: str) -> str:
    if value is None:
        return "n/a"
    if value_name == "rate":
        return f"{value * 100:.3f}%"
    if value_name == "count":
        return f"{value:.0f}"
    return f"{value:.1f} ms"


def format_delta(current: float | None, baseline: float | None) -> str:
    if current is None or baseline in (None, 0):
        return "n/a"
    return f"{((current - baseline) / baseline) * 100:+.2f}%"


if __name__ == "__main__":
    main()
