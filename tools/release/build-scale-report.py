#!/usr/bin/env python3
"""Validate k6, provider, seed, and Parquet production-scale evidence."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


ENDPOINT_BUDGETS_MS = {
    "warm-single-series": 200.0,
    "cold-range-json": 500.0,
    "rollups": 500.0,
    "aps-latest": 200.0,
    "aps-history": 500.0,
    "bulk-parquet": 30_000.0,
}
DEGRADATION_ENDPOINTS = [
    "warm-single-series",
    "cold-range-json",
    "rollups",
    "aps-latest",
    "aps-history",
]


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def metric(summary: dict, name: str) -> dict:
    try:
        return summary["metrics"][name]
    except KeyError as error:
        raise ValueError(f"k6 summary is missing metric `{name}`") from error


def p95(summary: dict, endpoint: str) -> float:
    values = metric(summary, f"http_req_duration{{endpoint:{endpoint}}}")["values"]
    return float(values["p(95)"])


def thresholds_pass(summary: dict) -> bool:
    for details in summary.get("metrics", {}).values():
        for result in details.get("thresholds", {}).values():
            passed = result.get("ok") if isinstance(result, dict) else result
            if passed is not True:
                return False
    return True


def extract_peak_heap(path: Path) -> int:
    match = re.search(r"max_bytes=(\d+)", path.read_text())
    if not match:
        raise ValueError("Parquet DHAT log does not contain max_bytes")
    return int(match.group(1))


def extract_scale_elapsed(path: Path) -> int:
    match = re.search(r"elapsed_ms=(\d+)", path.read_text())
    if not match:
        raise ValueError("Parquet partition log does not contain elapsed_ms")
    return int(match.group(1))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--parquet-time-log", type=Path, required=True)
    parser.add_argument("--parquet-heap-log", type=Path, required=True)
    parser.add_argument("--db-cpu", type=float, required=True)
    parser.add_argument("--pool-utilization", type=float, required=True)
    parser.add_argument("--ingestion-jobs", type=float, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    current = load(args.current)
    baseline = load(args.baseline)
    seed = load(args.seed)
    checks: list[dict] = []

    def add(name: str, measured: object, budget: object, passed: bool) -> None:
        checks.append(
            {"name": name, "measured": measured, "budget": budget, "passed": passed}
        )

    add("k6 thresholds", thresholds_pass(current), True, thresholds_pass(current))
    endpoint_values = {}
    for endpoint, budget in ENDPOINT_BUDGETS_MS.items():
        measured = p95(current, endpoint)
        endpoint_values[endpoint] = measured
        add(f"{endpoint} p95", measured, f"< {budget} ms", measured < budget)

    server_error = float(metric(current, "server_error_rate")["values"]["rate"])
    dropped = float(metric(current, "dropped_iterations")["values"]["count"])
    add("server error rate", server_error, "< 0.001", server_error < 0.001)
    add("dropped iterations", dropped, "0", dropped == 0)

    degradation = {}
    for endpoint in DEGRADATION_ENDPOINTS:
        paused = p95(baseline, endpoint)
        concurrent = endpoint_values[endpoint]
        ratio = (concurrent / paused) - 1.0 if paused > 0 else float("inf")
        degradation[endpoint] = ratio
        add(f"{endpoint} concurrent degradation", ratio, "<= 0.20", ratio <= 0.20)

    add("database CPU", args.db_cpu, "< 0.70", 0 <= args.db_cpu < 0.70)
    add(
        "database pool utilization",
        args.pool_utilization,
        "< 0.80",
        0 <= args.pool_utilization < 0.80,
    )
    add("concurrent ingestion jobs", args.ingestion_jobs, "> 0", args.ingestion_jobs > 0)
    add("catalog dataflows", seed.get("catalog_dataflows"), 100, seed.get("catalog_dataflows") == 100)
    add("launch dataflows", seed.get("launch_dataflows"), 20, seed.get("launch_dataflows") == 20)
    add(
        "seed observations",
        seed.get("actual_observations"),
        50_000_000,
        seed.get("actual_observations") == 50_000_000,
    )
    add("seed revisions", seed.get("revision_observations"), "> 0", seed.get("revision_observations", 0) > 0)
    add("five-minute AEMO rows", seed.get("aemo_five_minute_rows"), "> 0", seed.get("aemo_five_minute_rows", 0) > 0)
    add("compressed chunks", seed.get("chunks_compressed"), "> 0", seed.get("chunks_compressed", 0) > 0)

    peak_heap = extract_peak_heap(args.parquet_heap_log)
    partition_elapsed_ms = extract_scale_elapsed(args.parquet_time_log)
    add("1M-row Parquet peak heap", peak_heap, "< 104857600 bytes", peak_heap < 100 * 1024 * 1024)
    add("10x1M offline Parquet", partition_elapsed_ms, "< 30000 ms", partition_elapsed_ms < 30_000)

    status = "passed" if all(check["passed"] for check in checks) else "failed"
    report = {
        "status": status,
        "workload": {
            "duration": "30m",
            "requests_per_second": 1000,
            "mix_percent": {
                "warm_single_series_json": 45,
                "cold_range_json": 15,
                "rollups": 10,
                "catalog_source_search": 10,
                "aps_latest": 10,
                "aps_history": 5,
                "validation_rate_limit": 5,
            },
            "bulk_exports_per_api_replica": 4,
            "ingestion_concurrent": args.ingestion_jobs > 0,
        },
        "seed": seed,
        "endpoint_p95_ms": endpoint_values,
        "concurrent_degradation_ratio": degradation,
        "checks": checks,
    }
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "scale-report.json").write_text(json.dumps(report, indent=2) + "\n")
    rows = [
        "# Production scale certification",
        "",
        f"Status: `{status}`",
        "",
        "| Gate | Measured | Budget | Status |",
        "|---|---:|---:|---:|",
    ]
    for check in checks:
        gate_status = "passed" if check["passed"] else "failed"
        rows.append(
            f"| {check['name']} | `{check['measured']}` | `{check['budget']}` | `{gate_status}` |"
        )
    (args.output / "summary.md").write_text("\n".join(rows) + "\n")
    if status != "passed":
        raise SystemExit("one or more production scale gates failed")


if __name__ == "__main__":
    main()
