#!/usr/bin/env python3
"""Summarise cargo-mutants outcomes for CI and follow-up issue triage."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


CAUGHT = {"CaughtMutant"}
MISSED = {"MissedMutant", "Success"}
TIMEOUT = {"Timeout", "TimedOut"}
UNVIABLE = {"Unviable"}


def main() -> int:
    args = parse_args()
    outcomes_path = args.out_dir / "outcomes.json"
    if not outcomes_path.is_file():
        print(f"cargo-mutants outcomes not found: {outcomes_path}", file=sys.stderr)
        return 2

    outcomes = json.loads(outcomes_path.read_text()).get("outcomes", [])
    if not outcomes:
        print("cargo-mutants outcomes.json did not contain any outcomes", file=sys.stderr)
        return 2

    summary = summarize(outcomes, args.min_score)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.issue_body.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.write_text(markdown_report(summary))
    args.json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    args.issue_body.write_text(issue_body(summary))

    print(
        "Mutation score: "
        f"{summary['score']:.2f}% "
        f"({summary['caught']}/{summary['scored_total']} caught, "
        f"{summary['missed']} surviving, threshold {summary['min_score']:.2f}%)"
    )
    return 0 if summary["passed"] else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Path to cargo-mutants mutants.out directory.",
    )
    parser.add_argument("--min-score", type=float, required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument("--issue-body", type=Path, required=True)
    return parser.parse_args()


def summarize(outcomes: list[dict[str, Any]], min_score: float) -> dict[str, Any]:
    caught = 0
    missed = 0
    timeout = 0
    unviable = 0
    other = 0
    survivors: list[dict[str, Any]] = []

    for outcome in outcomes:
        kind = str(outcome.get("summary", ""))
        if kind in CAUGHT:
            caught += 1
        elif kind in MISSED:
            missed += 1
            survivors.append(mutant_details(outcome))
        elif kind in TIMEOUT:
            timeout += 1
            survivors.append(mutant_details(outcome))
        elif kind in UNVIABLE:
            unviable += 1
        else:
            other += 1

    scored_total = caught + missed + timeout
    if scored_total == 0:
        score = 0.0
    else:
        score = (caught / scored_total) * 100.0

    return {
        "caught": caught,
        "missed": missed,
        "timeout": timeout,
        "unviable": unviable,
        "other": other,
        "scored_total": scored_total,
        "total_outcomes": len(outcomes),
        "score": round(score, 2),
        "min_score": float(min_score),
        "passed": score >= min_score,
        "survivors": survivors,
    }


def mutant_details(outcome: dict[str, Any]) -> dict[str, Any]:
    mutant = outcome.get("scenario", {}).get("Mutant", {})
    span = mutant.get("span", {}).get("start", {})
    function = mutant.get("function", {})
    line = span.get("line")
    return {
        "summary": outcome.get("summary", ""),
        "package": mutant.get("package", ""),
        "file": mutant.get("file", ""),
        "line": line,
        "column": span.get("column"),
        "function": function.get("function_name", ""),
        "replacement": mutant.get("replacement", ""),
        "diff_path": outcome.get("diff_path", ""),
    }


def markdown_report(summary: dict[str, Any]) -> str:
    lines = [
        "# cargo-mutants Report",
        "",
        f"Mutation score: {summary['score']:.2f}%",
        f"Threshold: {summary['min_score']:.2f}%",
        f"Status: {'pass' if summary['passed'] else 'fail'}",
        "",
        "| Outcome | Count |",
        "|---|---:|",
        f"| Caught | {summary['caught']} |",
        f"| Surviving | {summary['missed']} |",
        f"| Timed out | {summary['timeout']} |",
        f"| Unviable | {summary['unviable']} |",
        f"| Other | {summary['other']} |",
        "",
    ]
    if summary["survivors"]:
        lines.extend(["## Surviving Mutants", ""])
        for survivor in summary["survivors"]:
            location = format_location(survivor)
            lines.extend(
                [
                    f"- `{location}` `{survivor['function']}`",
                    f"  - Replacement: `{survivor['replacement']}`",
                    f"  - Summary: `{survivor['summary']}`",
                ]
            )
            if survivor.get("diff_path"):
                lines.append(f"  - Diff: `{survivor['diff_path']}`")
    else:
        lines.extend(["## Surviving Mutants", "", "None."])
    lines.append("")
    return "\n".join(lines)


def issue_body(summary: dict[str, Any]) -> str:
    lines = [
        "## Follow-up add test work",
        "",
        "The weekly `cargo-mutants` run found surviving mutants. Add tests that catch the listed behavioral changes, then close this issue from the fixing PR.",
        "",
        f"- Mutation score: {summary['score']:.2f}%",
        f"- Threshold: {summary['min_score']:.2f}%",
        f"- Surviving mutants: {summary['missed']}",
        f"- Timed-out mutants: {summary['timeout']}",
        "",
        "## Surviving mutants",
        "",
    ]
    if summary["survivors"]:
        for survivor in summary["survivors"]:
            lines.append(
                f"- `{format_location(survivor)}` `{survivor['function']}` -> `{survivor['replacement']}`"
            )
    else:
        lines.append("None.")
    lines.append("")
    return "\n".join(lines)


def format_location(survivor: dict[str, Any]) -> str:
    file = survivor.get("file", "")
    line = survivor.get("line")
    if line is None:
        return str(file)
    return f"{file}:{line}"


if __name__ == "__main__":
    raise SystemExit(main())
