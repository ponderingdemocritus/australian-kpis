#!/usr/bin/env python3
"""Generate and validate source-research artifacts from audit findings."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import re
import sys
import tomllib
from typing import Any

ACTIONABLE_STATUSES = {"manual_review", "bot_filtered", "drift", "error"}
CLASSIFICATIONS = {
    "same_source",
    "moved",
    "bot_filtered",
    "source_retired",
    "candidate_replacement",
    "insufficient_evidence",
}
EVIDENCE_CHECKLIST = [
    "official publisher URL",
    "license or usage terms",
    "attribution text",
    "cadence or release timing",
    "source/dataflow scope match",
]


def load_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_register(path: pathlib.Path) -> dict[str, dict[str, Any]]:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    return {
        str(dataflow["dataflow_id"]): dataflow
        for dataflow in raw.get("dataflows", [])
    }


def slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def artifact_id(dataflow_id: str, occurrence: int) -> str:
    base = slug(dataflow_id)
    if occurrence == 1:
        return base
    return f"{base}__{occurrence}"


def now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


def should_research(finding: dict[str, Any], dataflow_id: str | None) -> bool:
    if dataflow_id and finding.get("dataflow_id") != dataflow_id:
        return False
    severity = str(finding.get("severity", ""))
    return severity in ACTIONABLE_STATUSES


def build_research_artifact(
    finding: dict[str, Any],
    register: dict[str, dict[str, Any]],
    retrieved_at: str,
) -> dict[str, Any]:
    dataflow_id = str(finding["dataflow_id"])
    register_record = register.get(dataflow_id)
    if register_record is None:
        raise ValueError(f"missing register record for {dataflow_id}")

    allowed_domains = []
    canonical_url = str(register_record.get("canonical_url", ""))
    if canonical_url.startswith("http"):
        host = canonical_url.split("/", 3)[2]
        allowed_domains.append(host)

    return {
        "schema_version": "source-research.v1",
        "artifact_id": "",
        "source_id": finding["source_id"],
        "dataflow_id": dataflow_id,
        "current_url": finding["current_url"],
        "audit_evidence": finding["evidence"],
        "audit_severity": finding["severity"],
        "register_status": register_record["status"],
        "register_canonical_url": canonical_url,
        "allowed_domains": allowed_domains,
        "required_evidence": EVIDENCE_CHECKLIST,
        "classification": "insufficient_evidence",
        "source_urls": [],
        "publisher_names": [],
        "retrieved_at": retrieved_at,
        "license_evidence": str(register_record.get("license", "")),
        "attribution_evidence": str(register_record.get("attribution", "")),
        "cadence_evidence": str(register_record.get("cadence", "")),
        "recommendation": finding["recommendation"],
        "risk_notes": [
            "Automated scheduled mode created a bounded research packet only.",
            "Human or approved agent review must add primary-source evidence before source mappings change.",
        ],
    }


def validate_research_artifact(artifact: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required_strings = [
        "schema_version",
        "source_id",
        "dataflow_id",
        "current_url",
        "audit_evidence",
        "audit_severity",
        "register_status",
        "register_canonical_url",
        "classification",
        "retrieved_at",
        "license_evidence",
        "attribution_evidence",
        "cadence_evidence",
        "recommendation",
    ]
    for field in required_strings:
        value = artifact.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{field} must be a non-empty string")

    if artifact.get("schema_version") != "source-research.v1":
        errors.append("schema_version must be source-research.v1")
    if artifact.get("classification") not in CLASSIFICATIONS:
        errors.append("classification is not allowed")
    for field in ["allowed_domains", "required_evidence", "source_urls", "publisher_names", "risk_notes"]:
        if not isinstance(artifact.get(field), list):
            errors.append(f"{field} must be a list")
    if not artifact.get("required_evidence"):
        errors.append("required_evidence must not be empty")
    if artifact.get("audit_severity") not in ACTIONABLE_STATUSES:
        errors.append("audit_severity is not actionable for research")
    return errors


def render_markdown(artifact: dict[str, Any]) -> str:
    lines = [
        f"# Source Research: {artifact['dataflow_id']}",
        "",
        f"- Source: `{artifact['source_id']}`",
        f"- Dataflow: `{artifact['dataflow_id']}`",
        f"- Classification: `{artifact['classification']}`",
        f"- Register status: `{artifact['register_status']}`",
        f"- Retrieved at: `{artifact['retrieved_at']}`",
        f"- Current URL: {artifact['current_url']}",
        f"- Register URL: {artifact['register_canonical_url']}",
        "",
        "## Audit Evidence",
        "",
        artifact["audit_evidence"],
        "",
        "## Required Evidence",
        "",
    ]
    lines.extend(f"- [ ] {item}" for item in artifact["required_evidence"])
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            artifact["recommendation"],
            "",
            "## Risk Notes",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in artifact["risk_notes"])
    lines.append("")
    return "\n".join(lines)


def generate(args: argparse.Namespace) -> int:
    report = load_json(args.report)
    register = load_register(args.register)
    args.out.mkdir(parents=True, exist_ok=True)
    retrieved_at = now_iso()
    artifacts: list[dict[str, Any]] = []
    occurrences: dict[str, int] = {}

    for finding in report.get("findings", []):
        if not should_research(finding, args.dataflow_id):
            continue
        artifact = build_research_artifact(finding, register, retrieved_at)
        dataflow_id = artifact["dataflow_id"]
        occurrences[dataflow_id] = occurrences.get(dataflow_id, 0) + 1
        artifact["artifact_id"] = artifact_id(dataflow_id, occurrences[dataflow_id])
        errors = validate_research_artifact(artifact)
        if errors:
            raise ValueError(f"{artifact.get('dataflow_id', 'unknown')}: {'; '.join(errors)}")
        stem = artifact["artifact_id"]
        (args.out / f"{stem}.json").write_text(
            json.dumps(artifact, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (args.out / f"{stem}.md").write_text(render_markdown(artifact), encoding="utf-8")
        artifacts.append(artifact)

    summary = {
        "schema_version": "source-research-summary.v1",
        "generated_at": retrieved_at,
        "artifacts_total": len(artifacts),
        "dataflow_ids": [artifact["dataflow_id"] for artifact in artifacts],
        "artifacts": [
            {
                "artifact_id": artifact["artifact_id"],
                "dataflow_id": artifact["dataflow_id"],
                "audit_severity": artifact["audit_severity"],
                "current_url": artifact["current_url"],
            }
            for artifact in artifacts
        ],
    }
    (args.out / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


def validate(args: argparse.Namespace) -> int:
    failed = False
    for path in sorted(args.research_dir.glob("*.json")):
        if path.name == "summary.json":
            continue
        artifact = load_json(path)
        errors = validate_research_artifact(artifact)
        if errors:
            failed = True
            print(f"{path}: {'; '.join(errors)}", file=sys.stderr)
    return 1 if failed else 0


def render_comment(args: argparse.Namespace) -> int:
    summary_path = args.research_dir / "summary.json"
    summary = load_json(summary_path)
    lines = [
        "<!-- source-research-review:summary -->",
        "## Source Research Review",
        "",
        f"Artifacts generated: `{summary['artifacts_total']}`",
        "",
    ]
    artifacts = summary.get("artifacts") or [
        {"artifact_id": slug(dataflow_id), "dataflow_id": dataflow_id}
        for dataflow_id in summary["dataflow_ids"]
    ]
    for item in artifacts:
        artifact = load_json(args.research_dir / f"{item['artifact_id']}.json")
        lines.append(
            f"- `{item['dataflow_id']}` (`{item['artifact_id']}`): `{artifact['classification']}`; {artifact['recommendation']}"
        )
    print("\n".join(lines))
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate")
    gen.add_argument("--report", type=pathlib.Path, required=True)
    gen.add_argument("--register", type=pathlib.Path, required=True)
    gen.add_argument("--out", type=pathlib.Path, required=True)
    gen.add_argument("--dataflow-id")
    gen.set_defaults(func=generate)

    val = sub.add_parser("validate")
    val.add_argument("--research-dir", type=pathlib.Path, required=True)
    val.set_defaults(func=validate)

    comment = sub.add_parser("render-comment")
    comment.add_argument("--research-dir", type=pathlib.Path, required=True)
    comment.set_defaults(func=render_comment)
    return root


def main() -> int:
    args = parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
