#!/usr/bin/env python3
"""Generate and validate source-research artifacts from audit findings."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import pathlib
import re
import sys
import tomllib
import urllib.parse
from collections.abc import Callable
from typing import Any, cast

ACTIONABLE_STATUSES = {"manual_review", "bot_filtered", "warning"}
CLASSIFICATIONS = {
    "same_source",
    "moved",
    "bot_filtered",
    "source_retired",
    "candidate_replacement",
    "insufficient_evidence",
}
SOURCE_REGISTER_VERSION = "source-register.v1"
EVIDENCE_CHECKLIST = [
    "official publisher URL",
    "license or usage terms",
    "attribution text",
    "cadence or release timing",
    "source/dataflow scope match",
]
REGISTER_TOP_LEVEL_KEYS = {"version", "dataflows"}
REGISTER_DATAFLOW_KEYS = {
    "source_id",
    "dataflow_id",
    "status",
    "owner_area",
    "canonical_url",
    "license",
    "attribution",
    "cadence",
    "review_frequency",
    "source_scope",
    "provenance_requirements",
    "validation_requirements",
    "expected_missing_reason",
    "retrieved_at",
    "reviewed_by",
    "reviewed_at",
    "manual_review_due_at",
    "replacement_candidate",
    "audit_policy",
    "additional_audit_policies",
}
REGISTER_REQUIRED_STRINGS = [
    "source_id",
    "dataflow_id",
    "status",
    "owner_area",
    "canonical_url",
    "license",
    "attribution",
    "cadence",
    "review_frequency",
    "source_scope",
]
REGISTER_POLICY_KEYS = {
    "contains_any": {"kind", "needles", "recommendation"},
    "directory_listing": {"kind", "required_patterns", "recommendation"},
    "budget_year": {"kind", "configured_year", "latest_year", "recommendation"},
    "licensed_product": {"kind", "recommendation"},
    "world_bank_bready_api": {"kind", "recommendation"},
    "manual_placeholder": {"kind", "reason", "recommendation"},
    "manual_register_only": {"kind", "reason", "recommendation"},
    "bot_filtered": {"kind", "expected_statuses", "semantic_fallback", "recommendation"},
}


def load_json(path: pathlib.Path) -> dict[str, Any]:
    raw: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return cast(dict[str, Any], raw)


def load_register(path: pathlib.Path) -> dict[str, dict[str, Any]]:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    errors = validate_register(raw)
    if errors:
        raise ValueError(f"{path}: {'; '.join(errors)}")

    register: dict[str, dict[str, Any]] = {}
    for dataflow in raw["dataflows"]:
        dataflow_id = cast(str, dataflow["dataflow_id"])
        register[dataflow_id] = cast(dict[str, Any], dataflow)
    return register


def validate_register(raw: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    version = raw.get("version")
    if version != SOURCE_REGISTER_VERSION:
        errors.append(f"version must be {SOURCE_REGISTER_VERSION}")

    dataflows = raw.get("dataflows")
    if not isinstance(dataflows, list) or not dataflows:
        errors.append("dataflows must be a non-empty list")
        return errors

    seen: set[str] = set()
    for index, dataflow in enumerate(dataflows):
        prefix = f"dataflows[{index}]"
        if not isinstance(dataflow, dict):
            errors.append(f"{prefix} must be a table")
            continue
        for field in REGISTER_REQUIRED_STRINGS:
            require_non_empty_string(dataflow, field, prefix, errors)
        require_non_empty_string_list(dataflow, "provenance_requirements", prefix, errors)
        require_non_empty_string_list(dataflow, "validation_requirements", prefix, errors)

        dataflow_id = dataflow.get("dataflow_id")
        if isinstance(dataflow_id, str) and dataflow_id.strip():
            if dataflow_id in seen:
                errors.append(f"duplicate dataflow id `{dataflow_id}`")
            seen.add(dataflow_id)

        audit_policy = dataflow.get("audit_policy")
        if isinstance(audit_policy, dict):
            validate_audit_policy(audit_policy, f"{prefix}.audit_policy", errors)
        else:
            errors.append(f"{prefix}.audit_policy must be a table")

        additional_policies = dataflow.get("additional_audit_policies", [])
        if not isinstance(additional_policies, list):
            errors.append(f"{prefix}.additional_audit_policies must be a list")
        else:
            for policy_index, policy in enumerate(additional_policies):
                policy_prefix = f"{prefix}.additional_audit_policies[{policy_index}]"
                if not isinstance(policy, dict):
                    errors.append(f"{policy_prefix} must be a table")
                    continue
                require_non_empty_string(policy, "url", policy_prefix, errors)
                policy_payload = dict(policy)
                policy_payload.pop("url", None)
                validate_audit_policy(policy_payload, policy_prefix, errors)
    return errors


def require_non_empty_string(
    record: dict[str, Any],
    field: str,
    prefix: str,
    errors: list[str],
) -> None:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{prefix}.{field} must be a non-empty string")


def require_non_empty_string_list(
    record: dict[str, Any],
    field: str,
    prefix: str,
    errors: list[str],
) -> None:
    value = record.get(field)
    if not isinstance(value, list) or not value or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        errors.append(f"{prefix}.{field} must be a non-empty string list")


def validate_audit_policy(policy: dict[str, Any], prefix: str, errors: list[str]) -> None:
    kind = policy.get("kind")
    if not isinstance(kind, str) or kind not in REGISTER_POLICY_KEYS:
        errors.append(f"{prefix}.kind is not allowed")
        return
    allowed_keys = REGISTER_POLICY_KEYS[kind]
    unknown_keys = set(policy) - allowed_keys
    if unknown_keys:
        errors.append(f"{prefix} has unknown keys: {', '.join(sorted(unknown_keys))}")
    require_non_empty_string(policy, "recommendation", prefix, errors)
    if kind == "contains_any":
        require_non_empty_string_list(policy, "needles", prefix, errors)
    elif kind == "directory_listing":
        require_non_empty_string_list(policy, "required_patterns", prefix, errors)
    elif kind == "budget_year":
        require_non_empty_string(policy, "configured_year", prefix, errors)
        require_non_empty_string(policy, "latest_year", prefix, errors)
    elif kind in {"manual_placeholder", "manual_register_only"}:
        require_non_empty_string(policy, "reason", prefix, errors)
    elif kind == "bot_filtered":
        statuses = policy.get("expected_statuses")
        if not isinstance(statuses, list) or not statuses or any(
            not isinstance(status, int) for status in statuses
        ):
            errors.append(f"{prefix}.expected_statuses must be a non-empty integer list")
        semantic_fallback = policy.get("semantic_fallback")
        if semantic_fallback is not None and (
            not isinstance(semantic_fallback, str) or not semantic_fallback.strip()
        ):
            errors.append(f"{prefix}.semantic_fallback must be a non-empty string")


def slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def finding_identity_suffix(finding: dict[str, Any]) -> str:
    current_url = str(finding.get("current_url", ""))
    parsed = urllib.parse.urlparse(current_url)
    url_identity = "_".join(
        part
        for part in [
            parsed.netloc,
            parsed.path.strip("/").replace("/", "_"),
            str(finding.get("severity", "")),
        ]
        if part
    )
    stable_input = "|".join(
        [
            str(finding.get("source_id", "")),
            str(finding.get("dataflow_id", "")),
            current_url,
            str(finding.get("severity", "")),
        ]
    )
    digest = hashlib.sha1(stable_input.encode("utf-8")).hexdigest()[:8]
    identity = slug(url_identity)[:80] or "finding"
    return f"{identity}__{digest}"


def artifact_id_for_finding(
    artifact: dict[str, Any],
    finding: dict[str, Any],
    used_ids: set[str],
) -> str:
    base = slug(str(artifact["dataflow_id"]))
    if artifact["current_url"] == artifact["register_canonical_url"] and base not in used_ids:
        return base
    return f"{base}__{finding_identity_suffix(finding)}"


def unique_non_empty(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        cleaned = value.strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            unique.append(cleaned)
    return unique


def publisher_name_from_attribution(attribution: str, source_id: str) -> str:
    cleaned = attribution.strip()
    for prefix in ("Source: ", "Sources: "):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :].strip()
            break
    return cleaned or source_id


def string_list_field(record: dict[str, Any], field: str) -> list[str]:
    value = record.get(field, [])
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def additional_audit_urls(record: dict[str, Any]) -> list[str]:
    policies = record.get("additional_audit_policies", [])
    if not isinstance(policies, list):
        return []
    urls: list[str] = []
    for policy in policies:
        if isinstance(policy, dict) and isinstance(policy.get("url"), str):
            urls.append(cast(str, policy["url"]))
    return urls


def host_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return ""
    return parsed.netloc


def now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


def is_rfc3339_timestamp(value: str) -> bool:
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def should_research(finding: dict[str, Any], dataflow_id: str | None) -> bool:
    if dataflow_id and finding.get("dataflow_id") != dataflow_id:
        return False
    severity = str(finding.get("severity", ""))
    return severity in ACTIONABLE_STATUSES


def finding_sort_key(finding: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(finding.get("dataflow_id", "")),
        str(finding.get("source_id", "")),
        str(finding.get("current_url", "")),
        str(finding.get("severity", "")),
        str(finding.get("evidence", "")),
    )


def build_research_artifact(
    finding: dict[str, Any],
    register: dict[str, dict[str, Any]],
    retrieved_at: str,
    audit_generated_at: str | None = None,
    register_version: str = "unknown",
) -> dict[str, Any]:
    dataflow_id = str(finding["dataflow_id"])
    register_record = register.get(dataflow_id)
    if register_record is None:
        raise ValueError(f"missing register record for {dataflow_id}")
    if finding["source_id"] != register_record.get("source_id"):
        raise ValueError(
            f"ambiguous provenance for {dataflow_id}: audit source_id "
            f"{finding['source_id']!r} does not match register source_id "
            f"{register_record.get('source_id')!r}"
        )

    canonical_url = str(register_record.get("canonical_url", ""))
    current_url = str(finding["current_url"])
    governed_urls = unique_non_empty([canonical_url, *additional_audit_urls(register_record)])
    allowed_domains = unique_non_empty([host_from_url(url) for url in governed_urls])
    source_urls = unique_non_empty([current_url, *governed_urls])
    publisher_names = [
        publisher_name_from_attribution(str(register_record.get("attribution", "")), str(finding["source_id"]))
    ]

    return {
        "schema_version": "source-research.v1",
        "artifact_id": "",
        "source_id": finding["source_id"],
        "dataflow_id": dataflow_id,
        "current_url": current_url,
        "audit_evidence": finding["evidence"],
        "audit_severity": finding["severity"],
        "register_status": register_record["status"],
        "register_canonical_url": canonical_url,
        "source_scope": str(register_record.get("source_scope", "")),
        "review_frequency": str(register_record.get("review_frequency", "")),
        "expected_missing_reason": str(register_record.get("expected_missing_reason", "") or ""),
        "replacement_candidate": str(register_record.get("replacement_candidate", "") or ""),
        "allowed_domains": allowed_domains,
        "required_evidence": EVIDENCE_CHECKLIST,
        "provenance_requirements": string_list_field(register_record, "provenance_requirements"),
        "validation_requirements": string_list_field(register_record, "validation_requirements"),
        "classification": "insufficient_evidence",
        "source_urls": source_urls,
        "publisher_names": publisher_names,
        "retrieved_at": retrieved_at,
        "generated_at": audit_generated_at or retrieved_at,
        "register_version": register_version or "unknown",
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
        "artifact_id",
        "source_id",
        "dataflow_id",
        "current_url",
        "audit_evidence",
        "audit_severity",
        "register_status",
        "register_canonical_url",
        "source_scope",
        "review_frequency",
        "classification",
        "retrieved_at",
        "generated_at",
        "register_version",
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
    register_version = artifact.get("register_version")
    if register_version != SOURCE_REGISTER_VERSION:
        errors.append(f"register_version must be {SOURCE_REGISTER_VERSION}")
    if artifact.get("classification") not in CLASSIFICATIONS:
        errors.append("classification is not allowed")
    for field in [
        "allowed_domains",
        "required_evidence",
        "provenance_requirements",
        "validation_requirements",
        "source_urls",
        "publisher_names",
        "risk_notes",
    ]:
        value = artifact.get(field)
        if not isinstance(value, list):
            errors.append(f"{field} must be a list")
        elif any(not isinstance(item, str) or not item.strip() for item in value):
            errors.append(f"{field} must contain only non-empty strings")
    for field in [
        "allowed_domains",
        "required_evidence",
        "provenance_requirements",
        "validation_requirements",
        "source_urls",
        "publisher_names",
        "risk_notes",
    ]:
        if not artifact.get(field):
            errors.append(f"{field} must not be empty")
    for field in ["expected_missing_reason", "replacement_candidate"]:
        value = artifact.get(field)
        if value is not None and not isinstance(value, str):
            errors.append(f"{field} must be a string")
    retrieved_at = artifact.get("retrieved_at")
    if isinstance(retrieved_at, str) and not is_rfc3339_timestamp(retrieved_at):
        errors.append("retrieved_at must be an RFC 3339 timestamp")
    generated_at = artifact.get("generated_at")
    if isinstance(generated_at, str) and not is_rfc3339_timestamp(generated_at):
        errors.append("generated_at must be an RFC 3339 timestamp")
    if artifact.get("audit_severity") not in ACTIONABLE_STATUSES:
        errors.append("audit_severity is not actionable for research")
    current_url = artifact.get("current_url")
    register_canonical_url = artifact.get("register_canonical_url")
    allowed_domains = artifact.get("allowed_domains")
    source_urls = artifact.get("source_urls")
    if isinstance(register_canonical_url, str):
        canonical_host = host_from_url(register_canonical_url)
        if not canonical_host:
            errors.append("register_canonical_url must be an http(s) URL")
        elif isinstance(allowed_domains, list) and canonical_host not in allowed_domains:
            errors.append("allowed_domains must include register_canonical_url host")
    if isinstance(current_url, str):
        current_host = host_from_url(current_url)
        if not current_host:
            errors.append("current_url must be an http(s) URL")
    else:
        current_host = ""
    if (
        isinstance(current_url, str)
        and isinstance(register_canonical_url, str)
        and isinstance(source_urls, list)
    ):
        if current_url not in source_urls:
            errors.append("source_urls must include current_url")
        if register_canonical_url not in source_urls:
            errors.append("source_urls must include register_canonical_url")
    if isinstance(source_urls, list) and isinstance(allowed_domains, list):
        allowed_domain_values = {
            domain for domain in allowed_domains if isinstance(domain, str)
        }
        for source_url in source_urls:
            if not isinstance(source_url, str):
                continue
            source_host = host_from_url(source_url)
            if not source_host:
                errors.append(f"source_urls entry `{source_url}` must be an http(s) URL")
            elif source_host != current_host and source_host not in allowed_domain_values:
                errors.append(
                    f"source_urls host `{source_host}` must be current_url host or allowed"
                )
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
        f"- Audit generated at: `{artifact['generated_at']}`",
        f"- Source register: `{artifact['register_version']}`",
        f"- Current URL: {artifact['current_url']}",
        f"- Register URL: {artifact['register_canonical_url']}",
        f"- Source scope: `{artifact['source_scope']}`",
        f"- Review frequency: `{artifact['review_frequency']}`",
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
            "## Register Context",
            "",
            f"- Expected missing reason: {artifact['expected_missing_reason'] or 'n/a'}",
            f"- Replacement candidate: {artifact['replacement_candidate'] or 'n/a'}",
            "",
            "### Provenance Requirements",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in artifact["provenance_requirements"])
    lines.extend(["", "### Validation Requirements", ""])
    lines.extend(f"- {item}" for item in artifact["validation_requirements"])
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
    used_artifact_ids: set[str] = set()
    audit_generated_at_value = report.get("generated_at")
    if not isinstance(audit_generated_at_value, str) or not audit_generated_at_value.strip():
        raise ValueError("source-location audit report must include generated_at")
    if not is_rfc3339_timestamp(audit_generated_at_value):
        raise ValueError("source-location audit report generated_at must be RFC 3339")
    register_version_value = report.get("register_version")
    if register_version_value != SOURCE_REGISTER_VERSION:
        raise ValueError(
            f"source-location audit report register_version must be {SOURCE_REGISTER_VERSION}"
        )
    audit_generated_at = audit_generated_at_value
    register_version = register_version_value

    findings = [
        finding
        for finding in report.get("findings", [])
        if isinstance(finding, dict) and should_research(finding, args.dataflow_id)
    ]
    for finding in sorted(findings, key=finding_sort_key):
        artifact = build_research_artifact(
            finding,
            register,
            retrieved_at,
            audit_generated_at,
            register_version,
        )
        artifact["artifact_id"] = artifact_id_for_finding(artifact, finding, used_artifact_ids)
        used_artifact_ids.add(str(artifact["artifact_id"]))
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
        "audit_generated_at": audit_generated_at,
        "audit_status": str(report.get("status") or "unknown"),
        "register_version": register_version,
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
    func = cast(Callable[[argparse.Namespace], int], args.func)
    return func(args)


if __name__ == "__main__":
    raise SystemExit(main())
