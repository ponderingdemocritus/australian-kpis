import argparse
import json
import pathlib
import tempfile
import unittest

from tools import source_research


class SourceResearchTests(unittest.TestCase):
    def test_generate_writes_valid_research_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            report = root / "source-location-audit.json"
            register = root / "source-register.v1.toml"
            out = root / "research"

            report.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-06-29T06:00:00+00:00",
                        "register_version": "source-register.v1",
                        "status": "manual_review",
                        "findings": [
                            {
                                "source_id": "rba",
                                "dataflow_id": "rba.statistical_tables",
                                "severity": "bot_filtered",
                                "current_url": "https://www.rba.gov.au/statistics/tables/",
                                "evidence": "URL returned HTTP 403.",
                                "recommendation": "Use reviewed direct artifacts.",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            register.write_text(
                """
version = "source-register.v1"

[[dataflows]]
source_id = "rba"
dataflow_id = "rba.statistical_tables"
status = "active"
owner_area = "adapter"
canonical_url = "https://www.rba.gov.au/statistics/tables/"
license = "RBA Copyright and Disclaimer Notice"
attribution = "Source: Reserve Bank of Australia"
cadence = "weekly"
review_frequency = "weekly"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]

[dataflows.audit_policy]
kind = "bot_filtered"
expected_statuses = [403]
recommendation = "Use reviewed direct artifacts."
""",
                encoding="utf-8",
            )

            exit_code = source_research.generate(
                argparse.Namespace(
                    report=report,
                    register=register,
                    out=out,
                    dataflow_id=None,
                )
            )

            self.assertEqual(exit_code, 0)
            artifact = json.loads((out / "rba.statistical_tables.json").read_text())
            self.assertEqual(artifact["artifact_id"], "rba.statistical_tables")
            self.assertEqual(artifact["classification"], "insufficient_evidence")
            self.assertEqual(artifact["allowed_domains"], ["www.rba.gov.au"])
            self.assertEqual(artifact["source_scope"], "test")
            self.assertEqual(artifact["review_frequency"], "weekly")
            self.assertEqual(artifact["expected_missing_reason"], "")
            self.assertEqual(artifact["replacement_candidate"], "")
            self.assertEqual(artifact["generated_at"], "2026-06-29T06:00:00+00:00")
            self.assertEqual(artifact["register_version"], "source-register.v1")
            self.assertEqual(artifact["provenance_requirements"], ["Preserve source provenance."])
            self.assertEqual(artifact["validation_requirements"], ["Validate source semantics."])
            self.assertEqual(
                artifact["source_urls"],
                ["https://www.rba.gov.au/statistics/tables/"],
            )
            self.assertEqual(artifact["publisher_names"], ["Reserve Bank of Australia"])
            self.assertEqual(source_research.validate(argparse.Namespace(research_dir=out)), 0)
            self.assertIn(
                "Source Research: rba.statistical_tables",
                (out / "rba.statistical_tables.md").read_text(),
            )

    def test_generate_preserves_multiple_findings_for_one_dataflow(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            report = root / "source-location-audit.json"
            register = root / "source-register.v1.toml"
            out = root / "research"

            report.write_text(
                json.dumps(
                    {
                        "findings": [
                            {
                                "source_id": "asx",
                                "dataflow_id": "asx.market_statistics",
                                "severity": "warning",
                                "current_url": "https://www.asx.com.au/about/market-statistics/historical-market-statistics",
                                "evidence": "Market statistics page changed.",
                                "recommendation": "Review the market statistics page.",
                            },
                            {
                                "source_id": "asx",
                                "dataflow_id": "asx.market_statistics",
                                "severity": "manual_review",
                                "current_url": "https://www.asx.com.au/legals/terms-of-use",
                                "evidence": "Terms evidence changed.",
                                "recommendation": "Review ASX terms.",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            register.write_text(
                """
version = "source-register.v1"

[[dataflows]]
source_id = "asx"
dataflow_id = "asx.market_statistics"
status = "active"
owner_area = "adapter"
canonical_url = "https://www.asx.com.au/about/market-statistics/historical-market-statistics"
license = "ASX terms of use"
attribution = "Source: ASX"
cadence = "monthly"
review_frequency = "weekly"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]

[dataflows.audit_policy]
kind = "contains_any"
needles = ["market statistics"]
recommendation = "Review the market statistics page."
""",
                encoding="utf-8",
            )

            exit_code = source_research.generate(
                argparse.Namespace(
                    report=report,
                    register=register,
                    out=out,
                    dataflow_id=None,
                )
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue((out / "asx.market_statistics.json").exists())
            generated_json = sorted(
                path.name for path in out.glob("asx.market_statistics*.json")
            )
            self.assertEqual(len(generated_json), 2)
            noncanonical_name = next(
                name for name in generated_json if name != "asx.market_statistics.json"
            )
            self.assertTrue(
                noncanonical_name.startswith(
                    "asx.market_statistics__www.asx.com.au_legals_terms-of-use_manual_review__"
                )
            )
            first = json.loads((out / "asx.market_statistics.json").read_text())
            second = json.loads((out / noncanonical_name).read_text())
            self.assertEqual(
                first["current_url"],
                "https://www.asx.com.au/about/market-statistics/historical-market-statistics",
            )
            self.assertEqual(
                second["current_url"],
                "https://www.asx.com.au/legals/terms-of-use",
            )
            summary = json.loads((out / "summary.json").read_text())
            self.assertEqual(summary["artifacts_total"], 2)
            self.assertEqual(
                [item["artifact_id"] for item in summary["artifacts"]],
                [
                    "asx.market_statistics",
                    noncanonical_name.removesuffix(".json"),
                ],
            )

    def test_generate_assigns_repeated_finding_ids_from_stable_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            report = root / "source-location-audit.json"
            register = root / "source-register.v1.toml"
            out = root / "research"

            report.write_text(
                json.dumps(
                    {
                        "findings": [
                            {
                                "source_id": "asx",
                                "dataflow_id": "asx.market_statistics",
                                "severity": "manual_review",
                                "current_url": "https://www.asx.com.au/legals/terms-of-use",
                                "evidence": "Terms evidence changed.",
                                "recommendation": "Review ASX terms.",
                            },
                            {
                                "source_id": "asx",
                                "dataflow_id": "asx.market_statistics",
                                "severity": "warning",
                                "current_url": "https://www.asx.com.au/about/market-statistics/historical-market-statistics",
                                "evidence": "Market statistics page changed.",
                                "recommendation": "Review the market statistics page.",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            register.write_text(
                """
version = "source-register.v1"

[[dataflows]]
source_id = "asx"
dataflow_id = "asx.market_statistics"
status = "active"
owner_area = "adapter"
canonical_url = "https://www.asx.com.au/about/market-statistics/historical-market-statistics"
license = "ASX terms of use"
attribution = "Source: ASX"
cadence = "monthly"
review_frequency = "weekly"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]

[dataflows.audit_policy]
kind = "contains_any"
needles = ["market statistics"]
recommendation = "Review the market statistics page."
""",
                encoding="utf-8",
            )

            exit_code = source_research.generate(
                argparse.Namespace(
                    report=report,
                    register=register,
                    out=out,
                    dataflow_id=None,
                )
            )

            self.assertEqual(exit_code, 0)
            generated_json = sorted(
                path.name for path in out.glob("asx.market_statistics*.json")
            )
            noncanonical_name = next(
                name for name in generated_json if name != "asx.market_statistics.json"
            )
            first = json.loads((out / "asx.market_statistics.json").read_text())
            second = json.loads((out / noncanonical_name).read_text())
            self.assertEqual(
                first["current_url"],
                "https://www.asx.com.au/about/market-statistics/historical-market-statistics",
            )
            self.assertEqual(
                second["current_url"],
                "https://www.asx.com.au/legals/terms-of-use",
            )

            report.write_text(
                json.dumps(
                    {
                        "findings": [
                            {
                                "source_id": "asx",
                                "dataflow_id": "asx.market_statistics",
                                "severity": "manual_review",
                                "current_url": "https://www.asx.com.au/legals/terms-of-use",
                                "evidence": "Terms evidence changed.",
                                "recommendation": "Review ASX terms.",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            out_single = root / "research-single"

            exit_code = source_research.generate(
                argparse.Namespace(
                    report=report,
                    register=register,
                    out=out_single,
                    dataflow_id=None,
                )
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue((out_single / noncanonical_name).exists())
            self.assertFalse((out_single / "asx.market_statistics.json").exists())

    def test_generate_includes_additional_audit_policy_domains(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            report = root / "source-location-audit.json"
            register = root / "source-register.v1.toml"
            out = root / "research"

            api_url = "https://api.worldbank.org/v2/country/AUS/indicator/IC.BRE.BE.OS"
            report.write_text(
                json.dumps(
                    {
                        "findings": [
                            {
                                "source_id": "worldbank",
                                "dataflow_id": "worldbank.bready",
                                "severity": "manual_review",
                                "current_url": api_url,
                                "evidence": "Australia values were null.",
                                "recommendation": "Review B-READY values.",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            register.write_text(
                f"""
version = "source-register.v1"

[[dataflows]]
source_id = "worldbank"
dataflow_id = "worldbank.bready"
status = "manual_pending"
owner_area = "scorecard"
canonical_url = "https://www.worldbank.org/en/businessready"
license = "World Bank terms"
attribution = "Source: World Bank B-READY"
cadence = "annual"
review_frequency = "weekly"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]
expected_missing_reason = "Manual pending until Australia values are non-null."

[dataflows.audit_policy]
kind = "contains_any"
needles = ["Business Ready"]
recommendation = "Review public page."

[[dataflows.additional_audit_policies]]
url = "{api_url}"
kind = "world_bank_bready_api"
recommendation = "Review API values."
""",
                encoding="utf-8",
            )

            exit_code = source_research.generate(
                argparse.Namespace(
                    report=report,
                    register=register,
                    out=out,
                    dataflow_id=None,
                )
            )

            self.assertEqual(exit_code, 0)
            summary = json.loads((out / "summary.json").read_text())
            artifact_id = summary["artifacts"][0]["artifact_id"]
            artifact = json.loads((out / f"{artifact_id}.json").read_text())
            self.assertEqual(
                artifact["allowed_domains"],
                ["www.worldbank.org", "api.worldbank.org"],
            )
            self.assertEqual(
                artifact["source_urls"],
                [api_url, "https://www.worldbank.org/en/businessready"],
            )
            self.assertEqual(
                artifact["expected_missing_reason"],
                "Manual pending until Australia values are non-null.",
            )

    def test_allowed_domains_exclude_drifted_current_url_host(self):
        artifact = source_research.build_research_artifact(
            {
                "source_id": "treasury",
                "dataflow_id": "treasury.budget",
                "severity": "warning",
                "current_url": "https://mirror.example.invalid/budget-paper.pdf",
                "evidence": "Configured source URL drifted.",
                "recommendation": "Review the official budget page.",
            },
            {
                "treasury.budget": {
                    "source_id": "treasury",
                    "status": "active",
                    "canonical_url": "https://budget.gov.au/content/bp1/index.htm",
                    "attribution": "Source: Australian Treasury",
                    "source_scope": "test",
                    "review_frequency": "weekly",
                    "provenance_requirements": ["Preserve source provenance."],
                    "validation_requirements": ["Validate source semantics."],
                }
            },
            "2026-06-30T00:00:00+00:00",
        )

        self.assertEqual(artifact["allowed_domains"], ["budget.gov.au"])
        self.assertEqual(
            artifact["source_urls"],
            [
                "https://mirror.example.invalid/budget-paper.pdf",
                "https://budget.gov.au/content/bp1/index.htm",
            ],
        )

    def test_build_rejects_source_id_mismatch(self):
        with self.assertRaisesRegex(ValueError, "ambiguous provenance"):
            source_research.build_research_artifact(
                {
                    "source_id": "abs",
                    "dataflow_id": "rba.statistical_tables",
                    "severity": "warning",
                    "current_url": "https://www.rba.gov.au/statistics/tables/",
                    "evidence": "Fixture mismatch.",
                    "recommendation": "Reject ambiguous provenance.",
                },
                {
                    "rba.statistical_tables": {
                        "source_id": "rba",
                        "status": "active",
                        "canonical_url": "https://www.rba.gov.au/statistics/tables/",
                        "attribution": "Source: Reserve Bank of Australia",
                        "source_scope": "test",
                        "review_frequency": "weekly",
                    }
                },
                "2026-06-30T00:00:00+00:00",
            )

    def test_generate_keeps_actionable_packets_for_mixed_error_audits(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            report = root / "source-location-audit.json"
            register = root / "source-register.v1.toml"
            out = root / "research"

            report.write_text(
                json.dumps(
                    {
                        "status": "error",
                        "findings": [
                            {
                                "source_id": "abs",
                                "dataflow_id": "abs.cpi",
                                "severity": "error",
                                "current_url": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI?detail=allstubs",
                                "evidence": "One audit request failed before an HTTP response.",
                                "recommendation": "Retry the source-location audit.",
                            },
                            {
                                "source_id": "abs",
                                "dataflow_id": "abs.cpi",
                                "severity": "warning",
                                "current_url": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI?detail=allstubs",
                                "evidence": "Another checked source moved.",
                                "recommendation": "Review ABS CPI source.",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            register.write_text(
                """
version = "source-register.v1"

[[dataflows]]
source_id = "abs"
dataflow_id = "abs.cpi"
status = "active"
owner_area = "adapter"
canonical_url = "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI?detail=allstubs"
license = "CC-BY-4.0"
attribution = "Source: Australian Bureau of Statistics"
cadence = "quarterly"
review_frequency = "weekly"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]

[dataflows.audit_policy]
kind = "contains_any"
needles = ["CPI"]
recommendation = "Review ABS CPI source."
""",
                encoding="utf-8",
            )

            exit_code = source_research.generate(
                argparse.Namespace(
                    report=report,
                    register=register,
                    out=out,
                    dataflow_id=None,
                )
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue((out / "abs.cpi.json").exists())
            artifact = json.loads((out / "abs.cpi.json").read_text())
            self.assertEqual(artifact["audit_severity"], "warning")
            summary = json.loads((out / "summary.json").read_text())
            self.assertEqual(summary["artifacts_total"], 1)
            self.assertEqual(summary["audit_status"], "error")
            self.assertEqual(summary["dataflow_ids"], ["abs.cpi"])
            self.assertEqual(source_research.validate(argparse.Namespace(research_dir=out)), 0)

    def test_validation_rejects_non_actionable_research_artifact(self):
        artifact = {
            "schema_version": "source-research.v1",
            "artifact_id": "abs.cpi",
            "source_id": "abs",
            "dataflow_id": "abs.cpi",
            "current_url": "https://data.api.abs.gov.au/",
            "audit_evidence": "ok",
            "audit_severity": "info",
            "register_status": "active",
            "register_canonical_url": "https://data.api.abs.gov.au/",
            "source_scope": "test",
            "review_frequency": "weekly",
            "expected_missing_reason": "",
            "replacement_candidate": "",
            "allowed_domains": ["data.api.abs.gov.au"],
            "required_evidence": ["official publisher URL"],
            "provenance_requirements": ["Preserve source provenance."],
            "validation_requirements": ["Validate source semantics."],
            "classification": "same_source",
            "source_urls": ["https://data.api.abs.gov.au/"],
            "publisher_names": ["ABS"],
            "retrieved_at": "2026-06-30T00:00:00+00:00",
            "generated_at": "2026-06-29T06:00:00+00:00",
            "register_version": "source-register.v1",
            "license_evidence": "CC-BY-4.0",
            "attribution_evidence": "Source: ABS",
            "cadence_evidence": "quarterly",
            "recommendation": "No action.",
            "risk_notes": [],
        }

        errors = source_research.validate_research_artifact(artifact)

        self.assertIn("audit_severity is not actionable for research", errors)

    def test_validation_rejects_register_domain_provenance_mismatch(self):
        artifact = {
            "schema_version": "source-research.v1",
            "artifact_id": "abs.cpi",
            "source_id": "abs",
            "dataflow_id": "abs.cpi",
            "current_url": "https://mirror.example.invalid/cpi",
            "audit_evidence": "ok",
            "audit_severity": "warning",
            "register_status": "active",
            "register_canonical_url": "https://data.api.abs.gov.au/",
            "source_scope": "test",
            "review_frequency": "weekly",
            "expected_missing_reason": "",
            "replacement_candidate": "",
            "allowed_domains": ["example.org"],
            "required_evidence": ["official publisher URL"],
            "provenance_requirements": ["Preserve source provenance."],
            "validation_requirements": ["Validate source semantics."],
            "classification": "insufficient_evidence",
            "source_urls": [
                "https://mirror.example.invalid/cpi",
                "https://data.api.abs.gov.au/",
                "https://unreviewed.example.net/cpi",
            ],
            "publisher_names": ["ABS"],
            "retrieved_at": "2026-06-30T00:00:00+00:00",
            "generated_at": "2026-06-29T06:00:00+00:00",
            "register_version": "source-register.v0",
            "license_evidence": "CC-BY-4.0",
            "attribution_evidence": "Source: ABS",
            "cadence_evidence": "quarterly",
            "recommendation": "Review source.",
            "risk_notes": ["Needs human review."],
        }

        errors = source_research.validate_research_artifact(artifact)

        self.assertIn("register_version must be source-register.v1", errors)
        self.assertIn("allowed_domains must include register_canonical_url host", errors)
        self.assertIn(
            "source_urls host `unreviewed.example.net` must be current_url host or allowed",
            errors,
        )

    def test_validation_accepts_drifted_current_url_plus_governed_register_url(self):
        artifact = {
            "schema_version": "source-research.v1",
            "artifact_id": "abs.cpi",
            "source_id": "abs",
            "dataflow_id": "abs.cpi",
            "current_url": "https://mirror.example.invalid/cpi",
            "audit_evidence": "ok",
            "audit_severity": "warning",
            "register_status": "active",
            "register_canonical_url": "https://data.api.abs.gov.au/",
            "source_scope": "test",
            "review_frequency": "weekly",
            "expected_missing_reason": "",
            "replacement_candidate": "",
            "allowed_domains": ["data.api.abs.gov.au"],
            "required_evidence": ["official publisher URL"],
            "provenance_requirements": ["Preserve source provenance."],
            "validation_requirements": ["Validate source semantics."],
            "classification": "insufficient_evidence",
            "source_urls": [
                "https://mirror.example.invalid/cpi",
                "https://data.api.abs.gov.au/",
            ],
            "publisher_names": ["ABS"],
            "retrieved_at": "2026-06-30T00:00:00+00:00",
            "generated_at": "2026-06-29T06:00:00+00:00",
            "register_version": "source-register.v1",
            "license_evidence": "CC-BY-4.0",
            "attribution_evidence": "Source: ABS",
            "cadence_evidence": "quarterly",
            "recommendation": "Review source.",
            "risk_notes": ["Needs human review."],
        }

        errors = source_research.validate_research_artifact(artifact)

        self.assertEqual(errors, [])

    def test_load_register_rejects_duplicate_dataflow_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            register = pathlib.Path(tmp) / "source-register.v1.toml"
            register.write_text(
                """
version = "source-register.v1"

[[dataflows]]
source_id = "abs"
dataflow_id = "abs.cpi"
status = "active"
owner_area = "adapter"
canonical_url = "https://example.test/a"
license = "CC-BY-4.0"
attribution = "Source: ABS"
cadence = "quarterly"
review_frequency = "weekly"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]

[dataflows.audit_policy]
kind = "contains_any"
needles = ["CPI"]
recommendation = "Review source."

[[dataflows]]
source_id = "abs"
dataflow_id = "abs.cpi"
status = "active"
owner_area = "adapter"
canonical_url = "https://example.test/b"
license = "CC-BY-4.0"
attribution = "Source: ABS"
cadence = "quarterly"
review_frequency = "weekly"
source_scope = "test"
provenance_requirements = ["Preserve source provenance."]
validation_requirements = ["Validate source semantics."]

[dataflows.audit_policy]
kind = "contains_any"
needles = ["CPI"]
recommendation = "Review source."
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "duplicate dataflow id `abs.cpi`"):
                source_research.load_register(register)

    def test_validation_rejects_missing_artifact_id_and_bad_timestamp(self):
        artifact = {
            "schema_version": "source-research.v1",
            "source_id": "abs",
            "dataflow_id": "abs.cpi",
            "current_url": "https://data.api.abs.gov.au/",
            "audit_evidence": "ok",
            "audit_severity": "warning",
            "register_status": "active",
            "register_canonical_url": "https://data.api.abs.gov.au/",
            "source_scope": "test",
            "review_frequency": "weekly",
            "expected_missing_reason": "",
            "replacement_candidate": "",
            "allowed_domains": ["data.api.abs.gov.au"],
            "required_evidence": ["official publisher URL"],
            "provenance_requirements": ["Preserve source provenance."],
            "validation_requirements": ["Validate source semantics."],
            "classification": "insufficient_evidence",
            "source_urls": ["https://data.api.abs.gov.au/"],
            "publisher_names": ["ABS"],
            "retrieved_at": "2026/06/30",
            "generated_at": "2026/06/29",
            "register_version": "source-register.v1",
            "license_evidence": "CC-BY-4.0",
            "attribution_evidence": "Source: ABS",
            "cadence_evidence": "quarterly",
            "recommendation": "Review source.",
            "risk_notes": ["Needs human review."],
        }

        errors = source_research.validate_research_artifact(artifact)

        self.assertIn("artifact_id must be a non-empty string", errors)
        self.assertIn("retrieved_at must be an RFC 3339 timestamp", errors)
        self.assertIn("generated_at must be an RFC 3339 timestamp", errors)


if __name__ == "__main__":
    unittest.main()
