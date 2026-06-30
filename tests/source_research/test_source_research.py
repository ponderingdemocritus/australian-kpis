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
canonical_url = "https://www.rba.gov.au/statistics/tables/"
license = "RBA Copyright and Disclaimer Notice"
attribution = "Source: Reserve Bank of Australia"
cadence = "weekly"
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
                                "severity": "drift",
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
canonical_url = "https://www.asx.com.au/about/market-statistics/historical-market-statistics"
license = "ASX terms of use"
attribution = "Source: ASX"
cadence = "monthly"
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
            self.assertTrue((out / "asx.market_statistics__2.json").exists())
            first = json.loads((out / "asx.market_statistics.json").read_text())
            second = json.loads((out / "asx.market_statistics__2.json").read_text())
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
                ["asx.market_statistics", "asx.market_statistics__2"],
            )

    def test_generate_skips_audit_error_findings(self):
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
                                "evidence": "No HTTP snapshot was available.",
                                "recommendation": "Retry the source-location audit.",
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
canonical_url = "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI?detail=allstubs"
license = "CC-BY-4.0"
attribution = "Source: Australian Bureau of Statistics"
cadence = "quarterly"
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
            self.assertFalse((out / "abs.cpi.json").exists())
            summary = json.loads((out / "summary.json").read_text())
            self.assertEqual(summary["artifacts_total"], 0)
            self.assertEqual(summary["dataflow_ids"], [])
            self.assertEqual(source_research.validate(argparse.Namespace(research_dir=out)), 0)

    def test_validation_rejects_non_actionable_research_artifact(self):
        artifact = {
            "schema_version": "source-research.v1",
            "source_id": "abs",
            "dataflow_id": "abs.cpi",
            "current_url": "https://data.api.abs.gov.au/",
            "audit_evidence": "ok",
            "audit_severity": "info",
            "register_status": "active",
            "register_canonical_url": "https://data.api.abs.gov.au/",
            "allowed_domains": ["data.api.abs.gov.au"],
            "required_evidence": ["official publisher URL"],
            "classification": "same_source",
            "source_urls": [],
            "publisher_names": [],
            "retrieved_at": "2026-06-30T00:00:00+00:00",
            "license_evidence": "CC-BY-4.0",
            "attribution_evidence": "Source: ABS",
            "cadence_evidence": "quarterly",
            "recommendation": "No action.",
            "risk_notes": [],
        }

        errors = source_research.validate_research_artifact(artifact)

        self.assertIn("audit_severity is not actionable for research", errors)


if __name__ == "__main__":
    unittest.main()
