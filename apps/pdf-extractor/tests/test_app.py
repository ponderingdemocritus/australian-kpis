from __future__ import annotations

import shutil
from collections.abc import Sequence
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from au_kpis_pdf_extractor.app import _configured_max_pages, create_app
from au_kpis_pdf_extractor.extractors import DeterministicExtractor
from au_kpis_pdf_extractor.models import (
    BackendInfo,
    ExtractionBackendKind,
    ExtractionResponse,
)
from au_kpis_pdf_extractor.runtime import configured_port
from au_kpis_pdf_extractor.storage import ObjectNotFound

FIXTURE = Path(__file__).parent / "fixtures" / "bp4_agency_resourcing_tables_2026_27.pdf"


class FixtureStorage:
    def __init__(self) -> None:
        self.keys: list[str] = []

    def fetch_to_path(self, s3_key: str, destination: Path) -> Path:
        self.keys.append(s3_key)
        shutil.copyfile(FIXTURE, destination)
        return destination


class MissingStorage:
    def fetch_to_path(self, s3_key: str, destination: Path) -> Path:
        raise ObjectNotFound(s3_key)


class RecordingExtractor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[int, ...]]] = []

    def extract(
        self,
        pdf_path: Path,
        artifact_key: str,
        pages: Sequence[int] | None = None,
    ) -> ExtractionResponse:
        assert pdf_path.exists()
        self.calls.append((artifact_key, tuple(pages or ())))
        return ExtractionResponse(
            artifact_key=artifact_key,
            backend=BackendInfo(
                kind=ExtractionBackendKind.DETERMINISTIC,
                name="recording",
                version="test",
                model_sha256=None,
            ),
            tables=[],
        )


def test_health_endpoint() -> None:
    app = create_app(storage_client=FixtureStorage(), extractor=DeterministicExtractor(max_pages=1))
    response = TestClient(app).get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_configured_max_pages_reads_optional_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AU_KPIS_PDF_EXTRACTOR__MAX_PAGES", raising=False)
    assert _configured_max_pages() is None

    monkeypatch.setenv("AU_KPIS_PDF_EXTRACTOR__MAX_PAGES", "2")
    assert _configured_max_pages() == 2

    monkeypatch.setenv("AU_KPIS_PDF_EXTRACTOR__MAX_PAGES", "")
    assert _configured_max_pages() is None


def test_configured_port_reads_railway_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PORT", raising=False)
    assert configured_port() == 8000

    monkeypatch.setenv("PORT", "9123")
    assert configured_port() == 9123

    monkeypatch.setenv("PORT", "not-a-port")
    with pytest.raises(ValueError, match="PORT must be a positive integer"):
        configured_port()

    monkeypatch.setenv("PORT", "70000")
    with pytest.raises(ValueError, match="PORT must be between 1 and 65535"):
        configured_port()


def test_extract_fetches_s3_key_and_returns_real_budget_tables() -> None:
    storage = FixtureStorage()
    app = create_app(storage_client=storage, extractor=DeterministicExtractor(max_pages=2))

    response = TestClient(app).post(
        "/extract",
        json={
            "s3_key": "artifacts/fixtures/bp4-agency-resourcing.pdf",
            "source_id": "treasury",
            "artifact_date": "2026-05-12",
            "strategy": "deterministic",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert storage.keys == ["artifacts/fixtures/bp4-agency-resourcing.pdf"]
    assert body["artifact_key"] == "artifacts/fixtures/bp4-agency-resourcing.pdf"
    assert body["backend"]["kind"] == "deterministic"
    assert body["backend"]["name"] == "pdfplumber+camelot"
    assert body["backend"]["model_sha256"] is None
    assert body["tables"]

    first_table = body["tables"][0]
    assert first_table["page"] >= 1
    assert len(first_table["bbox"]) == 4
    assert first_table["cells"]
    flattened = " ".join(cell for row in first_table["cells"] for cell in row)
    assert "Department" in flattened or "Agency Resourcing" in flattened
    assert first_table["diagnostics"]["source_backend"] in {"pdfplumber", "camelot"}


def test_extract_forwards_requested_pages_to_extractor() -> None:
    extractor = RecordingExtractor()
    app = create_app(storage_client=FixtureStorage(), extractor=extractor)

    response = TestClient(app).post(
        "/extract",
        json={
            "s3_key": "artifacts/fixtures/bp4-agency-resourcing.pdf",
            "source_id": "treasury",
            "pages": [12, 13],
        },
    )

    assert response.status_code == 200
    assert extractor.calls == [("artifacts/fixtures/bp4-agency-resourcing.pdf", (12, 13))]


def test_extract_rejects_invalid_page_numbers() -> None:
    app = create_app(storage_client=FixtureStorage(), extractor=RecordingExtractor())

    response = TestClient(app).post(
        "/extract",
        json={"s3_key": "artifacts/fixtures/report.pdf", "source_id": "treasury", "pages": [0]},
    )

    assert response.status_code == 422


def test_extract_maps_missing_s3_object_to_404() -> None:
    app = create_app(storage_client=MissingStorage(), extractor=DeterministicExtractor(max_pages=1))

    response = TestClient(app).post(
        "/extract",
        json={"s3_key": "artifacts/missing.pdf", "source_id": "treasury"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "S3 object not found: artifacts/missing.pdf"
