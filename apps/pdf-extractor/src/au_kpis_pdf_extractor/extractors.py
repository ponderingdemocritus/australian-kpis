from __future__ import annotations

import importlib.metadata
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pdfplumber

from au_kpis_pdf_extractor.models import (
    BackendInfo,
    ExtractionBackendKind,
    ExtractionResponse,
    TableCandidate,
)

LOGGER = logging.getLogger(__name__)


class ExtractionError(Exception):
    pass


class DeterministicExtractor:
    def __init__(self, max_pages: int | None = None) -> None:
        self.max_pages = max_pages

    def extract(
        self,
        pdf_path: Path,
        artifact_key: str,
        pages: Sequence[int] | None = None,
    ) -> ExtractionResponse:
        page_numbers = self._page_numbers(pages)
        tables = self._camelot_tables(pdf_path, page_numbers)
        if not tables:
            tables = self._pdfplumber_tables(pdf_path, page_numbers)

        return ExtractionResponse(
            artifact_key=artifact_key,
            backend=BackendInfo(
                kind=ExtractionBackendKind.DETERMINISTIC,
                name="pdfplumber+camelot",
                version=(
                    f"pdfplumber/{_package_version('pdfplumber')} "
                    f"camelot/{_package_version('camelot-py')}"
                ),
                model_sha256=None,
            ),
            tables=tables,
        )

    def _page_numbers(self, pages: Sequence[int] | None) -> tuple[int, ...] | None:
        if pages is not None:
            return tuple(dict.fromkeys(pages))
        if self.max_pages is None:
            return None
        return tuple(range(1, self.max_pages + 1))

    def _pdfplumber_tables(
        self,
        pdf_path: Path,
        page_numbers: tuple[int, ...] | None,
    ) -> list[TableCandidate]:
        tables: list[TableCandidate] = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if page_numbers is None:
                    selected_pages = pdf.pages
                else:
                    selected_pages = [
                        pdf.pages[page_number - 1]
                        for page_number in page_numbers
                        if 1 <= page_number <= len(pdf.pages)
                    ]
                for page in selected_pages:
                    for table_index, table in enumerate(page.find_tables()):
                        cells = _normalize_cells(table.extract())
                        if not cells:
                            continue
                        bbox = _bbox(table.bbox)
                        tables.append(
                            TableCandidate(
                                page=page.page_number,
                                bbox=bbox,
                                cells=cells,
                                diagnostics={
                                    "source_backend": "pdfplumber",
                                    "table_index": table_index,
                                    "row_count": len(cells),
                                    "column_count": max(len(row) for row in cells),
                                },
                            )
                        )
        except Exception as err:  # pragma: no cover - exercised through API error mapping
            raise ExtractionError(f"pdfplumber extraction failed: {err}") from err
        return tables

    def _camelot_tables(
        self,
        pdf_path: Path,
        page_numbers: tuple[int, ...] | None,
    ) -> list[TableCandidate]:
        try:
            import camelot  # type: ignore[import-untyped]
        except Exception as err:
            LOGGER.info("camelot unavailable; falling back to pdfplumber", exc_info=err)
            return []

        if page_numbers == ():
            return []
        pages = "all" if page_numbers is None else ",".join(str(page) for page in page_numbers)
        candidates: list[TableCandidate] = []
        errors: list[str] = []
        for flavor in ("lattice", "stream"):
            try:
                parsed = camelot.read_pdf(str(pdf_path), pages=pages, flavor=flavor)
            except Exception as err:
                errors.append(f"{flavor}: {err}")
                continue

            for table_index, table in enumerate(parsed):
                raw_cells = table.df.astype(str).values.tolist()
                cells = _normalize_cells(raw_cells)
                if not cells:
                    continue
                bbox = _bbox(getattr(table, "_bbox", (0.0, 0.0, 0.0, 0.0)))
                report = getattr(table, "parsing_report", {}) or {}
                candidates.append(
                    TableCandidate(
                        page=int(getattr(table, "page", 1)),
                        bbox=bbox,
                        cells=cells,
                        diagnostics={
                            "source_backend": "camelot",
                            "flavor": flavor,
                            "table_index": table_index,
                            **_json_safe_mapping(report),
                        },
                    )
                )
            if candidates:
                break

        if errors:
            LOGGER.info("camelot fallback diagnostics: %s", "; ".join(errors))
        return candidates


def _package_version(distribution_name: str) -> str:
    try:
        return importlib.metadata.version(distribution_name)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _normalize_cells(rows: list[list[Any]]) -> list[list[str]]:
    normalized: list[list[str]] = []
    for row in rows:
        cells = ["" if cell is None else str(cell).strip() for cell in row]
        if any(cells):
            normalized.append(cells)
    return normalized


def _bbox(value: Any) -> tuple[float, float, float, float]:
    raw = list(value)
    if len(raw) != 4:
        return (0.0, 0.0, 0.0, 0.0)
    return (float(raw[0]), float(raw[1]), float(raw[2]), float(raw[3]))


def _json_safe_mapping(value: dict[str, Any]) -> dict[str, str | int | float | bool | None]:
    safe: dict[str, str | int | float | bool | None] = {}
    for key, item in value.items():
        if isinstance(item, str | int | float | bool) or item is None:
            safe[str(key)] = item
        else:
            safe[str(key)] = str(item)
    return safe
