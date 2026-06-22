from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, Field


class ExtractionStrategy(StrEnum):
    DETERMINISTIC = "deterministic"
    MODEL_FALLBACK = "model_fallback"


class ExtractionBackendKind(StrEnum):
    DETERMINISTIC = "deterministic"
    MODEL = "model"


class ExtractRequest(BaseModel):
    s3_key: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    artifact_date: str | None = None
    strategy: ExtractionStrategy | None = None
    pages: list[Annotated[int, Field(ge=1)]] | None = Field(default=None, min_length=1)


class BackendInfo(BaseModel):
    kind: ExtractionBackendKind
    name: str
    version: str
    model_sha256: str | None = None


class CellSpan(BaseModel):
    row: int = Field(ge=0)
    column: int = Field(ge=0)
    row_span: int = Field(ge=1)
    column_span: int = Field(ge=1)


class TableCandidate(BaseModel):
    page: int = Field(ge=1)
    bbox: tuple[float, float, float, float]
    cells: list[list[str]]
    spans: list[CellSpan] = Field(default_factory=list)
    diagnostics: dict[str, Any] = Field(default_factory=dict)


class ExtractionResponse(BaseModel):
    artifact_key: str
    backend: BackendInfo
    tables: list[TableCandidate]
