from __future__ import annotations

import tempfile
from collections.abc import Iterator
from os import getenv
from pathlib import Path
from typing import Protocol

from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse

from au_kpis_pdf_extractor.extractors import DeterministicExtractor, ExtractionError
from au_kpis_pdf_extractor.models import ExtractionResponse, ExtractionStrategy, ExtractRequest
from au_kpis_pdf_extractor.storage import ObjectNotFound, S3StorageClient, StorageError


class StorageClient(Protocol):
    def fetch_to_path(self, s3_key: str, destination: Path) -> Path: ...


class Extractor(Protocol):
    def extract(self, pdf_path: Path, artifact_key: str) -> ExtractionResponse: ...


def create_app(
    *,
    storage_client: StorageClient | None = None,
    extractor: Extractor | None = None,
) -> FastAPI:
    app = FastAPI(title="Australian KPIs PDF extractor", version="0.1.0")
    storage = storage_client or S3StorageClient()
    table_extractor = extractor or DeterministicExtractor(max_pages=_configured_max_pages())

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/extract")
    async def extract(request: ExtractRequest) -> StreamingResponse:
        if request.strategy == ExtractionStrategy.MODEL_FALLBACK:
            raise HTTPException(status_code=501, detail="model fallback backend is not configured")

        with tempfile.TemporaryDirectory(prefix="au-kpis-pdf-") as temp_dir:
            pdf_path = Path(temp_dir) / "artifact.pdf"
            try:
                await run_in_threadpool(storage.fetch_to_path, request.s3_key, pdf_path)
                response = await run_in_threadpool(
                    table_extractor.extract,
                    pdf_path,
                    request.s3_key,
                )
            except ObjectNotFound as err:
                raise HTTPException(status_code=404, detail=str(err)) from err
            except StorageError as err:
                raise HTTPException(status_code=502, detail=str(err)) from err
            except ExtractionError as err:
                raise HTTPException(status_code=422, detail=str(err)) from err

        return StreamingResponse(
            _stream_json_response(response),
            media_type="application/json",
        )

    return app


def _stream_json_response(response: ExtractionResponse) -> Iterator[bytes]:
    payload = response.model_dump_json()
    yield payload.encode("utf-8")


def _configured_max_pages() -> int | None:
    raw_value = getenv("AU_KPIS_PDF_EXTRACTOR__MAX_PAGES")
    if raw_value is None or raw_value.strip() == "":
        return None

    try:
        max_pages = int(raw_value)
    except ValueError as err:
        raise ValueError("AU_KPIS_PDF_EXTRACTOR__MAX_PAGES must be a positive integer") from err

    if max_pages < 1:
        raise ValueError("AU_KPIS_PDF_EXTRACTOR__MAX_PAGES must be a positive integer")
    return max_pages


app = create_app()
