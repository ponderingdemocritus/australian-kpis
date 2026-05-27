FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ghostscript \
        libgl1 \
        libglib2.0-0 \
        libgomp1 \
        libsm6 \
        libxext6 \
        libxrender1 \
        poppler-utils \
        tcl \
        tk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY apps/pdf-extractor/pyproject.toml ./pyproject.toml
COPY apps/pdf-extractor/src ./src

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir .

RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --retries=6 \
    CMD curl -fsS http://127.0.0.1:8000/health >/dev/null || exit 1

CMD ["python", "-m", "au_kpis_pdf_extractor"]
