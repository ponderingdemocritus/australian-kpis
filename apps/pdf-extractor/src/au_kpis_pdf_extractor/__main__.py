from __future__ import annotations

import uvicorn


def main() -> None:
    uvicorn.run(
        "au_kpis_pdf_extractor.app:app",
        host="0.0.0.0",
        port=8000,
        proxy_headers=True,
    )


if __name__ == "__main__":
    main()
