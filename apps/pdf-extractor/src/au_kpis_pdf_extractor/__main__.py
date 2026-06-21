from __future__ import annotations

import uvicorn

from au_kpis_pdf_extractor.runtime import configured_port


def main() -> None:
    uvicorn.run(
        "au_kpis_pdf_extractor.app:app",
        host="0.0.0.0",
        port=configured_port(),
        proxy_headers=True,
    )


if __name__ == "__main__":
    main()
