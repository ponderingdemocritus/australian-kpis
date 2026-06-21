from __future__ import annotations

import os


def configured_port() -> int:
    raw_port = os.environ.get("PORT", "8000")
    try:
        port = int(raw_port)
    except ValueError as err:
        raise ValueError("PORT must be a positive integer") from err
    if port < 1 or port > 65535:
        raise ValueError("PORT must be between 1 and 65535")
    return port
