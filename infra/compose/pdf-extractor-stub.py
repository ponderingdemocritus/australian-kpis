"""Local PDF extractor stub for the docker compose development stack."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json


class Handler(BaseHTTPRequestHandler):
    server_version = "au-kpis-pdf-stub/0.1"

    def do_GET(self) -> None:
        if self.path == "/health":
            self._json(200, {"status": "ok"})
            return
        self._json(404, {"error": "not_found"})

    def do_POST(self) -> None:
        if self.path != "/extract":
            self._json(404, {"error": "not_found"})
            return

        length = int(self.headers.get("content-length", "0"))
        payload = self.rfile.read(length) if length else b"{}"
        try:
            request = json.loads(payload)
        except json.JSONDecodeError:
            self._json(400, {"error": "invalid_json"})
            return

        self._json(
            200,
            {
                "s3_key": request.get("s3_key"),
                "pages": [],
                "tables": [],
            },
        )

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _json(self, status: int, body: dict[str, object]) -> None:
        encoded = json.dumps(body, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def main() -> None:
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()


if __name__ == "__main__":
    main()
