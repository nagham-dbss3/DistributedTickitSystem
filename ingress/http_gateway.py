"""HTTP ingress proxy for booking requests.

The proxy is intentionally small and uses only the standard library. It gives
the simulator a real HTTP boundary while still forwarding requests into the
existing in-process gateway/service graph.
"""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from typing import Callable, Dict, Optional
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError


class BookingIngressProxy:
    """Local HTTP reverse-proxy style ingress for booking requests."""

    def __init__(
        self,
        request_handler: Callable[[Dict[str, object]], Dict[str, object]],
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self.request_handler = request_handler
        self.host = host
        self.port = int(port)
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def base_url(self) -> str:
        if not self._server:
            return f"http://{self.host}:{self.port}"
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def start(self) -> None:
        if self._server:
            return

        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                if self.path != "/book":
                    self._write_json(404, {"ok": False, "error": "not_found"})
                    return

                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                    result = proxy.request_handler(payload)
                    status = 200 if result.get("ok") else 409
                    self._write_json(status, result)
                except Exception as exc:
                    self._write_json(500, {"ok": False, "error": "ingress_error", "message": str(exc)})

            def log_message(self, format: str, *args) -> None:
                return

            def _write_json(self, status: int, payload: Dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="BookingIngressProxy",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if not self._server:
            return
        self._server.shutdown()
        self._server.server_close()
        self._server = None
        self._thread = None

    def submit_booking(self, seat_id: int, requester: str, timeout: float = 3.0) -> Dict[str, object]:
        """Submit a booking request through the HTTP ingress."""
        if not self._server:
            raise RuntimeError("Booking ingress proxy is not running")

        body = json.dumps({"seat_id": seat_id, "requester": requester}).encode("utf-8")
        req = urlrequest.Request(
            f"{self.base_url}/book",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )

        try:
            with urlrequest.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            try:
                return json.loads(exc.read().decode("utf-8"))
            except Exception:
                return {"ok": False, "error": "http_error", "message": str(exc)}
        except URLError as exc:
            raise RuntimeError(f"Ingress request failed: {exc}") from exc
