"""A loopback HTTP peer for protocol faults; it does not simulate Rust lifecycle rules."""

from __future__ import annotations

import json
import socket
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REQUEST = '{ "dataset": "comments", "operation": "stable-op", "sql": "SELECT private_text FROM input" }'
RELEASE = {"id": "stable-op", "dataset": {"dataset": "comments"}, "verified": True}


def reply(body=RELEASE, *, status=200, mode="length", declared_length=None, headers=None):
    if not isinstance(body, bytes):
        body = json.dumps(body).encode()
    return {"body": body, "status": status, "mode": mode, "declared_length": declared_length, "headers": headers or {}}


@contextmanager
def server(*responses):
    requests = []
    pending = list(responses)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            requests.append((self.path, payload))
            current = pending.pop(0) if pending else reply(b"unexpected request", status=500)
            if current["mode"] == "disconnect":
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                self.close_connection = True
                return
            self.send_response(current["status"])
            self.send_header("Content-Type", "application/json")
            self.send_header("Connection", "close")
            body = current["body"]
            if current["mode"] == "chunked":
                self.send_header("Transfer-Encoding", "chunked")
            elif current["mode"] == "length":
                length = current["declared_length"]
                self.send_header("Content-Length", str(len(body) if length is None else length))
            for key, value in current["headers"].items():
                self.send_header(key, value)
            self.end_headers()
            try:
                if current["mode"] == "chunked":
                    for start in range(0, len(body), 23):
                        chunk = body[start:start + 23]
                        self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                    self.wfile.write(b"0\r\n\r\n")
                else:
                    self.wfile.write(body)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            self.close_connection = True

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}", requests
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
