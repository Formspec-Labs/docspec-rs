"""Bounded HTTP metadata transport to one existing loopback DocSpec host."""

from __future__ import annotations

import http.client
import ipaddress
import json
import math
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

# Match the Rust HTTP router's request limit. This adapter never transfers values.
REQUEST_LIMIT = 1 << 20
RESPONSE_LIMIT = 1 << 20
# A finite ceiling also applies when callers raise the default response allowance.
MAX_RESPONSE_LIMIT = 32 << 20
ROUTES = frozenset(
    {
        "/plan",
        "/run",
        "/updates/run",
        "/schemas/run",
        "/backfills/run",
        "/refreshes/run",
        "/exports/run",
    }
)
_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


class AdapterError(Exception):
    """A terminal adapter error; automatic retry will not repair this request."""


class ResponseError(AdapterError):
    """An unusable response; the operation may already have completed in Rust."""


class ResponseTooLarge(ResponseError):
    """The configured metadata response allowance was exceeded."""


class UncertainOutcome(AdapterError):
    """The connection did not establish the outcome; retry only the same request."""


class RemoteRefusal(AdapterError):
    """An explicit Rust refusal, retained separately from transport uncertainty."""

    def __init__(self, status: int, response: dict[str, Any]):
        self.status = status
        self.response = response
        self.code = response["code"]
        self.message = response["message"]
        super().__init__(f"DocSpec refused {self.code} (HTTP {status})")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ValueError("non-finite JSON number")


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite JSON number")
    return number


def _json_object(payload: bytes) -> dict[str, Any]:
    value = json.loads(
        payload.decode("utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=_invalid_constant,
        parse_float=_finite_float,
    )
    if not isinstance(value, dict):
        raise ValueError("JSON object required")
    return value


def _identifier(value: Any) -> bool:
    return isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None


@dataclass(frozen=True)
class DocSpecClient:
    """Submit one request without retries, proxies, redirects or lifecycle storage.

    Timeouts apply to socket operations, not to the Rust operation's lifetime.
    The caller owns scheduling and may retry an uncertain outcome with the exact
    original JSON. Explicit refusals always remain terminal to this adapter.
    """

    base_url: str = "http://127.0.0.1:8080"
    timeout_seconds: float = 60.0
    max_response_bytes: int = RESPONSE_LIMIT

    def __post_init__(self) -> None:
        self._endpoint()
        if (
            not isinstance(self.timeout_seconds, (int, float))
            or isinstance(self.timeout_seconds, bool)
            or not math.isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            raise AdapterError("timeout_seconds must be finite and positive")
        if (
            not isinstance(self.max_response_bytes, int)
            or isinstance(self.max_response_bytes, bool)
            or not 1 <= self.max_response_bytes <= MAX_RESPONSE_LIMIT
        ):
            raise AdapterError("max_response_bytes must be between 1 byte and 32 MiB")

    def _endpoint(self) -> tuple[str, int]:
        try:
            parsed = urlsplit(self.base_url)
            host = parsed.hostname or ""
            address = ipaddress.ip_address(host)
            port = parsed.port if parsed.port is not None else 80
            valid = (
                parsed.scheme == "http"
                and address.is_loopback
                and "%" not in host
                and parsed.username is None
                and parsed.password is None
                and parsed.path in ("", "/")
                and not parsed.query
                and not parsed.fragment
                and 1 <= port <= 65535
                and not any(character.isspace() for character in self.base_url)
            )
        except (ValueError, TypeError) as error:
            raise AdapterError("base_url must be HTTP on a literal loopback IP") from error
        if not valid:
            raise AdapterError("base_url must be HTTP on a literal loopback IP without credentials or a path")
        return host, port

    def submit(self, route: str, request_json: str) -> dict[str, Any]:
        """Return a small acknowledgment bound to the caller's original identity.

        No complete request, source values, native object inventory or result pin
        is returned to Dagster. Rust retains the result and resolves its identity.
        """
        if route not in ROUTES:
            raise AdapterError("route is not an allowed DocSpec operation")
        if not isinstance(request_json, str) or len(request_json) > REQUEST_LIMIT:
            raise AdapterError("request JSON exceeds the 1 MiB request allowance")
        try:
            payload = request_json.encode("utf-8")
            if len(payload) > REQUEST_LIMIT:
                raise AdapterError("request JSON exceeds the 1 MiB request allowance")
            request = _json_object(payload)
        except (ValueError, RecursionError) as error:
            raise AdapterError("request must be a UTF-8 JSON object with unique members") from error
        dataset, operation = request.get("dataset"), request.get("operation")
        if not _identifier(dataset) or not _identifier(operation):
            raise AdapterError("request requires its original bounded dataset and operation IDs")

        host, port = self._endpoint()
        connection = http.client.HTTPConnection(host, port, timeout=self.timeout_seconds)
        try:
            connection.request(
                "POST",
                route,
                body=payload,
                headers={"Content-Type": "application/json", "Accept": "application/json", "Accept-Encoding": "identity"},
            )
            response = connection.getresponse()
            if 300 <= response.status < 400:
                raise ResponseError("DocSpec redirect refused; the operation outcome is unknown")
            raw = self._read_bounded(response)
            try:
                body = _json_object(raw)
            except (ValueError, RecursionError) as error:
                if response.status >= 500:
                    raise UncertainOutcome("DocSpec server response did not establish the operation outcome") from error
                raise ResponseError("DocSpec returned invalid JSON; the operation may have completed") from error
            if not 200 <= response.status < 300:
                if _identifier(body.get("code")) and isinstance(body.get("message"), str):
                    raise RemoteRefusal(response.status, body)
                if "code" in body or "message" in body:
                    raise ResponseError("DocSpec returned a malformed refusal; inspect it before retrying")
                if response.status >= 500:
                    raise UncertainOutcome("DocSpec server response did not establish the operation outcome")
                raise ResponseError(f"DocSpec returned HTTP {response.status} without a structured refusal")
            return self._summary(route, dataset, operation, body)
        except (OSError, http.client.HTTPException) as error:
            # Do not attach raw network diagnostics or request bodies to Dagster events.
            raise UncertainOutcome("DocSpec connection ended without a confirmed outcome; retain the original request") from error
        finally:
            connection.close()

    def _read_bounded(self, response: http.client.HTTPResponse) -> bytes:
        if response.getheader("Content-Encoding", "identity").lower() != "identity":
            raise ResponseError("encoded responses are unsupported; the operation may have completed")
        lengths = response.headers.get_all("Content-Length", [])
        expected = None
        if lengths:
            if len(lengths) != 1 or not re.fullmatch(r"[0-9]+", lengths[0]):
                raise ResponseError("invalid Content-Length; the operation outcome is unknown")
            # Refuse very long numeric strings before parsing them as integers.
            if len(lengths[0]) > 10:
                raise ResponseTooLarge("response exceeds its metadata allowance; the operation may have completed")
            expected = int(lengths[0])
            if expected > self.max_response_bytes:
                raise ResponseTooLarge("response exceeds its metadata allowance; the operation may have completed")
        transfers = response.headers.get_all("Transfer-Encoding", [])
        if transfers and (lengths or len(transfers) != 1 or transfers[0].lower() != "chunked"):
            raise ResponseError("unsupported or ambiguous HTTP framing; the operation outcome is unknown")
        # HTTPResponse removes chunk framing. The extra byte detects excess even
        # when Content-Length is absent or the body is chunked.
        raw = response.read(self.max_response_bytes + 1)
        if len(raw) > self.max_response_bytes:
            raise ResponseTooLarge("response exceeds its metadata allowance; the operation may have completed")
        if expected is not None and len(raw) != expected:
            raise UncertainOutcome("DocSpec response ended before its declared length")
        return raw

    @staticmethod
    def _summary(route: str, dataset: str, operation: str, body: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {"route": route, "dataset": dataset, "operation": operation}
        if route == "/plan":
            if body.get("dataset") != dataset or body.get("operation") != operation or type(body.get("completed")) is not bool:
                raise ResponseError("plan acknowledgment does not match the submitted operation")
            result.update(outcome="planned", completed=body["completed"])
        else:
            selected_dataset = body.get("dataset")
            if route != "/exports/run" and isinstance(selected_dataset, dict):
                selected_dataset = selected_dataset.get("dataset")
            if body.get("id") != operation or selected_dataset != dataset:
                raise ResponseError("result acknowledgment does not match the submitted operation")
            result.update(outcome="completed", result_id=operation)
        return result
