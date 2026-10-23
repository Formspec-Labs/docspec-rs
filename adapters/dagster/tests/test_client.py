"""Transport qualification using real bounded loopback HTTP responses."""

import json
import unittest
from unittest.mock import patch

from docspec_dagster.client import (
    REQUEST_LIMIT,
    AdapterError,
    DocSpecClient,
    RemoteRefusal,
    ResponseError,
    ResponseTooLarge,
    UncertainOutcome,
)
from http_fixture import RELEASE, REQUEST, reply, server


class ClientTests(unittest.TestCase):
    def test_preserves_exact_native_request_and_returns_only_small_identity_summary(self):
        with server(reply({**RELEASE, "private_inventory": ["not-for-events"]})) as (url, requests):
            with patch.dict("os.environ", {"HTTP_PROXY": "http://192.0.2.1:9"}):
                result = DocSpecClient(url).submit("/run", REQUEST)
        self.assertEqual(requests, [("/run", REQUEST.encode())])
        self.assertEqual(result, {"route": "/run", "dataset": "comments", "operation": "stable-op", "outcome": "completed", "result_id": "stable-op"})

    def test_plan_and_export_acknowledgments_keep_the_same_identity(self):
        fixtures = [
            ("/plan", {"dataset": "comments", "operation": "stable-op", "completed": False, "limits": {"ignored": True}}, "planned"),
            ("/receipts/run", RELEASE, "completed"),
            ("/exports/run", {"dataset": "comments", "id": "stable-op"}, "completed"),
        ]
        for route, body, outcome in fixtures:
            with self.subTest(route=route), server(reply(body)) as (url, _):
                result = DocSpecClient(url).submit(route, REQUEST)
                self.assertEqual(result["outcome"], outcome)
                self.assertNotIn("limits", result)

    def test_wrong_acknowledgment_refuses_without_claiming_completion(self):
        for body in ({**RELEASE, "id": "other-op"}, {**RELEASE, "dataset": {"dataset": "other"}}):
            with self.subTest(body=body), server(reply(body)) as (url, _):
                with self.assertRaises(ResponseError):
                    DocSpecClient(url).submit("/run", REQUEST)

    def test_bounded_success_with_length_chunked_and_no_length(self):
        for mode in ("length", "chunked", "no_length"):
            with self.subTest(mode=mode), server(reply(mode=mode)) as (url, _):
                self.assertEqual(DocSpecClient(url, max_response_bytes=256).submit("/run", REQUEST)["result_id"], "stable-op")

    def test_excess_response_refuses_for_every_framing_mode(self):
        for mode in ("length", "chunked", "no_length"):
            with self.subTest(mode=mode), server(reply({**RELEASE, "extra": "x" * 4096}, mode=mode)) as (url, requests):
                with self.assertRaises(ResponseTooLarge):
                    DocSpecClient(url, max_response_bytes=256).submit("/run", REQUEST)
                self.assertEqual(len(requests), 1)

    def test_large_declared_length_refuses_before_waiting_for_payload(self):
        with server(reply(b"", declared_length=1000000)) as (url, _):
            with self.assertRaises(ResponseTooLarge):
                DocSpecClient(url, max_response_bytes=256).submit("/run", REQUEST)

    def test_partial_response_and_disconnect_have_unknown_outcome(self):
        for response in (reply(b"{}", declared_length=20), reply(mode="disconnect")):
            with self.subTest(response=response), server(response) as (url, requests):
                with self.assertRaises(UncertainOutcome):
                    DocSpecClient(url).submit("/run", REQUEST)
                self.assertEqual(len(requests), 1)

    def test_explicit_refusals_preserve_code_message_and_status_even_for_503(self):
        for status, code in ((409, "operation_conflict"), (503, "capacity")):
            detail = {"code": code, "message": "retained work remains available"}
            with self.subTest(status=status), server(reply(detail, status=status)) as (url, requests):
                with self.assertRaises(RemoteRefusal) as caught:
                    DocSpecClient(url).submit("/run", REQUEST)
                self.assertEqual(caught.exception.response, detail)
                self.assertEqual(caught.exception.status, status)
                self.assertEqual(len(requests), 1)

    def test_unstructured_server_failure_is_uncertain(self):
        with server(reply(b"server stopped", status=502)) as (url, _):
            with self.assertRaises(UncertainOutcome):
                DocSpecClient(url).submit("/run", REQUEST)

    def test_malformed_refusal_is_terminal_even_when_server_status_is_500(self):
        with server(reply({"code": "invalid code", "message": "cannot proceed"}, status=500)) as (url, _):
            with self.assertRaises(ResponseError):
                DocSpecClient(url).submit("/run", REQUEST)

    def test_redirect_is_never_followed(self):
        with server(reply(status=307, headers={"Location": "http://192.0.2.1:9/run"})) as (url, requests):
            with self.assertRaises(ResponseError):
                DocSpecClient(url).submit("/run", REQUEST)
            self.assertEqual(len(requests), 1)

    def test_ambiguous_framing_and_content_encoding_refuse(self):
        for headers in ({"Transfer-Encoding": "chunked"}, {"Content-Encoding": "gzip"}, {"Content-Length": "2"}):
            with self.subTest(headers=headers), server(reply(headers=headers)) as (url, _):
                with self.assertRaises(ResponseError):
                    DocSpecClient(url).submit("/run", REQUEST)

        with server(reply(mode="no_length", headers={"Transfer-Encoding": "gzip"})) as (url, _):
            with self.assertRaises(ResponseError):
                DocSpecClient(url).submit("/run", REQUEST)

    def test_response_json_must_be_unique_finite_and_an_object(self):
        for body in (b"[]", b'{"id":"x","id":"y"}', b'{"x":NaN}', b'{"x":1e999}', b"not json"):
            with self.subTest(body=body), server(reply(body)) as (url, _):
                with self.assertRaises(ResponseError):
                    DocSpecClient(url).submit("/run", REQUEST)

    def test_url_scope_is_literal_loopback_without_dns_credentials_or_paths(self):
        for url in (
            "http://localhost:8080", "http://192.0.2.1:8080", "https://127.0.0.1",
            "http://user:password@127.0.0.1", "http://127.0.0.1/api", "http://127.0.0.1?target=x",
            "http://127.0.0.1#x", "http://127.0.0.1:0", "http://[::1%lo0]", " http://127.0.0.1",
        ):
            with self.subTest(url=url), self.assertRaises(AdapterError):
                DocSpecClient(url)
        DocSpecClient("http://[::1]:8080/")

    def test_invalid_requests_never_reach_http(self):
        invalid = (
            ("/retention/removal/execute", REQUEST),
            ("/run?other=1", REQUEST),
            ("/run", '{"dataset":"comments","operation":"one","operation":"two"}'),
            ("/run", '{"dataset":"comments","operation":"stable-op","value":NaN}'),
            ("/run", '{"dataset":"comments","operation":"stable-op","value":1e999}'),
            ("/run", '{"dataset":"comments"}'),
            ("/run", "x" * (REQUEST_LIMIT + 1)),
            ("/run", json.dumps({"dataset": "comments", "operation": "stable-op", "text": "é" * (REQUEST_LIMIT // 2)}, ensure_ascii=False)),
        )
        with patch("http.client.HTTPConnection") as connection:
            for route, payload in invalid:
                with self.subTest(route=route, size=len(payload)), self.assertRaises(AdapterError):
                    DocSpecClient().submit(route, payload)
            connection.assert_not_called()

    def test_invalid_capacity_and_timeout_refuse_at_construction(self):
        for options in ({"max_response_bytes": 0}, {"max_response_bytes": 33 << 20}, {"timeout_seconds": float("inf")}, {"timeout_seconds": 0}):
            with self.subTest(options=options), self.assertRaises(AdapterError):
                DocSpecClient(**options)

    def test_timeout_header_and_gateway_timeout_uncertainty_and_capabilities(self):
        with server(reply({"routes": ["/run", "/plan"]})) as (url, requests):
            caps = DocSpecClient(url, timeout_seconds=12.5).capabilities()
        self.assertEqual(caps, {"routes": ["/run", "/plan"]})
        self.assertEqual(requests, [("/capabilities", b"")])
        with server(reply({"code": "request_timeout", "message": "HTTP request deadline elapsed"}, status=504)) as (url, _):
            with self.assertRaises(UncertainOutcome):
                DocSpecClient(url, timeout_seconds=1.5).submit("/run", REQUEST)


if __name__ == "__main__":
    unittest.main()
