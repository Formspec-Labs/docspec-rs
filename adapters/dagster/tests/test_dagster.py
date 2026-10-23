"""Exercise native Dagster execution and retry events against a loopback HTTP peer."""

import importlib.util
import os
import unittest
from unittest.mock import patch

from http_fixture import RELEASE, REQUEST, reply, server

DAGSTER_AVAILABLE = importlib.util.find_spec("dagster") is not None
if DAGSTER_AVAILABLE:
    from dagster import RetryPolicy, job

    from docspec_dagster.definitions import DocSpecResource, docspec_job, docspec_operation

    @job(op_retry_policy=RetryPolicy(max_retries=3, delay=0))
    def caller_retry_job():
        docspec_operation()


@unittest.skipUnless(DAGSTER_AVAILABLE, "Dagster is not installed; install the adapter's pinned dependency")
class DagsterTests(unittest.TestCase):
    def execute(self, url, *, selected_job=None, retries=1, max_response_bytes=1024):
        clean_env = {
            k: v
            for k, v in os.environ.items()
            if k.upper() not in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "DAGSTER_HOME")
        }
        with patch.dict("os.environ", clean_env, clear=True):
            return (selected_job or docspec_job).execute_in_process(
                run_config={"ops": {"docspec_operation": {"config": {"route": "/run", "request_json": REQUEST}}}},
                resources={"docspec": DocSpecResource(base_url=url, uncertain_retries=retries, retry_delay_seconds=0, max_response_bytes=max_response_bytes)},
                raise_on_error=False,
            )

    def test_native_execution_emits_only_identity_metadata(self):
        with server(reply({**RELEASE, "private_inventory": ["must-not-be-emitted"]})) as (url, requests):
            result = self.execute(url)
        self.assertTrue(result.success)
        summary = result.output_for_node("docspec_operation")
        self.assertEqual(summary["operation"], "stable-op")
        self.assertNotIn("private_inventory", summary)
        output = next(event for event in result.all_events if event.event_type_value == "STEP_OUTPUT")
        self.assertEqual(set(output.event_specific_data.metadata), set(summary))
        self.assertEqual(requests, [("/run", REQUEST.encode())])

    def test_uncertain_response_retries_exactly_the_original_request(self):
        with server(reply(mode="disconnect"), reply()) as (url, requests):
            result = self.execute(url)
        self.assertTrue(result.success)
        self.assertEqual(requests, [("/run", REQUEST.encode()), ("/run", REQUEST.encode())])
        self.assertEqual(sum(event.event_type_value == "STEP_UP_FOR_RETRY" for event in result.all_events), 1)

    def test_explicit_refusal_overrides_the_callers_retry_policy(self):
        with server(reply({"code": "capacity", "message": "completed phases retained"}, status=503)) as (url, requests):
            result = self.execute(url, selected_job=caller_retry_job, retries=3)
        self.assertFalse(result.success)
        self.assertEqual(len(requests), 1)
        self.assertFalse(any(event.event_type_value == "STEP_UP_FOR_RETRY" for event in result.all_events))
        failure = result.failure_data_for_node("docspec_operation").user_failure_data
        self.assertEqual(failure.metadata["code"].value, "capacity")
        self.assertEqual(failure.metadata["message"].value, "completed phases retained")

    def test_exhausted_unknown_outcome_does_not_become_a_claim_of_rust_failure(self):
        with server(reply(mode="disconnect"), reply(mode="disconnect")) as (url, requests):
            result = self.execute(url, selected_job=caller_retry_job)
        self.assertFalse(result.success)
        self.assertEqual(len(requests), 2)
        failure = result.failure_data_for_node("docspec_operation").user_failure_data
        self.assertEqual(failure.metadata["outcome"].value, "unknown")

    def test_large_refusal_message_has_an_explicit_event_truncation_flag(self):
        with server(reply({"code": "operation_conflict", "message": "x" * 3000}, status=409)) as (url, _):
            result = self.execute(url, max_response_bytes=4096)
        self.assertFalse(result.success)
        failure = result.failure_data_for_node("docspec_operation").user_failure_data
        self.assertEqual(len(failure.metadata["message"].value), 2048)
        self.assertTrue(failure.metadata["message_truncated"].value)

    def test_response_limit_is_terminal_even_with_a_caller_retry_policy(self):
        with server(reply({**RELEASE, "large": "x" * 4096}, mode="chunked")) as (url, requests):
            result = self.execute(url, selected_job=caller_retry_job, max_response_bytes=256)
        self.assertFalse(result.success)
        self.assertEqual(len(requests), 1)
        failure = result.failure_data_for_node("docspec_operation").user_failure_data
        self.assertEqual(failure.metadata["outcome"].value, "unknown")

    def test_mismatched_acknowledgment_preserves_unknown_outcome(self):
        with server(reply({**RELEASE, "id": "different-operation"})) as (url, requests):
            result = self.execute(url, selected_job=caller_retry_job)
        self.assertFalse(result.success)
        self.assertEqual(len(requests), 1)
        failure = result.failure_data_for_node("docspec_operation").user_failure_data
        self.assertEqual(failure.metadata["outcome"].value, "unknown")
        self.assertIn("same native request", failure.description)


if __name__ == "__main__":
    unittest.main()
