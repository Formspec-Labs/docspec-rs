"""Opt-in native Dagster execution against the real Rust HTTP host."""

from contextlib import contextmanager
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
import importlib.util
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

DAGSTER_AVAILABLE = importlib.util.find_spec("dagster") is not None
if DAGSTER_AVAILABLE:
    from docspec_dagster.definitions import DocSpecResource, docspec_job


@unittest.skipUnless(DAGSTER_AVAILABLE, "Dagster is required for real Rust integration")
class RustIntegrationTests(unittest.TestCase):
    """End-to-end Dagster adapter qualification against a real loopback Rust host.

    Set `DOCSPEC_BINARY` to a compiled `docspec` executable (built with `--features test-util`
    for pause/resume recovery tests) and `DOCSPEC_TEST_INPUT` to a small (< 1 MiB) Parquet file.
    """

    def test_same_native_request_reconciles_without_more_retained_writes(self):
        """Replaying an identical `/run` request reconciles the retained release without new writes."""
        self._qualify("replay")

    def test_advertised_routes_select_native_receipts_update_schema_backfill_and_summary(self):
        """Exercise `/plan`, `/run`, `/receipts/run`, `/updates/run`, `/schemas/run`, `/backfills/run`, `/refreshes/run`, and `/exports/run`."""
        self._qualify("routes")

    def test_native_export_response_loss_retries_without_retained_rewrites(self):
        """Retrying `/exports/run` after lost HTTP response headers reconciles the single export artifact."""
        self._qualify("export")

    def test_native_completed_response_loss_retries_without_retained_rewrites(self):
        """In-process Dagster step retry after lost HTTP response reconciles the completed `/run` release."""
        self._qualify("retry")

    def test_replacement_python_worker_reconciles_native_completed_response_loss(self):
        """A replacement Python worker process reconciles a completed release after response loss."""
        self._qualify("replacement")

    def test_killed_python_worker_reconciles_unfinished_native_request(self):
        """Killing the Python worker mid-flight leaves the Rust operation running to completion for replacement reconciliation."""
        self._qualify("workerkill")

    def test_killed_python_worker_and_native_host_resume_retained_processing(self):
        """Killing both the Python worker and the Rust host mid-flight resumes from retained processing state without duplicating work."""
        self._qualify("hostkill")

    def _qualify(self, mode):
        binary_name = os.environ.get("DOCSPEC_BINARY")
        input_name = os.environ.get("DOCSPEC_TEST_INPUT")
        if not binary_name and not input_name:
            self.skipTest("set DOCSPEC_BINARY and DOCSPEC_TEST_INPUT to opt into real Rust integration")
        self.assertTrue(binary_name and input_name, "both integration paths must be set")
        binary = Path(binary_name).resolve(strict=True)
        source = Path(input_name).resolve(strict=True)
        self.assertTrue(binary.is_file() and os.access(binary, os.X_OK), "DOCSPEC_BINARY must be an executable")
        self.assertTrue(source.is_file(), "DOCSPEC_TEST_INPUT must be a small Parquet file")
        self.assertLessEqual(source.stat().st_size, 1 << 20, "use a tiny fixture, not a retained corpus")

        with tempfile.TemporaryDirectory(prefix="docspec-dagster-") as directory:
            root = Path(directory)
            workspace = root / "workspace"
            log_path = root / "host.log"
            interrupted = mode in ("workerkill", "hostkill")
            if interrupted:
                # Never remove or modify the shared integration source. Native
                # recovery must succeed after this fixture-owned locator is gone.
                private_source = root / "input.parquet"
                shutil.copyfile(source, private_source)
                source = private_source
            with (root / "hash.json").open("wb") as output, (root / "hash.log").open("wb") as errors:
                hashed = subprocess.run([str(binary), "hash", str(source)], stdout=output, stderr=errors, timeout=30, check=False)
            self.assertEqual(hashed.returncode, 0, self._tail(root / "hash.log"))
            with (root / "hash.json").open("rb") as output:
                encoded = output.read((64 << 10) + 1)
            self.assertLessEqual(len(encoded), 64 << 10)
            digest = json.loads(encoded)["digest"]

            # Match the Rust correctness fixtures' bounded local profile. These
            # limits and deadlines establish no performance or memory claim.
            limits = root / "limits.json"
            limits.write_text(json.dumps({
                "compute_threads": 2,
                "memory_bytes": 512 << 20,
                "scratch_bytes": 16 << 20,
                "max_batch_bytes": 4 << 20,
                "max_metadata_bytes": 16 << 20,
            }))
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            with log_path.open("wb") as log:
                environment = os.environ.copy()
                for name in ("DOCSPEC_TEST_PAUSE_BEFORE", "DOCSPEC_TEST_PAUSE_MARKER", "DOCSPEC_TEST_RESUME_MARKER"):
                    environment.pop(name, None)
                if interrupted:
                    environment.update({
                        "DOCSPEC_TEST_PAUSE_BEFORE": "validation",
                        "DOCSPEC_TEST_PAUSE_MARKER": str(root / "native-paused"),
                        "DOCSPEC_TEST_RESUME_MARKER": str(root / "native-resume"),
                    })
                host = self._host(binary, workspace, limits, port, log, environment)
                hosts = [host]
                try:
                    self._wait_ready(host, port, log_path)
                    request_json = json.dumps({
                        "dataset": "dagster_fixture",
                        "operation": "stable_build",
                        "input": str(source),
                        "expected_digest": digest,
                        "sql": ("SELECT CAST(1 AS BIGINT) AS value" if mode == "routes" else
                                "SELECT CAST(1 AS BIGINT) AS value FROM input" if mode == "export" else "SELECT * FROM input"),
                        "definition": "dagster-fixture-v1",
                    })
                    config = {"ops": {"docspec_operation": {"config": {"route": "/run", "request_json": request_json}}}}
                    resource = DocSpecResource(base_url=f"http://127.0.0.1:{port}", uncertain_retries=0)

                    if interrupted:
                        host, baseline, first_summary, recovered = self._recover_interrupted(
                            mode, root, binary, workspace, limits, port, log, log_path, host, hosts, config, source,
                        )
                    elif mode in ("replay", "export", "routes"):
                        first = docspec_job.execute_in_process(run_config=config, resources={"docspec": resource}, raise_on_error=False)
                        self.assertTrue(first.success, self._tail(log_path))
                        baseline = self._state(port, workspace)
                        first_summary = first.output_for_node("docspec_operation")
                        second = docspec_job.execute_in_process(run_config=config, resources={"docspec": resource}, raise_on_error=False)
                        self.assertTrue(second.success, self._tail(log_path))
                        recovered = second.output_for_node("docspec_operation")
                    else:
                        # The relay does not fabricate completion: it consumes the
                        # entire real Rust 200 response and captures native state
                        # before closing the worker socket without sending headers.
                        with lost_response_relay(port, lambda: self._state(port, workspace)) as relay:
                            url = f"http://127.0.0.1:{relay.server_address[1]}"
                            if mode == "retry":
                                result = docspec_job.execute_in_process(
                                    run_config=config,
                                    resources={"docspec": DocSpecResource(base_url=url, uncertain_retries=1, retry_delay_seconds=0, timeout_seconds=30)},
                                    raise_on_error=False,
                                )
                                self.assertTrue(result.success, self._tail(log_path))
                                self.assertEqual(sum(event.event_type_value == "STEP_UP_FOR_RETRY" for event in result.all_events), 1)
                                recovered = result.output_for_node("docspec_operation")
                            else:
                                config_path = root / "worker-config.json"
                                config_path.write_text(json.dumps(config))
                                first_worker = self._worker(root, config_path, url, "lost")
                                self.assertFalse(first_worker["success"])
                                self.assertEqual(first_worker["outcome"], "unknown")
                                replacement = self._worker(root, config_path, url, "replacement")
                                self.assertTrue(replacement["success"], self._tail(log_path))
                                self.assertNotEqual(first_worker["pid"], replacement["pid"])
                                recovered = replacement["summary"]
                            self.assertEqual(relay.errors, [])
                            self.assertEqual(relay.requests, [("/run", request_json.encode())] * 2)
                            self.assertEqual(relay.completions[0]["id"], "stable_build")
                            self.assertEqual(relay.completions[1], relay.completions[0])
                            baseline = relay.baseline
                            first_summary = {"route": "/run", "dataset": "dagster_fixture", "operation": "stable_build", "outcome": "completed", "result_id": "stable_build"}
                    self.assertEqual(recovered, first_summary)
                    self.assertEqual(first_summary["result_id"], "stable_build")
                    self.assertEqual(baseline[0]["id"], "stable_build")
                    self.assertTrue(baseline[0]["verified"])
                    self.assertTrue(baseline[2], "the real host must retain native output")
                    self.assertEqual(self._state(port, workspace), baseline)
                    if mode == "export":
                        self._export_recovery(port, workspace, log_path, baseline[0])
                        baseline = self._state(port, workspace)
                    # A real route refusal proves forwarding reaches Rust rather
                    # than merely accepting the new route in a protocol fixture.
                    from docspec_dagster.client import DocSpecClient, RemoteRefusal
                    with self.assertRaises(RemoteRefusal) as refusal:
                        DocSpecClient(f"http://127.0.0.1:{port}").submit("/receipts/run", json.dumps({
                            "dataset": "dagster_fixture", "operation": "missing_receipts_base",
                            "definition": "dagster-fixture-v1", "base": "absent",
                            "replacements": {"path": str(source), "digest": digest},
                            "deletions": None,
                        }))
                    self.assertEqual(refusal.exception.status, 409)
                    self.assertEqual(refusal.exception.code, "stale_base")
                    self.assertEqual(self._state(port, workspace), baseline)
                    if mode == "routes":
                        self._routes(root, binary, port, workspace, log_path, request_json, baseline[0])
                finally:
                    host = hosts[-1]
                    if host.poll() is None:
                        host.send_signal(signal.SIGINT)
                        try:
                            host.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            host.kill()
                            host.wait(timeout=10)
                            self.fail("Rust host did not finish graceful shutdown: " + self._tail(log_path))
                    self.assertEqual(host.returncode, 0, self._tail(log_path))

    def _routes(self, root, binary, port, workspace, log_path, build_request, seed):
        url = f"http://127.0.0.1:{port}"

        def submit(route, request):
            payload = json.dumps(request)
            result = docspec_job.execute_in_process(
                run_config={"ops": {"docspec_operation": {"config": {"route": route, "request_json": payload}}}},
                resources={"docspec": DocSpecResource(base_url=url, uncertain_retries=0)},
                raise_on_error=False,
            )
            self.assertTrue(result.success, self._tail(log_path))
            self.assertEqual(result.output_for_node("docspec_operation"), {
                "route": route, "dataset": request["dataset"], "operation": request["operation"],
                "outcome": "completed", "result_id": request["operation"],
            })
            path = (f"/datasets/{request['dataset']}/exports/{request['operation']}" if route == "/exports/run"
                    else f"/datasets/{request['dataset']}/versions/{request['operation']}")
            return self._get(port, path)

        def version(pin):
            return {"dataset": pin["dataset"]["dataset"], "version": pin["id"], "manifest": pin["manifest"]}

        def stage(name, sql, columns):
            request = json.loads(build_request)
            request.update(dataset=name, operation="stage", sql=sql)
            pin = submit("/run", request)
            exported = submit("/exports/run", {
                "dataset": name, "operation": "file", "source": version(pin),
                "channel": "input", "expected": None,
                "policy": {"group": "source", "rows": "all", "columns": columns},
            })
            self.assertEqual(exported["rows"], 1)
            return {"path": exported["object"]["path"], "digest": exported["object"]["digest"]}

        before_plan = self._state(port, workspace)
        planned = docspec_job.execute_in_process(
            run_config={"ops": {"docspec_operation": {"config": {"route": "/plan", "request_json": build_request}}}},
            resources={"docspec": DocSpecResource(base_url=url, uncertain_retries=0)}, raise_on_error=False,
        )
        self.assertTrue(planned.success, self._tail(log_path))
        self.assertEqual(planned.output_for_node("docspec_operation"), {
            "route": "/plan", "dataset": "dagster_fixture", "operation": "stable_build",
            "outcome": "planned", "completed": True,
        })
        self.assertEqual(self._state(port, workspace), before_plan)
        self.assertEqual(seed["dataset"]["groups"]["source"]["rows"], 1)
        files = [obj for obj in seed["dataset"]["groups"]["source"]["objects"] if obj["path"].endswith(".parquet")]
        self.assertEqual(len(files), 1)
        literal_file = files[0]["path"].replace("'", "''")
        receipts = stage("receipt_input", f"SELECT '{literal_file}' AS source_file, CAST(0 AS BIGINT) AS source_row, 'scheduled' AS evidence",
                         ["source_file", "source_row", "evidence"])
        attached = submit("/receipts/run", {
            "dataset": "dagster_fixture", "operation": "attached", "base": seed["id"],
            "definition": "dagster-fixture-v1", "replacements": receipts, "deletions": None,
        })
        self.assertEqual(attached["dataset"]["groups"]["source"], seed["dataset"]["groups"]["source"])
        self.assertEqual(attached["dataset"]["evidence"]["table"]["rows"], 1)
        replacement = stage("replacement_input", "SELECT CAST(2 AS BIGINT) AS value", ["value"])
        updated = submit("/updates/run", {
            "dataset": "dagster_fixture", "operation": "updated", "base": attached["id"],
            "definition": "dagster-fixture-v1", "keys": ["value"], "replacements": replacement, "deletions": None,
        })
        self.assertEqual(updated["dataset"]["groups"]["source"]["rows"], 2)
        self.assertEqual(updated["dataset"]["evidence"]["table"], attached["dataset"]["evidence"]["table"])
        schema = submit("/schemas/run", {
            "dataset": "dagster_fixture", "operation": "renamed", "base": updated["id"],
            "definition": "dagster-fixture-v1", "next_definition": "renamed-v2",
            "fields": [{"id": 1, "name": "renamed"}],
        })
        self.assertEqual(schema["dataset"]["definition"], "renamed-v2")
        self.assertEqual(schema["dataset"]["groups"]["source"]["rows"], 2)
        enriched = submit("/backfills/run", {
            "dataset": "dagster_fixture", "operation": "enriched", "base": schema["id"],
            "definition": "renamed-v2", "group": "plus_one", "group_definition": "plus-one-v1",
            "fields": [{"id": 10, "name": "next_value", "expression": "renamed + 1"}],
        })
        self.assertEqual(enriched["dataset"]["groups"]["plus_one"]["rows"], 2)
        self.assertEqual(enriched["dataset"]["groups"]["source"], schema["dataset"]["groups"]["source"])
        summary = submit("/refreshes/run", {
            "dataset": "summary", "operation": "counted", "base": None, "definition": "count-v1",
            "policy": "captured", "mode": "full",
            "transform": {"kind": "aggregate", "input": version(enriched),
                          "grouping": {"keys": [], "aggregates": [{"name": "rows", "function": {"kind": "count_rows"}}]}},
        })
        self.assertEqual(summary["dataset"]["groups"]["source"]["rows"], 1)
        self.assertEqual(summary["dataset"]["related"]["coverage"]["inputs"], {"dagster_fixture": version(enriched)})
        self.assertEqual(summary["dataset"]["related"]["evidence"]["rows"], 2)
        self.assertEqual(self._get(port, "/datasets/dagster_fixture/latest"), enriched)
        self.assertEqual(self._get(port, "/datasets/summary/latest"), summary)
        # Independent native reconstruction qualifies the sequence's values,
        # receipts and scalar expression; the Python adapter still emits only IDs.
        # Summary verification traverses its exact enriched input as well.
        saved = root / "summary.json"
        saved.write_text(json.dumps(summary))
        with (root / "verify-summary.log").open("wb") as output:
            checked = subprocess.run([str(binary), "verify", str(saved), "--deep"],
                                     stdout=output, stderr=output, timeout=30, check=False)
        self.assertEqual(checked.returncode, 0, self._tail(root / "verify-summary.log"))

    def _export_recovery(self, port, workspace, log_path, source):
        source_ref = {"dataset": "dagster_fixture", "version": source["id"], "manifest": source["manifest"]}
        request_json = json.dumps({
            "dataset": "dagster_fixture", "operation": "stable_export", "source": source_ref,
            "channel": "distribution", "expected": None,
            "policy": {"group": "source", "rows": "all", "columns": ["value"]},
        })
        config = {"ops": {"docspec_operation": {"config": {"route": "/exports/run", "request_json": request_json}}}}
        channel = "/datasets/dagster_fixture/exports/channels/distribution"

        def state():
            return self._get(port, channel), self._state(port, workspace)

        # The existing relay consumes the real native completion before losing
        # only its response. It forwards exactly the same JSON on native retry.
        with lost_response_relay(port, state) as relay:
            resource = DocSpecResource(
                base_url=f"http://127.0.0.1:{relay.server_address[1]}",
                uncertain_retries=1, retry_delay_seconds=0, timeout_seconds=30,
            )
            result = docspec_job.execute_in_process(
                run_config=config, resources={"docspec": resource}, raise_on_error=False,
            )
            self.assertTrue(result.success, self._tail(log_path))
            self.assertEqual(sum(event.event_type_value == "STEP_UP_FOR_RETRY" for event in result.all_events), 1)
            self.assertEqual(result.output_for_node("docspec_operation"), {
                "route": "/exports/run", "dataset": "dagster_fixture", "operation": "stable_export",
                "outcome": "completed", "result_id": "stable_export",
            })
            self.assertEqual(relay.errors, [])
            self.assertEqual(relay.requests, [("/exports/run", request_json.encode())] * 2)
            self.assertEqual(len(relay.completions), 2)
            exported = relay.completions[0]
            self.assertEqual(relay.completions[1], exported)
            self.assertEqual(exported["source"], source_ref)
            self.assertEqual(exported["rows"], source["dataset"]["groups"]["source"]["rows"])
            self.assertEqual([field["name"] for field in exported["fields"]], ["value"])
            self.assertEqual(exported["fields"][0]["data_type"], "Int64")
            self.assertEqual(exported["validation"]["object"], exported["object"])
            self.assertEqual(state(), relay.baseline)
            baseline = relay.baseline
        replay = docspec_job.execute_in_process(
            run_config=config,
            resources={"docspec": DocSpecResource(base_url=f"http://127.0.0.1:{port}", uncertain_retries=0)},
            raise_on_error=False,
        )
        self.assertTrue(replay.success, self._tail(log_path))
        self.assertEqual(replay.output_for_node("docspec_operation"), result.output_for_node("docspec_operation"))
        self.assertEqual(state(), baseline)
        self.assertEqual(self._get(port, "/datasets/dagster_fixture/exports/stable_export"), exported)
        self.assertEqual(self._get(port, "/datasets/dagster_fixture/latest"), source)
        self.assertLessEqual(log_path.stat().st_size, 1 << 20)
        work = [line.removeprefix("DOCSPEC_TEST_WORK ") for line in log_path.read_text().splitlines()
                if line.startswith("DOCSPEC_TEST_WORK ")]
        for phase in ("export-materialization", "export-validation"):
            self.assertEqual(work.count(phase), 1, f"{phase} must execute once across export recovery and replay: {work}")

    @staticmethod
    def _host(binary, workspace, limits, port, log, environment):
        return subprocess.Popen(
            [str(binary), "--workspace", str(workspace), "--limits", str(limits), "serve", "--bind", f"127.0.0.1:{port}"],
            stdout=log, stderr=log, env=environment,
        )

    def _recover_interrupted(self, mode, root, binary, workspace, limits, port, log, log_path, host, hosts, config, source):
        config_path = root / "worker-config.json"
        original_config = json.dumps(config)
        config_path.write_text(original_config)
        url = f"http://127.0.0.1:{port}"
        report = root / "killed-report.json"
        worker_log = root / "killed-worker.log"
        with worker_log.open("wb") as output:
            worker = subprocess.Popen(
                [sys.executable, str(Path(__file__).with_name("native_worker.py")), str(config_path), url, str(report)],
                stdout=output, stderr=output,
            )
            try:
                deadline = time.monotonic() + 30
                while not (root / "native-paused").is_file():
                    self.assertIsNone(worker.poll(), self._tail(worker_log))
                    self.assertIsNone(host.poll(), self._tail(log_path))
                    if time.monotonic() >= deadline:
                        self.fail("native validation pause was not reached; use a test-util Rust binary: " + self._tail(log_path))
                    time.sleep(0.05)
                self.assertIsNone(worker.poll(), self._tail(worker_log))
                started = json.loads(report.read_text())
                self.assertEqual(started.get("started"), True, "worker must still be awaiting the unfinished native response")
                self.assertEqual(started.get("submission"), "waiting")
                self.assertEqual(started["pid"], worker.pid)
                paused_records = self._get(port, "/datasets/dagster_fixture")
                processing = {
                    key: value for key, value in paused_records.items()
                    if isinstance(value, dict) and value.get("phase") == "processing"
                }
                self.assertTrue(processing, "processing must have durable phase evidence before worker death")
                self.assertNotIn("result/stable_build", paused_records)
                self.assertNotIn("DOCSPEC_TEST_WORK validation", self._tail(log_path))
                paused_files = self._retained_files(workspace)
                self.assertTrue(paused_files)
                self.assertIsNone(worker.poll(), self._tail(worker_log))
                # Include inspection/hashing in the interruption deadline. The
                # worker socket timeout is 60 seconds, so this cannot qualify a
                # worker that already timed out while Dagster finalized its job.
                self.assertLess(time.monotonic(), deadline)
                self.assertLess(time.monotonic() - started["submitted_at"], 30)
                self.assertEqual(json.loads(report.read_text()), started)
                worker.kill()
                worker.wait(timeout=10)
                self.assertEqual(worker.returncode, -signal.SIGKILL)
                self.assertEqual(json.loads(report.read_text()), started)
                source.unlink()
                if mode == "hostkill":
                    # Keep the resume marker absent until the actual native
                    # process is dead; validation cannot race host termination.
                    self.assertFalse((root / "native-resume").exists())
                    host.kill()
                    host.wait(timeout=10)
                    self.assertEqual(host.returncode, -signal.SIGKILL)
            finally:
                if worker.poll() is None:
                    worker.kill()
                    worker.wait(timeout=10)
                # Let outer graceful cleanup drain a live native request even
                # when an assertion fails while it is paused.
                (root / "native-resume").touch()

        if mode == "hostkill":
            environment = os.environ.copy()
            for name in ("DOCSPEC_TEST_PAUSE_BEFORE", "DOCSPEC_TEST_PAUSE_MARKER", "DOCSPEC_TEST_RESUME_MARKER"):
                environment.pop(name, None)
            host = self._host(binary, workspace, limits, port, log, environment)
            hosts.append(host)
            self._wait_ready(host, port, log_path)
        else:
            # Caller loss leaves the owned Rust task running. Let its exact
            # operation finish before qualifying replacement reconciliation;
            # hostkill separately proves cold recovery from unfinished work.
            deadline = time.monotonic() + 30
            while "result/stable_build" not in self._get(port, "/datasets/dagster_fixture"):
                self.assertIsNone(host.poll(), self._tail(log_path))
                self.assertLess(time.monotonic(), deadline, "surviving native operation did not complete")
                time.sleep(0.05)
        replacement = self._worker(root, config_path, url, "replacement")
        self.assertTrue(replacement["success"], self._tail(log_path))
        self.assertNotEqual(started["pid"], replacement["pid"])
        self.assertEqual(replacement["request_sha256"], started["request_sha256"])
        self.assertEqual(replacement["route"], started["route"])
        self.assertEqual(config_path.read_text(), original_config)
        baseline = self._state(port, workspace)
        for key, value in processing.items():
            self.assertEqual(baseline[1][key], value, "completed processing evidence must remain exact")
        for path, identity in paused_files.items():
            self.assertEqual(baseline[2].get(path), identity, "durable files must survive without rewrites")
        replay = self._worker(root, config_path, url, "replay")
        self.assertTrue(replay["success"], self._tail(log_path))
        self.assertEqual(replay["summary"], replacement["summary"])
        self.assertEqual(self._state(port, workspace), baseline)
        self.assertLessEqual(log_path.stat().st_size, 1 << 20)
        work = [line.removeprefix("DOCSPEC_TEST_WORK ") for line in log_path.read_text().splitlines()
                if line.startswith("DOCSPEC_TEST_WORK ")]
        for phase in ("processing", "validation", "readback"):
            self.assertEqual(work.count(phase), 1, f"{phase} must execute exactly once across recovery and replay: {work}")
        return host, baseline, replacement["summary"], replay["summary"]

    def _state(self, port, workspace):
        return (self._get(port, "/datasets/dagster_fixture/latest"),
                self._get(port, "/datasets/dagster_fixture"), self._retained_files(workspace))

    def _worker(self, root, config_path, url, name, timeout_seconds=60, environment=None):
        report = root / f"{name}-report.json"
        log = root / f"{name}-worker.log"
        with log.open("wb") as output:
            process = subprocess.Popen(
                [sys.executable, str(Path(__file__).with_name("native_worker.py")), str(config_path), url, str(report), str(timeout_seconds)],
                stdout=output, stderr=output, env=environment,
            )
            try:
                process.wait(timeout=timeout_seconds + 30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
                self.fail("Python worker exceeded its fixture deadline: " + self._tail(log))
            except BaseException:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
                raise
        self.assertEqual(process.returncode, 0, self._tail(log))
        self.assertTrue(report.is_file(), self._tail(log))
        self.assertLessEqual(report.stat().st_size, 4096)
        return json.loads(report.read_text())

    def _wait_ready(self, host, port, log_path):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            self.assertIsNone(host.poll(), self._tail(log_path))
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    self.assertIsNone(host.poll(), self._tail(log_path))
                    return
            except OSError:
                time.sleep(0.05)
        self.fail("Rust host did not bind its loopback listener: " + self._tail(log_path))

    def _get(self, port, path):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            raw = response.read((32 << 20) + 1)
            self.assertLessEqual(len(raw), 32 << 20)
            return json.loads(raw)
        finally:
            connection.close()

    @staticmethod
    def _retained_files(workspace):
        # Compare native files and retained objects, excluding scratch and the
        # SQLite runtime files. This catches repeated native writes even if the
        # latest application records contain unchanged logical values.
        inventory = {}
        total_bytes = 0
        for directory in (workspace / "warehouse", workspace / "objects"):
            if not directory.exists():
                continue
            for path in sorted(directory.rglob("*")):
                if path.is_file():
                    total_bytes += path.stat().st_size
                    if total_bytes > 64 << 20 or len(inventory) >= 2048:
                        raise AssertionError("tiny-fixture retained inventory exceeded its qualification bound")
                    digest = hashlib.sha256()
                    with path.open("rb") as stream:
                        for chunk in iter(lambda: stream.read(64 << 10), b""):
                            digest.update(chunk)
                    inventory[str(path.relative_to(workspace))] = (digest.hexdigest(), path.stat().st_size, path.stat().st_mtime_ns)
        return inventory

    @staticmethod
    def _tail(path):
        with path.open("rb") as stream:
            stream.seek(max(0, path.stat().st_size - 8192))
            return stream.read(8192).decode("utf-8", errors="replace")


@contextmanager
def lost_response_relay(native_port, capture_state):
    """Forward exactly two bounded requests; discard only the first real reply."""
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            connection = http.client.HTTPConnection("127.0.0.1", native_port, timeout=30)
            try:
                self.connection.settimeout(30)
                length = int(self.headers["Content-Length"])
                if not 0 < length <= 1 << 20 or len(self.server.requests) >= 2:
                    raise ValueError("relay request bound exceeded")
                payload = self.rfile.read(length)
                if len(payload) != length:
                    raise ValueError("short relay request")
                self.server.requests.append((self.path, payload))
                connection.request("POST", self.path, payload, {"Content-Type": "application/json"})
                response = connection.getresponse()
                raw = response.read((32 << 20) + 1)
                if response.status != 200 or len(raw) > 32 << 20:
                    raise ValueError("real Rust completion required within response bound")
                self.server.completions.append(json.loads(raw))
                if len(self.server.requests) == 1:
                    self.server.baseline = capture_state()
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except Exception as error:
                self.server.errors.append(repr(error))
                self.close_connection = True
            finally:
                connection.close()

        def log_message(self, *_args):
            pass

    relay = HTTPServer(("127.0.0.1", 0), Handler)
    relay.requests, relay.completions, relay.errors = [], [], []
    relay.baseline = None
    thread = threading.Thread(target=relay.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    try:
        yield relay
    finally:
        relay.shutdown()
        relay.server_close()
        thread.join(timeout=5)
        if thread.is_alive():
            raise AssertionError("lost-response relay did not stop")


if __name__ == "__main__":
    unittest.main()
