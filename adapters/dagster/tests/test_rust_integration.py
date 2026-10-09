"""Opt-in native Dagster execution against the real Rust HTTP host."""

import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import unittest

DAGSTER_AVAILABLE = importlib.util.find_spec("dagster") is not None
if DAGSTER_AVAILABLE:
    from docspec_dagster.definitions import DocSpecResource, docspec_job


@unittest.skipUnless(DAGSTER_AVAILABLE, "Dagster is required for real Rust integration")
class RustIntegrationTests(unittest.TestCase):
    def test_same_native_request_reconciles_without_more_retained_writes(self):
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
                host = subprocess.Popen(
                    [str(binary), "--workspace", str(workspace), "--limits", str(limits), "serve", "--bind", f"127.0.0.1:{port}"],
                    stdout=log,
                    stderr=log,
                )
                try:
                    self._wait_ready(host, port, log_path)
                    request_json = json.dumps({
                        "dataset": "dagster_fixture",
                        "operation": "stable_build",
                        "input": str(source),
                        "expected_digest": digest,
                        "sql": "SELECT * FROM input",
                        "definition": "dagster-fixture-v1",
                    })
                    config = {"ops": {"docspec_operation": {"config": {"route": "/run", "request_json": request_json}}}}
                    resource = DocSpecResource(base_url=f"http://127.0.0.1:{port}", uncertain_retries=0)

                    first = docspec_job.execute_in_process(run_config=config, resources={"docspec": resource}, raise_on_error=False)
                    self.assertTrue(first.success, self._tail(log_path))
                    first_summary = first.output_for_node("docspec_operation")
                    first_latest = self._get(port, "/datasets/dagster_fixture/latest")
                    first_records = self._get(port, "/datasets/dagster_fixture")
                    first_files = self._retained_files(workspace)
                    self.assertTrue(first_files, "the real host must retain native output")

                    second = docspec_job.execute_in_process(run_config=config, resources={"docspec": resource}, raise_on_error=False)
                    self.assertTrue(second.success, self._tail(log_path))
                    self.assertEqual(second.output_for_node("docspec_operation"), first_summary)
                    self.assertEqual(first_summary["result_id"], "stable_build")
                    self.assertEqual(first_latest["id"], "stable_build")
                    self.assertTrue(first_latest["verified"])
                    self.assertEqual(self._get(port, "/datasets/dagster_fixture/latest"), first_latest)
                    self.assertEqual(self._get(port, "/datasets/dagster_fixture"), first_records)
                    self.assertEqual(self._retained_files(workspace), first_files)
                finally:
                    if host.poll() is None:
                        host.send_signal(signal.SIGINT)
                        try:
                            host.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            host.kill()
                            host.wait(timeout=10)
                            self.fail("Rust host did not finish graceful shutdown: " + self._tail(log_path))
                    self.assertEqual(host.returncode, 0, self._tail(log_path))

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
        for directory in (workspace / "warehouse", workspace / "objects"):
            if not directory.exists():
                continue
            for path in sorted(directory.rglob("*")):
                if path.is_file():
                    digest = hashlib.sha256()
                    with path.open("rb") as stream:
                        for chunk in iter(lambda: stream.read(64 << 10), b""):
                            digest.update(chunk)
                    inventory[str(path.relative_to(workspace))] = digest.hexdigest()
        return inventory

    @staticmethod
    def _tail(path):
        with path.open("rb") as stream:
            stream.seek(max(0, path.stat().st_size - 8192))
            return stream.read(8192).decode("utf-8", errors="replace")


if __name__ == "__main__":
    unittest.main()
