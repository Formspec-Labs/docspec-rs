"""Synthetic harness checks only; no corpus reads or performance claims."""

import base64
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

import integrity
from integrity import copy_frozen_file, file_digests
import oracle
import run
import workload as w


class QualificationHarnessTests(unittest.TestCase):
    def test_captured_subprocesses_survive_replaced_originals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original, captured = root / "original", root / "captured"
            original.mkdir()
            captured.mkdir()
            sources = {"docspec": "#!/bin/sh\nprintf 'old executable'\n",
                       "oracle.py": "from workload import VALUE\nfrom integrity import SUFFIX\nprint(VALUE + SUFFIX)\n",
                       "workload.py": "VALUE = 'old workload'\n",
                       "integrity.py": "SUFFIX = ' and integrity'\n"}
            for filename, source in sources.items():
                path = original / filename
                path.write_text(source)
                if filename == "docspec":
                    path.chmod(0o755)
                identity = copy_frozen_file(path, captured / filename)
                self.assertEqual(identity["bytes"], len(source.encode()))
                self.assertEqual(identity["source_stat"]["ino"], path.stat().st_ino)
                self.assertEqual((captured / filename).stat().st_mode & 0o222, 0)
                path.unlink()
                path.write_text("raise RuntimeError('replacement must not run')\n")
            self.assertEqual(subprocess.check_output([str(captured / "docspec")], text=True), "old executable")
            self.assertEqual(subprocess.check_output(
                [sys.executable, "-B", str(captured / "oracle.py")], text=True), "old workload and integrity\n")

    def test_capture_refuses_observed_source_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.write_bytes(b"original")
            real_fstat = os.fstat
            calls = 0

            def mutate_at_final_stat(descriptor):
                nonlocal calls
                calls += 1
                if calls == 2:
                    source.write_bytes(b"changed after reading")
                return real_fstat(descriptor)

            with mock.patch.object(integrity.os, "fstat", side_effect=mutate_at_final_stat):
                with self.assertRaisesRegex(ValueError, "source changed during capture"):
                    copy_frozen_file(source, root / "retained-failed-copy")
            self.assertTrue((root / "retained-failed-copy").exists())

    def test_oracle_requires_retained_bytes_even_when_external_fixture_survives(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "workspace"
            workspace.mkdir()
            external = root / "external.parquet"
            admitted_path = workspace / "admitted.parquet"
            table = pa.table({"value": ["same", None, ""]}).replace_schema_metadata({b"marker": b"one"})
            pq.write_table(table, external)
            shutil.copyfile(external, admitted_path)
            identity = file_digests(external)
            selected = {"path": str(external), **identity, "rows": 3,
                        "schema_ipc_base64": base64.b64encode(table.schema.serialize()).decode()}
            admitted = {"path": str(admitted_path), "bytes": identity["bytes"], "digest": identity["blake3"]}
            checker = oracle.Oracle(root)
            try:
                checker.input("good", selected, admitted)
                admitted_path.unlink()
                self.assertTrue(external.exists())
                with self.assertRaises(FileNotFoundError):
                    checker.input("missing", selected, admitted)
                # Only retained file metadata bytes change: values, types and
                # length remain equal, so a decoded-only oracle would miss this.
                pq.write_table(table.replace_schema_metadata({b"marker": b"two"}), admitted_path)
                self.assertEqual(admitted_path.stat().st_size, identity["bytes"])
                self.assertTrue(pq.read_table(admitted_path).equals(table, check_metadata=False))
                with self.assertRaisesRegex(ValueError, "admitted input bytes differ"):
                    checker.input("corrupt", selected, admitted)
            finally:
                checker.connection.close()

    def test_oracle_reconstructs_from_retained_input_without_external_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "workspace").mkdir()
            retained = root / "workspace/input.parquet"
            table = pa.table({"value": ["retained"]})
            pq.write_table(table, retained)
            identity = file_digests(retained)
            selected = {"path": str(root / "absent-external.parquet"), **identity, "rows": 1,
                        "schema_ipc_base64": base64.b64encode(table.schema.serialize()).decode()}
            admitted = {"path": str(retained), "bytes": identity["bytes"], "digest": identity["blake3"]}
            checker = oracle.Oracle(root)
            try:
                checker.input("retained", selected, admitted)
                self.assertEqual(checker.connection.execute('SELECT value FROM retained').fetchall(), [("retained",)])
            finally:
                checker.connection.close()

    def test_flat_export_rejects_changed_field_ids_and_pin_mapping(self):
        native = pa.schema([pa.field("value", pa.int64(), metadata={b"PARQUET:field_id": b"7"})])
        description = [{"id": 7, "name": "value", "data_type": "Int64", "nullable": True}]
        oracle.flat_schema(native, native, description, ["value"], "correct")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "export.parquet"
            changed = pa.schema([pa.field("value", pa.int64(), metadata={b"PARQUET:field_id": b"8"})])
            table = pa.Table.from_arrays([pa.array([1, None, 1], type=pa.int64())], schema=changed)
            pq.write_table(table, path)
            actual = pq.ParquetFile(path).schema_arrow
            oracle.same_schema(actual, native, "unchanged values/types")
            self.assertEqual(pq.read_table(path).to_pylist(), [{"value": 1}, {"value": None}, {"value": 1}])
            with self.assertRaisesRegex(ValueError, "exported field ID"):
                oracle.flat_schema(actual, native, description, ["value"], "changed ID")
        for change in [{"id": 8}, {"name": "other"}, {"data_type": "Int32"}, {"nullable": False}]:
            with self.assertRaisesRegex(ValueError, "ExportPin.fields"):
                oracle.flat_schema(native, native, [{**description[0], **change}], ["value"], "changed pin")

    def test_comparison_preserves_duplicates_nulls_empty_and_nested_order(self):
        con = duckdb.connect()
        try:
            con.execute("CREATE TABLE expected AS SELECT * FROM (VALUES "
                        "(NULL::VARCHAR, [1,2]), ('', [2,1]), ('repeat', [1,2]), "
                        "('repeat', [1,2])) t(value, nested)")
            con.execute("CREATE TABLE actual AS SELECT * FROM expected")
            oracle.exact_equal(con, "expected", "actual", ["value", "nested"], "equal")
            for sql in [
                "CREATE OR REPLACE TABLE actual AS SELECT DISTINCT * FROM expected",
                "CREATE OR REPLACE TABLE actual AS SELECT coalesce(value, '') AS value, nested FROM expected",
                "CREATE OR REPLACE TABLE actual AS SELECT value, list_sort(nested) AS nested FROM expected",
            ]:
                con.execute(sql)
                with self.assertRaises(ValueError):
                    oracle.exact_equal(con, "expected", "actual", ["value", "nested"], "changed")
        finally:
            con.close()

    def test_schema_check_precedes_possible_sql_coercion(self):
        a = pa.schema([pa.field("number", pa.int64(), nullable=False)])
        for b in [pa.schema([pa.field("number", pa.int32(), nullable=False)]),
                  pa.schema([pa.field("number", pa.int64(), nullable=True)])]:
            with self.assertRaises(ValueError):
                oracle.same_schema(a, b, "typed")

    def test_combined_scratch_includes_oracle_and_engine(self):
        sample = {name: {"bytes": size, "allocated_bytes": size, "mtime_ns": 0}
                  for name, size in [("run-01/workspace/scratch/a", 10),
                                     ("run-01/oracle-scratch/b", 20),
                                     ("run-01/workspace/objects/data.parquet", 30)]}
        totals = run.monitor.totals(sample, ("run-01/workspace/scratch", "run-01/oracle-scratch"))
        self.assertEqual(totals["scratch"]["bytes"], 30)
        self.assertEqual(totals["total"]["bytes"], 60)
        legacy = run.monitor.totals({"scratch/a": sample["run-01/workspace/scratch/a"]})
        self.assertEqual(legacy["scratch"]["bytes"], 10)

    def test_specs_keep_full_values_and_bind_receipts_without_a_fake_join(self):
        releases = {name: {"id": "initial", "dataset": {"dataset": name},
                           "manifest": {"path": name + ".json", "digest": "d", "bytes": 1}}
                    for name in ["comments", "dockets", "docket-receipts"]}
        request = w.join(releases)
        fields = request["transform"]["join"]["fields"]
        self.assertEqual([f["field"] for f in fields if f["side"] == "left"], w.COMMENTS + w.LENGTHS)
        self.assertEqual(request["transform"]["right"], w.version(releases["dockets"]))
        self.assertEqual(set(w.seal(releases)["members"]), {"dockets", "docket-receipts"})
        self.assertEqual(w.source("docket-receipts", {"path": "receipt.parquet", "blake3": "d"})["sql"],
                         "SELECT * FROM input")


if __name__ == "__main__":
    unittest.main()
