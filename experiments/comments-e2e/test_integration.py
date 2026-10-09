"""Optional complete synthetic CLI/readback check; never performance acceptance."""

import base64
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from integrity import file_digests
import run
import workload as w


@unittest.skipUnless(os.environ.get("DOCSPEC_BINARY"), "requires an already built Rust executable")
class CompleteFixture(unittest.TestCase):
    def test_complete_native_and_flat_flow_with_independent_oracle(self):
        binary = Path(os.environ["DOCSPEC_BINARY"]).resolve(strict=True)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            series = root / "series"
            series.mkdir()
            rows = [dict.fromkeys(w.COMMENTS) for _ in range(5)]
            for item, values in zip(rows, [
                ("same", "A", "Ω", ""), ("same", "A", "Ω", ""),
                ("unmatched", "B", None, "body"), ("null", None, "", None),
                ("last", "A", "full text", "nested text"),
            ]):
                item.update(zip(["comment_id", "docket_id", "comment", "text_content"], values))
                item["attachments_json"] = '[{"name":"raw","order":[2,1]}]'
            comments = pa.Table.from_pylist(rows, schema=pa.schema([pa.field(n, pa.string()) for n in w.COMMENTS]))
            dockets = pa.Table.from_pylist([
                {**dict.fromkeys(w.DOCKETS), "docket_id": "A", "title": "matched"},
                {**dict.fromkeys(w.DOCKETS), "docket_id": None, "title": "null must not match"},
            ], schema=pa.schema([pa.field(n, pa.string()) for n in w.DOCKETS]))
            receipts = pa.table({
                "record_id": ["r1", "r1", "r2"],
                "witnesses": pa.array([
                    [{"label": "Ω", "positions": [2, 1]}], [], None,
                ], type=pa.list_(pa.struct([
                    pa.field("label", pa.string()), pa.field("positions", pa.list_(pa.int64())),
                ]))),
            })
            selected = {"inputs": {}}
            for name, table in [("comments", comments), ("dockets", dockets), ("docket-receipts", receipts)]:
                path = root / (name + ".parquet")
                pq.write_table(table, path)
                selected["inputs"][name] = {"path": str(path), **file_digests(path), "rows": table.num_rows,
                    "schema_ipc_base64": base64.b64encode(table.schema.serialize()).decode()}
            # The only fixture population override is inside this explicit test
            # wrapper. Production CLI, input admission and acceptance remain 10M.
            oracle_script = root / "fixture_oracle.py"
            oracle_script.write_text(
                "import sys\n" + f"sys.path.insert(0, {str(w.HERE)!r})\n" +
                "import oracle\noracle.ROWS = 5\noracle.main()\n")
            limits = {"compute_threads": 2, "memory_bytes": 2 << 30, "scratch_bytes": 64 << 20,
                      "batch_rows": 2, "max_batch_bytes": 4 << 20, "max_metadata_bytes": 16 << 20}
            with patch.object(run, "LIMITS", limits):
                result = run.run_one(series, 1, selected, binary, 256 << 20, 1, oracle_path=oracle_script)
            self.assertTrue(result["passed"], result)
            self.assertEqual(len(result["stages"]), 16)
            proof = run.read(series / "run-01/oracle.stdout.json")
            self.assertTrue(proof["exact_readback_passed"])
            self.assertEqual((proof["comment_rows"], proof["matched_rows"], proof["unmatched_rows"]), (5, 3, 2))
            self.assertNotIn("full_local_performance_acceptance_passed", result)


if __name__ == "__main__":
    unittest.main()
