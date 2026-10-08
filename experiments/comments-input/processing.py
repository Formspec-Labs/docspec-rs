"""Collect the frozen batch/codec diagnostics with the existing bounded monitor."""

import itertools
import json
import platform
import subprocess
import sys
import time
from pathlib import Path

from run import GIB, HERE, REPO, command, inventory, save, sha, totals


def compare(source, output):
    import pyarrow.parquet as pq

    rows = 0
    with pq.ParquetFile(source) as before, pq.ParquetFile(output) as after:
        assert before.schema_arrow.equals(after.schema_arrow, check_metadata=True)
        assert before.metadata.num_rows == after.metadata.num_rows
        for left, right in itertools.zip_longest(
                before.iter_batches(batch_size=4096, use_threads=False),
                after.iter_batches(batch_size=4096, use_threads=False)):
            assert left is not None and right is not None
            assert left.equals(right, check_metadata=True), "ordered values or schema differ"
            rows += left.num_rows
    print(json.dumps({"exact_ordered_values_equal": True, "rows": rows}), flush=True)


def main():
    directory = HERE / time.strftime("processing-%Y%m%dT%H%M%S")
    directory.mkdir()
    workspace = directory / "workspace"
    workspace.mkdir()
    prepared = json.loads((HERE / "preparation-20261008.json").read_text())
    failure = HERE / "run-20261008T103609"
    inspection = json.loads((HERE / "admitted-input-inspect.json").read_text())
    admissions = [value for key, value in inspection.items()
                  if key.startswith("phase/") and value.get("phase") == "admission"]
    assert len(admissions) == 1 and len(admissions[0]["outputs"]) == 1
    admitted = admissions[0]["outputs"][0]
    assert admitted["digest"] == prepared["identity"]["digest"]
    assert admitted["bytes"] == prepared["identity"]["bytes"]
    source = Path(admitted["path"])
    completed = [item for item in json.loads((failure / "partial-output-diagnosis.json").read_text())
                 if item["file"].startswith("objects/") and item["footer_readable"]]
    assert len(completed) == 1
    codec_input = failure / "workspace" / completed[0]["file"]
    scan = REPO / "target/release/examples/comments_scan"
    codec = REPO / "target/release/examples/comments_codec"
    save(directory / "environment.json", {
        "platform": platform.platform(), "python": sys.version,
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        "status": subprocess.check_output(["git", "status", "--short"], cwd=REPO, text=True),
        "input": admitted, "identity_basis": "Retained immutable object qualified by completed admission",
        "input_mtime_ns": source.stat().st_mtime_ns,
        "codec_input": str(codec_input), "codec_input_sha256": sha(codec_input),
        "codec_input_bytes": codec_input.stat().st_size,
        "cache": "not cleared; retained inputs may be warm; one diagnostic per arm",
        "digests": {name: sha(path) for name, path in {"scan": scan, "codec": codec,
            "driver": Path(__file__), "monitor": HERE / "run.py", "lock": REPO / "Cargo.lock",
            "admitted_inspection": HERE / "admitted-input-inspect.json",
            "protocol": HERE / "processing-protocol.md"}.items()}})
    results = {"diagnostics_complete": False, "full_performance_acceptance_passed": False}
    print(f"Artifacts: {directory}", flush=True)
    try:
        assert source.stat().st_size == prepared["identity"]["bytes"]
        # These diagnostic workspaces contain no admitted full-input copy.
        disk_bound = 2 * GIB
        for batch_rows, views in ((4096, True), (512, True), (4096, False), (512, False)):
            label = f"scan-{batch_rows}-views-{str(views).lower()}"
            value, metrics = command(directory, workspace, label,
                [str(scan), str(source), str(batch_rows), str(views).lower()], disk_bound)
            assert value["rows"] == 10_000_000
            results[label] = {"rows": value["rows"], "oversized_batches": value["oversized_batches"],
                "max_array_bytes": value["max_array_bytes"],
                "largest_oversized_row_bytes": value["max_row_value_bytes_in_oversized_batches"],
                "largest_oversized_value_bytes": value["max_value_bytes_in_oversized_batches"],
                "metrics": metrics}
            save(directory / "results.json", results)
        for encoding in ("none", "snappy", "zstd"):
            target = workspace / f"{encoding}.parquet"
            value, metrics = command(directory, workspace, encoding,
                [str(codec), str(codec_input), str(target), encoding], disk_bound)
            check, readback = command(directory, workspace, f"{encoding}-readback",
                [sys.executable, str(Path(__file__)), "compare", str(codec_input), str(target)], disk_bound)
            assert check["rows"] == value["rows"] == completed[0]["rows"]
            results[encoding] = {"output": value, "metrics": metrics, "readback": check,
                                 "readback_metrics": readback}
            save(directory / "results.json", results)
        results["diagnostics_complete"] = True
    except Exception as error:
        results["failure"] = repr(error)
    results["final_workspace"] = totals(inventory(workspace))
    save(directory / "results.json", results)
    print(json.dumps(results), flush=True)
    return 0 if results["diagnostics_complete"] else 1


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "compare":
        compare(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        raise SystemExit(main())
