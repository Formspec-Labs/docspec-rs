"""Run the frozen local probe against retained, digest-verified workspace assets."""

import collections
import hashlib
import json
import platform
import re
import subprocess
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq


REPO = Path(__file__).resolve().parents[2]
SOURCE = REPO.parent / "experiments/docspec-spicyregs-performance-20261007/inputs/bill-actions"
BINARY = REPO / "target/debug/docspec"
OUTPUT = REPO / "experiments/local-reuse" / time.strftime("run-%Y%m%dT%H%M%S")


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1 << 20), b""):
            digest.update(data)
    return digest.hexdigest()


def inventory(root):
    return {str(p.relative_to(root)): {"size": p.stat().st_size, "mtime_ns": p.stat().st_mtime_ns}
            for p in sorted(root.rglob("*")) if p.is_file()}


def command(directory, label, *arguments):
    stdout = directory / f"{label}.stdout.json"
    stderr = directory / f"{label}.stderr.log"
    timing = directory / f"{label}.time.log"
    argv = [str(BINARY), *map(str, arguments)]
    (directory / f"{label}.argv.json").write_text(json.dumps(argv, indent=2) + "\n")
    start = time.monotonic()
    with stdout.open("w") as out, stderr.open("w") as err:
        result = subprocess.run(["/usr/bin/time", "-l", "-o", str(timing), *argv],
                                stdout=out, stderr=err, timeout=120, check=False)
    elapsed = time.monotonic() - start
    metrics = {"elapsed_seconds": elapsed, "exit_code": result.returncode}
    match = re.search(r"(\d+)\s+maximum resident set size", timing.read_text())
    metrics["peak_rss_bytes"] = int(match.group(1)) if match else None
    (directory / f"{label}.metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    if result.returncode:
        raise RuntimeError(f"{label} failed: see {stderr}")
    return json.loads(stdout.read_text()) if stdout.stat().st_size else None, metrics


def rows(table):
    return collections.Counter(json.dumps(row, sort_keys=True, ensure_ascii=False)
                               for row in table.to_pylist())


def independent_check(directory, workspace, source, release_path, label):
    start = time.monotonic()
    output = directory / f"{label}.arrow"
    command(directory, label, "--workspace", workspace, "read", release_path, output)
    original = pq.read_table(source)
    with pa.memory_map(str(output), "r") as stream:
        decoded = ipc.open_stream(stream).read_all()
    # Iceberg field-id metadata is expected to differ. Types and nullability must agree.
    assert original.schema.equals(decoded.schema, check_metadata=False), (original.schema, decoded.schema)
    assert rows(original) == rows(decoded), "exact values or multiplicity differ"
    return {"rows": len(original), "schema_and_values_equal": True,
            "elapsed_seconds": time.monotonic() - start}


def main():
    OUTPUT.mkdir()
    members = json.loads((SOURCE / "members.json").read_text())["members"]
    for member in members:
        assert sha(SOURCE / member["objectKey"]) == member["sha256"].removeprefix("sha256:")
    environment = {"platform": platform.platform(), "python": sys.version, "pyarrow": pa.__version__,
                   "rustc": subprocess.check_output(["rustc", "--version"], cwd=REPO, text=True).strip(),
                   "source_manifest_sha256": sha(SOURCE / "members.json"),
                   "cache": "not cleared; inputs may be cached; ordinary workstation background load",
                   "digests": {name: sha(path) for name, path in {
                       "binary": BINARY, "lock": REPO / "Cargo.lock", "harness": Path(__file__),
                       "protocol": Path(__file__).with_name("protocol.md")}.items()}}
    (OUTPUT / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    results = []
    for member in members:
        source = SOURCE / member["objectKey"]
        for repetition in range(3):
            directory = OUTPUT / f"{source.stem}-{repetition + 1}"
            directory.mkdir()
            workspace = directory / "workspace"
            case = {"case": source.stem, "repetition": repetition + 1}
            try:
                identity, _ = command(directory, "hash", "hash", source)
                spec = {"dataset": source.stem, "operation": "fresh", "input": str(source),
                        "expected_digest": identity["digest"], "sql": "SELECT * FROM input", "definition": "probe-v1"}
                spec_path = directory / "request.json"
                spec_path.write_text(json.dumps(spec, indent=2) + "\n")
                first, case["fresh"] = command(directory, "fresh", "--workspace", workspace, "run", spec_path)
                spec["operation"] = "unchanged"
                unchanged_path = directory / "unchanged-request.json"
                unchanged_path.write_text(json.dumps(spec, indent=2) + "\n")
                second, case["unchanged"] = command(directory, "unchanged", "--workspace", workspace, "run", unchanged_path)
                assert first["dataset"]["groups"] == second["dataset"]["groups"]
                assert first["dataset"]["inputs"] == second["dataset"]["inputs"]
                before = inventory(workspace)
                third, case["retry"] = command(directory, "retry", "--workspace", workspace, "run", unchanged_path)
                assert second == third
                assert before == inventory(workspace), "completed retry mutated retained files"
                (directory / "inventory.json").write_text(json.dumps(before, indent=2) + "\n")
                case["independent_check"] = independent_check(directory, workspace, source,
                                                               directory / "unchanged.stdout.json", "read")
                case["source_unchanged"] = sha(source) == member["sha256"].removeprefix("sha256:")
                assert case["source_unchanged"]
                assert all(case[arm]["peak_rss_bytes"] < 24 << 30 for arm in ["fresh", "unchanged", "retry"])
                case["passed"] = True
            except Exception as error:
                case["passed"] = False
                case["failure"] = repr(error)
            results.append(case)
            (OUTPUT / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            print(json.dumps(case), flush=True)
            if not case["passed"]:
                break
    print(f"Artifacts: {OUTPUT}")
    return 0 if all(case["passed"] for case in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
