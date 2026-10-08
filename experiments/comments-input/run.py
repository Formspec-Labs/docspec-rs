"""Run the frozen Comments pilot, retaining measurements and failed workspaces."""

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psutil


REPO = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
BINARY = REPO / "target/release/docspec"
GIB = 1 << 30
LIMITS = {"compute_threads": 12, "memory_bytes": 24 * GIB,
          "scratch_bytes": 8 * GIB, "batch_rows": 4096,
          "max_batch_bytes": 64 << 20, "max_metadata_bytes": 32 << 20}


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def inventory(root):
    """Metadata only; paths are unique, while APFS extents may still be shared."""
    files = {}
    for directory, _, names in os.walk(root):
        for name in names:
            path = Path(directory) / name
            try:
                stat = path.stat()
            except FileNotFoundError:  # Native spill/SQLite files can disappear.
                continue
            files[str(path.relative_to(root))] = {
                "bytes": stat.st_size, "allocated_bytes": stat.st_blocks * 512,
                "mtime_ns": stat.st_mtime_ns}
    return files


def totals(files):
    result = {}
    for name, stat in files.items():
        path = Path(name)
        if path.parts[0] == "scratch":
            category = "scratch"
        elif path.parts[0] == "warehouse":
            category = "lifecycle"
        elif path.parts[0] == "objects":
            if path.parent.name == "data" and path.suffix == ".parquet":
                category = "native_data"
            elif path.suffix == ".parquet":
                category = "admitted_or_prepared_data"
            else:
                category = "object_metadata"
        else:
            category = "catalog_and_other"
        for group in (category, "total"):
            counts = result.setdefault(group, {"files": 0, "bytes": 0, "allocated_bytes": 0})
            counts["files"] += 1
            for key in ("bytes", "allocated_bytes"):
                counts[key] += stat[key]
    return result


def tree_rss(pid):
    try:
        parent = psutil.Process(pid)
        processes = [parent, *parent.children(recursive=True)]
    except psutil.NoSuchProcess:
        return 0
    total = 0
    for process in processes:
        try:
            total += process.memory_info().rss
        except psutil.NoSuchProcess:
            pass
    return total


def measured_totals(workspace):
    start = time.monotonic()
    usage = totals(inventory(workspace))
    return usage, time.monotonic() - start


def command(directory, workspace, label, argv, disk_bound):
    """Same raw command/time evidence as local-reuse, plus bounded live sampling."""
    save(directory / f"{label}.argv.json", argv)
    before = inventory(workspace)
    save(directory / f"{label}.before.json", before)
    metrics = {"sampled_peak_tree_rss_bytes": 0, "min_volume_free_bytes": shutil.disk_usage(REPO).free,
               "sampled_peak_workspace_bytes": 0, "sampled_peak_allocated_bytes": 0,
               "sampled_peak_scratch_bytes": 0, "inventory_sampling_seconds": 0.0,
               "before": totals(before), "stop_reason": None}
    timing = directory / f"{label}.time.log"
    with (directory / f"{label}.stdout.json").open("w") as out, \
            (directory / f"{label}.stderr.log").open("w") as err, \
            (directory / f"{label}.samples.jsonl").open("w") as samples:
        start = time.monotonic()
        process = subprocess.Popen(["/usr/bin/time", "-l", "-o", str(timing), *argv],
                                   stdout=out, stderr=err, cwd=REPO, start_new_session=True)
        sampler = ThreadPoolExecutor(max_workers=1)
        pending_inventory = None
        next_inventory = 0.0
        try:
            while True:
                elapsed = time.monotonic() - start
                rss = tree_rss(process.pid)
                free = shutil.disk_usage(REPO).free
                sample = {"seconds": elapsed, "tree_rss_bytes": rss, "volume_free_bytes": free}
                metrics["sampled_peak_tree_rss_bytes"] = max(metrics["sampled_peak_tree_rss_bytes"], rss)
                metrics["min_volume_free_bytes"] = min(metrics["min_volume_free_bytes"], free)
                if pending_inventory is not None and pending_inventory.done():
                    usage, scan_seconds = pending_inventory.result()
                    metrics["inventory_sampling_seconds"] += scan_seconds
                    sample["workspace"] = usage
                    for metric, group, key in [("sampled_peak_workspace_bytes", "total", "bytes"),
                                                ("sampled_peak_allocated_bytes", "total", "allocated_bytes"),
                                                ("sampled_peak_scratch_bytes", "scratch", "bytes")]:
                        metrics[metric] = max(metrics[metric], usage.get(group, {}).get(key, 0))
                    pending_inventory = None
                if pending_inventory is None and elapsed >= next_inventory:
                    pending_inventory = sampler.submit(measured_totals, workspace)
                    next_inventory = elapsed + 5
                samples.write(json.dumps(sample) + "\n")
                samples.flush()
                if elapsed > 1800:
                    metrics["stop_reason"] = "1800-second deadline"
                elif rss > LIMITS["memory_bytes"]:
                    metrics["stop_reason"] = "sampled process-tree RSS exceeded 24 GiB"
                elif free < 2 * GIB:
                    metrics["stop_reason"] = "less than 2 GiB volume free space"
                elif metrics["sampled_peak_scratch_bytes"] > LIMITS["scratch_bytes"]:
                    metrics["stop_reason"] = "sampled scratch exceeded 8 GiB"
                elif metrics["sampled_peak_workspace_bytes"] > disk_bound:
                    metrics["stop_reason"] = "sampled workspace exceeded preflight allowance"
                if metrics["stop_reason"]:
                    break
                try:
                    process.wait(timeout=1)
                    break
                except subprocess.TimeoutExpired:
                    pass
        finally:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
            # The time wrapper may exit before a child finishes handling TERM.
            # Reap the entire private group even when the wrapper has exited.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            metrics.update(elapsed_seconds=time.monotonic() - start, exit_code=process.returncode)
            sampler.shutdown(wait=True)
            if pending_inventory is not None:
                usage, scan_seconds = pending_inventory.result()
                metrics["inventory_sampling_seconds"] += scan_seconds
                samples.write(json.dumps({"seconds": time.monotonic() - start, "workspace": usage}) + "\n")
                for metric, group, key in [("sampled_peak_workspace_bytes", "total", "bytes"),
                                            ("sampled_peak_allocated_bytes", "total", "allocated_bytes"),
                                            ("sampled_peak_scratch_bytes", "scratch", "bytes")]:
                    metrics[metric] = max(metrics[metric], usage.get(group, {}).get(key, 0))
            after = inventory(workspace)
            metrics["after"] = totals(after)
            metrics["volume_free_after_bytes"] = shutil.disk_usage(REPO).free
            for metric, group, key in [("sampled_peak_workspace_bytes", "total", "bytes"),
                                        ("sampled_peak_allocated_bytes", "total", "allocated_bytes"),
                                        ("sampled_peak_scratch_bytes", "scratch", "bytes")]:
                metrics[metric] = max(metrics[metric], metrics["after"].get(group, {}).get(key, 0))
            if timing.exists():
                match = re.search(r"(\d+)\s+maximum resident set size", timing.read_text())
                metrics["time_max_rss_bytes"] = int(match.group(1)) if match else None
            if not metrics["stop_reason"]:
                if metrics["elapsed_seconds"] > 1800:
                    metrics["stop_reason"] = "1800-second deadline"
                elif (metrics.get("time_max_rss_bytes") or 0) > LIMITS["memory_bytes"]:
                    metrics["stop_reason"] = "reported maximum RSS exceeded 24 GiB"
                elif metrics["sampled_peak_workspace_bytes"] > disk_bound:
                    metrics["stop_reason"] = "workspace exceeded preflight allowance"
                elif metrics["sampled_peak_scratch_bytes"] > LIMITS["scratch_bytes"]:
                    metrics["stop_reason"] = "scratch exceeded 8 GiB"
                elif metrics["volume_free_after_bytes"] < 2 * GIB:
                    metrics["stop_reason"] = "less than 2 GiB volume free space after command"
            save(directory / f"{label}.after.json", after)
            save(directory / f"{label}.metrics.json", metrics)
    print(json.dumps({"arm": label, **metrics}), flush=True)
    if process.returncode or metrics["stop_reason"]:
        raise RuntimeError(f"{label} failed: see retained logs and metrics")
    return json.loads((directory / f"{label}.stdout.json").read_text()), metrics


def independent_readback(release_path, source, limits=None):
    import pyarrow.parquet as pq
    from pyiceberg.table import StaticTable

    limits = LIMITS if limits is None else limits

    release = json.loads(release_path.read_text())
    assert release["verified"]
    dataset = release["dataset"]
    manifest = Path(release["manifest"]["path"])
    assert manifest.stat().st_size == release["manifest"]["bytes"]
    assert json.loads(manifest.read_text()) == dataset
    pin = dataset["groups"]["source"]
    native = json.loads(Path(pin["metadata"]["path"]).read_text())
    assert native["current-snapshot-id"] == pin["snapshot"]
    assert native["current-schema-id"] == pin["schema_id"]
    table = StaticTable.from_metadata(pin["metadata"]["path"])
    objects = {obj["path"]: obj for obj in pin["objects"]}
    live = set()
    schema = pq.read_schema(source)
    row_count = sample_rows = sample_bytes = file_count = 0
    for task in table.scan(snapshot_id=pin["snapshot"]).plan_files():
        assert not task.delete_files, "this source-build probe expects no delete files"
        path = Path(task.file.file_path)
        assert str(path) not in live
        live.add(str(path))
        assert path.stat().st_size == task.file.file_size_in_bytes == objects[str(path)]["bytes"]
        with pq.ParquetFile(path) as parquet:
            assert schema.equals(parquet.schema_arrow, check_metadata=False)
            assert parquet.metadata.num_rows == task.file.record_count
            row_count += parquet.metadata.num_rows
            if file_count < 3:
                batch = next(parquet.iter_batches(batch_size=8, use_threads=False), None)
                if batch is not None:
                    batch.validate(full=True)
                    sample_rows += batch.num_rows
                    sample_bytes += batch.nbytes
                    assert batch.nbytes <= limits["max_batch_bytes"]
            file_count += 1
    assert live == {name for name in objects if name.endswith(".parquet")}
    assert row_count == pin["rows"] == 10_000_000
    print(json.dumps({"rows": row_count, "data_files": file_count, "sample_rows": sample_rows,
                      "sample_bytes": sample_bytes, "shape_types_counts_and_sample_passed": True}))


def main():
    preparation = json.loads((HERE / "preparation-20261008.json").read_text())
    source = Path(preparation["path"])
    assert source.stat().st_size == preparation["identity"]["bytes"]
    directory = HERE / time.strftime("run-%Y%m%dT%H%M%S")
    directory.mkdir()
    workspace = directory / "workspace"
    workspace.mkdir()
    save(directory / "limits.json", LIMITS)
    disk_bound = source.stat().st_size * 3 + LIMITS["scratch_bytes"]
    environment = {"platform": platform.platform(), "python": sys.version,
                   "libraries": {name: importlib.metadata.version(name) for name in ("psutil", "pyarrow", "pyiceberg")},
                   "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
                   "worktree_status": subprocess.check_output(["git", "status", "--short"], cwd=REPO, text=True),
                   "cpu_count": os.cpu_count(), "load_average": os.getloadavg(),
                   "source_mtime_ns": source.stat().st_mtime_ns,
                   "physical_memory_bytes": psutil.virtual_memory().total,
                   "volume_free_bytes": shutil.disk_usage(REPO).free, "workspace_stop_bytes": disk_bound,
                   "cache": "not cleared; input may be warm; ordinary workstation background load",
                   "sampling": {"rss_free_space_seconds": 1, "workspace_seconds": 5,
                                "peak_scope": "sampled lower bounds; short-lived peaks may be missed"},
                   "digests": {name: sha(path) for name, path in {
                       "binary": BINARY, "lock": REPO / "Cargo.lock", "harness": Path(__file__),
                       "protocol": HERE / "protocol.md", "preparation": HERE / "preparation-20261008.json"}.items()}}
    save(directory / "environment.json", environment)
    result = {"bounded_checks_passed": False, "implemented_slice_only": True,
              "payload_read_avoidance_measured": False, "full_performance_acceptance_passed": False}
    print(f"Artifacts: {directory}", flush=True)
    try:
        assert environment["volume_free_bytes"] >= disk_bound + 2 * GIB, "preflight leaves less than 2 GiB headroom"
        prefix = [str(BINARY), "--workspace", str(workspace), "--limits", str(directory / "limits.json")]
        spec = {"dataset": "comments", "operation": "fresh", "input": str(source),
                "expected_digest": preparation["identity"]["digest"],
                "sql": "SELECT * FROM input", "definition": "comments-probe-v1"}
        save(directory / "request.json", spec)
        fresh, result["fresh"] = command(directory, workspace, "fresh", [*prefix, "run", str(directory / "request.json")], disk_bound)
        latest, result["selection"] = command(directory, workspace, "selection", [*prefix, "latest", "comments"], disk_bound)
        assert latest == fresh, "catalog selection differs from returned release"
        result["readback"], result["readback_metrics"] = command(
            directory, workspace, "readback", [sys.executable, str(Path(__file__)), "readback",
                                             str(directory / "fresh.stdout.json"), str(source)], disk_bound)
        result["fresh_with_readback_seconds"] = sum(result[name]["elapsed_seconds"] for name in ("fresh", "selection", "readback_metrics"))
        assert result["fresh_with_readback_seconds"] <= 1800
        spec["operation"] = "unchanged"
        save(directory / "unchanged-request.json", spec)
        second, result["unchanged"] = command(directory, workspace, "unchanged", [*prefix, "run", str(directory / "unchanged-request.json")], disk_bound)
        assert fresh["dataset"]["groups"] == second["dataset"]["groups"], "unchanged input rewrote native tables"
        assert fresh["dataset"]["inputs"] == second["dataset"]["inputs"], "unchanged operation copied input"
        before = inventory(workspace)
        third, result["retry"] = command(directory, workspace, "retry", [*prefix, "run", str(directory / "unchanged-request.json")], disk_bound)
        assert second == third, "retry did not return original release"
        assert before == inventory(workspace), "completed retry mutated retained files"
        result["bounded_checks_passed"] = True
    except Exception as error:
        result["failure"] = repr(error)
    result["final_workspace"] = totals(inventory(workspace))
    result["retained_amplification_vs_input"] = result["final_workspace"].get("total", {}).get("bytes", 0) / source.stat().st_size
    save(directory / "results.json", result)
    print(json.dumps(result), flush=True)
    return 0 if result["bounded_checks_passed"] else 1


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "readback":
        read_limits = json.loads(Path(sys.argv[4]).read_text()) if len(sys.argv) > 4 else None
        independent_readback(Path(sys.argv[2]), Path(sys.argv[3]), read_limits)
    else:
        raise SystemExit(main())
