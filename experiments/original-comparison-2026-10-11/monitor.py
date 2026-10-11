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


def totals(files, scratch_prefixes=("scratch",)):
    result = {}
    for name, stat in files.items():
        path = Path(name)
        if any(path.parts[:len(Path(prefix).parts)] == Path(prefix).parts
               for prefix in scratch_prefixes):
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


def measured_totals(workspace, scratch_prefixes=("scratch",)):
    start = time.monotonic()
    usage = totals(inventory(workspace), scratch_prefixes)
    return usage, time.monotonic() - start


def signal_group(pid, sig):
    """Darwin may return EPERM for an already empty process group."""
    try:
        os.killpg(pid, sig)
    except PermissionError:
        # Accept only an independently observed empty group; never hide live work.
        for process in psutil.process_iter(["pid"]):
            try:
                if os.getpgid(process.pid) == pid:
                    raise RuntimeError(f"cannot signal live benchmark group {pid}")
            except ProcessLookupError:
                continue
        # No process belongs to the just-reaped private group.


def command(directory, workspace, label, argv, disk_bound, *, limits=None,
            timeout_seconds=1800, process_tree_root=None,
            scratch_prefixes=("scratch",), progress_seconds=None, deadline_monotonic=None,
            json_output=True):
    """Retain raw command/time evidence and sampled bounds; optionally decode JSON."""
    limits = LIMITS if limits is None else limits
    if timeout_seconds <= 0:
        raise RuntimeError(f"{label}: no remaining time in the run deadline")
    save(directory / f"{label}.argv.json", argv)
    before = inventory(workspace)
    save(directory / f"{label}.before.json", before)
    metrics = {"sampled_peak_tree_rss_bytes": 0, "min_volume_free_bytes": shutil.disk_usage(workspace).free,
               "sampled_peak_workspace_bytes": 0, "sampled_peak_allocated_bytes": 0,
               "sampled_peak_scratch_bytes": 0, "inventory_sampling_seconds": 0.0,
               "before": totals(before, scratch_prefixes), "stop_reason": None}
    timing = directory / f"{label}.time.log"
    with (directory / f"{label}.stdout.json").open("w") as out, \
            (directory / f"{label}.stderr.log").open("w") as err, \
            (directory / f"{label}.samples.jsonl").open("w") as samples:
        start = time.monotonic()
        if deadline_monotonic is not None:
            timeout_seconds = min(timeout_seconds, deadline_monotonic - start)
            if timeout_seconds <= 0:
                raise RuntimeError(f"{label}: run deadline elapsed before process launch")
        process = subprocess.Popen(["/usr/bin/time", "-l", "-o", str(timing), *argv],
                                   stdout=out, stderr=err, cwd=REPO, start_new_session=True)
        sampler = ThreadPoolExecutor(max_workers=1)
        pending_inventory = None
        next_inventory = 0.0
        next_progress = progress_seconds
        try:
            while True:
                elapsed = time.monotonic() - start
                rss = tree_rss(process_tree_root or process.pid)
                free = shutil.disk_usage(workspace).free
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
                    pending_inventory = sampler.submit(measured_totals, workspace, scratch_prefixes)
                    next_inventory = elapsed + 5
                samples.write(json.dumps(sample) + "\n")
                samples.flush()
                if next_progress is not None and elapsed >= next_progress:
                    print(json.dumps({"stage": label, **sample}), flush=True)
                    next_progress = elapsed + progress_seconds
                if elapsed > timeout_seconds:
                    metrics["stop_reason"] = f"{timeout_seconds:g}-second remaining deadline"
                elif rss > limits["memory_bytes"]:
                    metrics["stop_reason"] = f"sampled process-tree RSS exceeded {limits['memory_bytes']} bytes"
                elif free < 2 * GIB:
                    metrics["stop_reason"] = "less than 2 GiB volume free space"
                elif metrics["sampled_peak_scratch_bytes"] > limits["scratch_bytes"]:
                    metrics["stop_reason"] = f"sampled scratch exceeded {limits['scratch_bytes']} bytes"
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
                    signal_group(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    signal_group(process.pid, signal.SIGKILL)
            # The time wrapper may exit before a child finishes handling TERM.
            # Reap the entire private group even when the wrapper has exited.
            try:
                signal_group(process.pid, signal.SIGKILL)
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
            metrics["after"] = totals(after, scratch_prefixes)
            metrics["volume_free_after_bytes"] = shutil.disk_usage(workspace).free
            for metric, group, key in [("sampled_peak_workspace_bytes", "total", "bytes"),
                                        ("sampled_peak_allocated_bytes", "total", "allocated_bytes"),
                                        ("sampled_peak_scratch_bytes", "scratch", "bytes")]:
                metrics[metric] = max(metrics[metric], metrics["after"].get(group, {}).get(key, 0))
            if timing.exists():
                match = re.search(r"(\d+)\s+maximum resident set size", timing.read_text())
                metrics["time_max_rss_bytes"] = int(match.group(1)) if match else None
            if not metrics["stop_reason"]:
                if metrics["elapsed_seconds"] > timeout_seconds:
                    metrics["stop_reason"] = f"{timeout_seconds:g}-second remaining deadline"
                elif (metrics.get("time_max_rss_bytes") or 0) > limits["memory_bytes"]:
                    metrics["stop_reason"] = f"reported maximum RSS exceeded {limits['memory_bytes']} bytes"
                elif metrics["sampled_peak_workspace_bytes"] > disk_bound:
                    metrics["stop_reason"] = "workspace exceeded preflight allowance"
                elif metrics["sampled_peak_scratch_bytes"] > limits["scratch_bytes"]:
                    metrics["stop_reason"] = f"scratch exceeded {limits['scratch_bytes']} bytes"
                elif metrics["volume_free_after_bytes"] < 2 * GIB:
                    metrics["stop_reason"] = "less than 2 GiB volume free space after command"
            save(directory / f"{label}.after.json", after)
            save(directory / f"{label}.metrics.json", metrics)
    print(json.dumps({"arm": label, **metrics}), flush=True)
    if process.returncode or metrics["stop_reason"]:
        raise RuntimeError(f"{label} failed: see retained logs and metrics")
    output = json.loads((directory / f"{label}.stdout.json").read_text()) if json_output else None
    return output, metrics

