"""Prepare or execute the frozen full local Comments acceptance workload.

Preparation is explicit and separate from measurement. Acceptance always requires
three fresh full-workload runs; pilot runs the identical work once and cannot pass
acceptance. Nothing is downloaded, built, deleted, published, or resumed here.
"""

import argparse
import base64
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from integrity import read_source


# Load the small local modules from the same bytes retained as series evidence.
# Once loaded, ordinary edits in this shared worktree cannot alter their code.
LOADED_SOURCES = {"runner": read_source(Path(__file__))}


def loaded_module(name, path):
    captured = read_source(path)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    exec(compile(captured[0], str(path), "exec"), module.__dict__)
    LOADED_SOURCES[name] = captured
    return module


integrity = loaded_module("integrity", Path(__file__).with_name("integrity.py"))
file_digests = integrity.file_digests
loaded_module("workload", Path(__file__).with_name("workload.py"))
from workload import (COMMENTS, COMMENTS_BYTES, COMMENTS_DIGEST, DOCKET_INPUTS,
                      DOCKET_ROOT, GENERATION, HERE, LIMITS, PREPARATION, REPO, ROWS,
                      export, join, seal, source, summary)


monitor = loaded_module("comments_input_monitor", REPO / "experiments/comments-input/run.py")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    # A repeated invocation must never replace an earlier observation.
    with Path(path).open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def metadata(path):
    stat = Path(path).stat()
    return {"bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns,
            "allocated_bytes": stat.st_blocks * 512}


def dependencies():
    return {name: importlib.metadata.version(name)
            for name in ["pyarrow", "duckdb", "pyiceberg", "psutil", "blake3"]}


def prepare(args):
    """Verify each small input once per explicit preparation; never hash Comments."""
    import pyarrow.parquet as pq

    output = args.output.resolve()
    require(not output.exists(), "preparation record already exists; reuse it")
    output.parent.mkdir(parents=True, exist_ok=True)
    # Keep failed preparation observations; do not overwrite the final selection file.
    attempt = output.with_name(output.name + ".attempt-" + uuid.uuid4().hex + ".json")
    record = {"format": "comments-e2e-inputs-v1", "complete": False,
              "libraries": dependencies(), "inputs": {}, "started_at": time.time(),
              "comments_payload_rehashed": False,
              "docket_artifact": str(DOCKET_ROOT / "artifact.json"),
              "docket_members": str(DOCKET_ROOT / "members.json"),
              "docket_generation": GENERATION}
    started = time.monotonic()
    try:
        prepared = read(PREPARATION)
        require(prepared["complete"] and prepared["rows_selected"] == ROWS, "incomplete frozen Comments")
        require(prepared["identity"] == {"bytes": COMMENTS_BYTES, "digest": COMMENTS_DIGEST},
                "frozen Comments identity differs")
        comment_path = Path(prepared["path"]).resolve(strict=True)
        require(comment_path == REPO / "workspaces/qualification/comments-10m-20261008/comments.parquet",
                "Comments selection path differs")
        require(comment_path.stat().st_size == COMMENTS_BYTES, "frozen Comments size differs")
        footer = pq.ParquetFile(comment_path)
        require(footer.metadata.num_rows == ROWS and footer.schema_arrow.names == COMMENTS,
                "frozen Comments shape differs")
        record["inputs"]["comments"] = {
            "path": str(comment_path), **metadata(comment_path), "rows": ROWS,
            "blake3": COMMENTS_DIGEST, "sha256": None,
            "schema_ipc_base64": base64.b64encode(footer.schema_arrow.serialize()).decode(),
            "provenance": {"preparation": str(PREPARATION),
                           "preparation_sha256": monitor.sha(PREPARATION),
                           "selection": prepared["selection"], "source": prepared["source"]}}
        artifact, members = read(DOCKET_ROOT / "artifact.json"), read(DOCKET_ROOT / "members.json")
        require(artifact["artifactDigest"] == "sha256:" + GENERATION, "docket generation differs")
        expected_members = {member["objectKey"]: member for member in members["members"]}
        manifest_sha = monitor.sha(DOCKET_ROOT / "members.json")
        require(artifact["memberManifests"][0]["sha256"] == "sha256:" + manifest_sha,
                "docket member manifest digest differs")
        record["docket_members_sha256"] = manifest_sha
        record["docket_artifact_sha256"] = monitor.sha(DOCKET_ROOT / "artifact.json")
        for name, (filename, size, rows, expected_sha) in DOCKET_INPUTS.items():
            path = (DOCKET_ROOT / filename).resolve(strict=True)
            before = metadata(path)
            member = expected_members[filename]
            require(before["bytes"] == member["byteSize"] == size and member["recordCount"] == rows,
                    "docket member size/count differs")
            require(member["sha256"] == "sha256:" + expected_sha, "docket member identity differs")
            hash_started = time.monotonic()
            identity = file_digests(path, sha256=True)
            record.setdefault("digest_checks", {})[name] = {
                **identity, "path": str(path), "seconds": time.monotonic() - hash_started,
                "payload_passes": 1}
            require(identity["sha256"] == expected_sha, "docket payload SHA-256 differs")
            require(identity["bytes"] == size, "docket BLAKE3 byte count differs")
            require(metadata(path) == before, "docket file changed during preparation")
            footer = pq.ParquetFile(path)
            require(footer.metadata.num_rows == rows, "docket footer row count differs")
            record["inputs"][name] = {"path": str(path), **before, "rows": rows,
                "sha256": identity["sha256"], "blake3": identity["blake3"],
                "schema_ipc_base64": base64.b64encode(footer.schema_arrow.serialize()).decode()}
        record["complete"] = True
    except BaseException as error:
        record["failure"] = repr(error)
        raise
    finally:
        record["elapsed_seconds"] = time.monotonic() - started
        write(attempt, record)
    write(output, record)
    print(json.dumps({"preparation": str(output), "attempt": str(attempt), "complete": True}))


def validated_inputs(path):
    record = read(path)
    require(record["format"] == "comments-e2e-inputs-v1" and record["complete"], "inputs are not prepared")
    require(set(record["inputs"]) == {"comments", "dockets", "docket-receipts"}, "input set differs")
    require(record["docket_generation"] == GENERATION, "docket generation differs")
    comments = record["inputs"]["comments"]
    require(comments["blake3"] == COMMENTS_DIGEST and comments["rows"] == ROWS and
            comments["bytes"] == COMMENTS_BYTES, "Comments population/identity differs")
    for name, (_, size, rows, sha) in DOCKET_INPUTS.items():
        item = record["inputs"][name]
        require((item["bytes"], item["rows"], item["sha256"]) == (size, rows, sha),
                "docket selection differs")
    for item in record["inputs"].values():
        now = metadata(item["path"])
        require((now["bytes"], now["mtime_ns"]) == (item["bytes"], item["mtime_ns"]),
                "prepared input metadata changed; engine admission must verify the selected bytes")
    return record


def freeze_execution(series, binary, prepared, build_receipt):
    """Select the executable and subprocess sources once, before any timed run."""
    frozen = series / "frozen"
    frozen.mkdir()
    started = time.monotonic()
    capture = {"format": "comments-e2e-execution-v1", "complete": False,
               "started_at": time.time(), "artifacts": {},
               "scope": "series-owned read-only files; protection from normal worktree drift"}
    try:
        for name, filename in {"runner": "run.py", "workload": "workload.py",
                               "integrity": "integrity.py",
                               "comments_input_monitor": "comments-input-monitor.py"}.items():
            payload, identity = LOADED_SOURCES[name]
            capture["artifacts"][name] = integrity.save_loaded_source(frozen / filename, payload, identity)
        for name, path, filename in [
            ("binary", binary, "docspec"), ("oracle", HERE / "oracle.py", "oracle.py"),
            ("protocol", HERE / "protocol.md", "protocol.md"),
            ("lock", REPO / "Cargo.lock", "Cargo.lock"),
            ("prepared", prepared, "prepared.json"),
            ("build_receipt", build_receipt, "build-receipt.json"),
        ]:
            capture["artifacts"][name] = integrity.copy_frozen_file(path, frozen / filename)
        # The producer's build receipt is retained verbatim, without inventing an
        # attestation format. Its binary identity can be matched to the captured
        # executable identity; dirty source is also retained for reconstruction.
        diff = subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=REPO)
        with (frozen / "worktree.diff").open("xb") as stream:
            stream.write(diff)
        (frozen / "worktree.diff").chmod(0o444)
        capture["worktree_diff"] = {"path": str(frozen / "worktree.diff"),
                                    "bytes": len(diff), "sha256": hashlib.sha256(diff).hexdigest()}
        frozen.chmod(0o555)
        capture["complete"] = True
    except BaseException as error:
        capture["failure"] = repr(error)
        raise
    finally:
        capture["finished_at"] = time.time()
        capture["elapsed_seconds"] = time.monotonic() - started
        write(series / "execution-capture.json", capture)
    return capture


def environment(capture, args):
    import psutil

    return {"platform": platform.platform(), "python": sys.version, "executable": sys.executable,
        "libraries": dependencies(), "cpu_count": os.cpu_count(), "load_average": os.getloadavg(),
        "physical_memory_bytes": psutil.virtual_memory().total,
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        "worktree_status": subprocess.check_output(["git", "status", "--short"], cwd=REPO, text=True),
        "binary": capture["artifacts"]["binary"]["path"],
        "oracle": capture["artifacts"]["oracle"]["path"], "mode": args.mode,
        "execution_capture": "execution-capture.json",
        "cache": "OS caches are not cleared; frozen inputs may be warm; application workspaces are fresh",
        "background_load_note": args.background_load_note,
        "background_processes": subprocess.check_output(
            ["ps", "-axo", "pid,ppid,pcpu,rss,comm"], text=True),
        "limits": LIMITS, "oracle_memory_bytes": 18 << 30,
        "retained_bytes_per_run_allowance": args.retained_bytes_per_run,
        "retained_allowance_is_estimate": True,
        "sampling": {"rss_free_seconds": 1, "disk_seconds": 5,
            "scope": "runner and descendants; peaks are sampled lower bounds; time -l also records child RSS",
            "scratch": "all series run workspace/scratch and oracle-scratch directories"},
        "digests": {name: item["sha256"] for name, item in capture["artifacts"].items()}}


def run_one(series, index, selected, binary, allowance, repetitions, oracle_path=None):
    run = series / f"run-{index:02}"
    run.mkdir()
    workspace = run / "workspace"
    workspace.mkdir()
    write(run / "inputs.json", selected)
    write(run / "limits.json", LIMITS)
    prefix = [str(binary), "--workspace", str(workspace), "--limits", str(run / "limits.json")]
    scratch = tuple(prefix for i in range(1, repetitions + 1)
                    for prefix in [f"run-{i:02}/workspace/scratch", f"run-{i:02}/oracle-scratch"])
    before = monitor.totals(monitor.inventory(series), scratch)
    disk_bound = before.get("total", {}).get("bytes", 0) + allowance + LIMITS["scratch_bytes"]
    result = {"run": index, "passed": False, "stages": {}, "failure": None,
              "deadline_seconds": 1800, "input_bytes": sum(v["bytes"] for v in selected["inputs"].values())}
    started = time.monotonic()
    deadline = started + 1800

    def command(label, argv):
        print(json.dumps({"stage": label, "status": "starting", "run": index,
                          "remaining_seconds": deadline - time.monotonic()}), flush=True)
        value, metrics = monitor.command(run, series, label, argv, disk_bound,
            limits=LIMITS, timeout_seconds=deadline - time.monotonic(), process_tree_root=os.getpid(),
            scratch_prefixes=scratch, progress_seconds=30, deadline_monotonic=deadline)
        result["stages"][label] = metrics
        return value

    def request(label, verb, spec):
        path = run / (label + ".request.json")
        write(path, spec)
        return command(label, [*prefix, verb, str(path)])

    try:
        releases = {}
        for name in ["comments", "dockets", "docket-receipts"]:
            releases[name] = request(name, "run", source(name, selected["inputs"][name]))
        request("seal", "seal-generation", seal(releases))
        releases["comments-dockets"] = request("comments-dockets", "refresh", join(releases))
        releases["docket-summary"] = request("docket-summary", "refresh", summary(releases["comments-dockets"]))
        exports = {name: request(name + "-export", "export", export(releases[name]))
                   for name in ["comments-dockets", "docket-summary"]}
        for name, release in releases.items():
            require(command(name + "-selected", [*prefix, "latest", name]) == release,
                    "catalog native selection differs")
        for name, pin in exports.items():
            require(command(name + "-export-selected", [*prefix, "latest-export", name, "download"]) == pin,
                    "catalog export selection differs")
        # Production passes the captured path. The optional override also permits
        # explicitly synthetic integration fixtures without changing the CLI gate.
        oracle = command("oracle", [sys.executable, "-B", str(oracle_path or HERE / "oracle.py"), str(run)])
        require(oracle["exact_readback_passed"], "independent exact readback did not pass")
        require(time.monotonic() <= deadline, "full local run exceeded 1800 seconds")
        retained = monitor.totals(monitor.inventory(run))["total"]["bytes"]
        require(retained <= allowance, "retained run bytes exceeded the frozen allowance")
        result["passed"] = True
    except BaseException as error:
        result["failure"] = repr(error)
    finally:
        result["elapsed_seconds"] = time.monotonic() - started
        if result["elapsed_seconds"] > 1800:
            result["passed"] = False
            result["failure"] = result["failure"] or "full local run exceeded 1800 seconds"
        # Failed command metrics remain discoverable even when command() raised.
        result["stages"] = {path.name.removesuffix(".metrics.json"): read(path)
                            for path in sorted(run.glob("*.metrics.json"))}
        result["retained_workspace"] = monitor.totals(monitor.inventory(workspace))
        result["retained_run"] = monitor.totals(monitor.inventory(run))
        result["retained_amplification_vs_inputs"] = result["retained_run"]["total"]["bytes"] / result["input_bytes"]
        stages = list(result["stages"].values())
        for key in ["sampled_peak_tree_rss_bytes", "sampled_peak_workspace_bytes",
                    "sampled_peak_allocated_bytes", "sampled_peak_scratch_bytes", "time_max_rss_bytes"]:
            result[key] = max((m.get(key) or 0 for m in stages), default=0)
        result["peak_additional_logical_bytes"] = max(0, result["sampled_peak_workspace_bytes"] -
                                                      before.get("total", {}).get("bytes", 0))
        result["peak_additional_allocated_bytes"] = max(0, result["sampled_peak_allocated_bytes"] -
                                                        before.get("total", {}).get("allocated_bytes", 0))
        result["min_volume_free_bytes"] = min((m["min_volume_free_bytes"] for m in stages),
                                              default=shutil.disk_usage(series).free)
        result["inventory_sampling_seconds"] = sum(m["inventory_sampling_seconds"] for m in stages)
        write(run / "result.json", result)
    return result


def execute(args):
    binary = args.binary.resolve(strict=True)
    args.inputs = args.inputs.resolve(strict=True)
    args.build_receipt = args.build_receipt.resolve(strict=True)
    args.output = args.output.resolve()
    selected = validated_inputs(args.inputs)
    require(args.retained_bytes_per_run > 0, "a positive retained-output allowance is required")
    args.output.mkdir(parents=True, exist_ok=True)
    series = args.output / (time.strftime("%Y%m%dT%H%M%S") + "-" + args.mode + "-" + uuid.uuid4().hex[:8])
    series.mkdir()
    repetitions = 3 if args.mode == "acceptance" else 1
    result = {"mode": args.mode, "required_fresh_runs": 3, "runs": [],
              "full_local_performance_acceptance_passed": False, "failure": None}
    print(json.dumps({"artifacts": str(series), "mode": args.mode}), flush=True)
    try:
        capture = freeze_execution(series, binary, args.inputs, args.build_receipt)
        require(read(capture["artifacts"]["prepared"]["path"]) == selected,
                "prepared input record changed before execution capture")
        env = environment(capture, args)
        free = shutil.disk_usage(series).free
        needed = repetitions * args.retained_bytes_per_run + LIMITS["scratch_bytes"] + (2 << 30)
        env.update(free_bytes_before=free, required_free_bytes=needed)
        write(series / "environment.json", env)
        write(series / "inputs.json", selected)
        require(free >= needed, "disk preflight cannot fit all retained runs plus 32 GiB scratch and 2 GiB reserve")
        for index in range(1, repetitions + 1):
            run = run_one(series, index, selected, Path(capture["artifacts"]["binary"]["path"]),
                          args.retained_bytes_per_run, repetitions,
                          oracle_path=Path(capture["artifacts"]["oracle"]["path"]))
            result["runs"].append(run)
            if not run["passed"]:
                raise RuntimeError(f"run {index} failed; retained; remaining repetitions not run")
        result["full_local_performance_acceptance_passed"] = args.mode == "acceptance" and len(result["runs"]) == 3
    except BaseException as error:
        result["failure"] = repr(error)
    finally:
        result["retained_series"] = monitor.totals(monitor.inventory(series))
        result["volume_free_after_bytes"] = shutil.disk_usage(series).free
        write(series / "results.json", result)
        print(json.dumps(result), flush=True)
    return 0 if result["failure"] is None else 1


def main():
    def terminated(signum, _frame):
        raise InterruptedError(f"runner received signal {signum}")

    # Unwind through the reused monitor so it terminates its private child group
    # and retains failure metrics when an operator cancels this runner.
    signal.signal(signal.SIGTERM, terminated)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preparation = commands.add_parser("prepare", help="explicitly verify small inputs; reuse frozen Comments identity")
    preparation.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("run", help="full acceptance or one explicitly non-acceptance pilot")
    run.add_argument("--binary", type=Path, default=REPO / "target/release/docspec")
    run.add_argument("--inputs", type=Path, required=True)
    run.add_argument("--build-receipt", type=Path, required=True,
                     help="existing release build receipt, retained verbatim with the captured binary")
    run.add_argument("--output", type=Path, default=HERE / "runs")
    run.add_argument("--mode", choices=["acceptance", "pilot"], required=True)
    run.add_argument("--retained-bytes-per-run", type=int, required=True,
                     help="predeclared retained-output allowance; not a measured estimate")
    run.add_argument("--background-load-note", required=True,
                     help="record concurrent work; acceptance requires unrelated builds/tests/benchmarks stopped")
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args)
        return 0
    return execute(args)


if __name__ == "__main__":
    raise SystemExit(main())
