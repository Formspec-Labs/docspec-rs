"""Measure the frozen bounded history probe using the existing local harness."""

import json
import statistics
import time
from pathlib import Path

import run


output = run.REPO / "experiments/local-reuse" / time.strftime("history-%Y%m%dT%H%M%S")
output.mkdir()
workspace = output / "workspace"
source = run.SOURCE / "bill_actions.parquet"
member = next(m for m in json.loads((run.SOURCE / "members.json").read_text())["members"]
              if m["objectKey"] == source.name)
assert run.sha(source) == member["sha256"].removeprefix("sha256:")
(output / "environment.json").write_text(json.dumps({
    "binary_sha256": run.sha(run.BINARY), "lock_sha256": run.sha(run.REPO / "Cargo.lock"),
    "harness_sha256": run.sha(Path(__file__)), "common_harness_sha256": run.sha(Path(run.__file__)),
    "protocol_sha256": run.sha(Path(__file__).with_name("history-protocol.md")),
    "source": member, "cache": "not cleared; ordinary workstation background load"}, indent=2) + "\n")
identity, _ = run.command(output, "hash", "hash", source)
first = None
results = []
start = time.monotonic()
failure = None
try:
    for number in range(1, 51):
        if time.monotonic() - start > 300:
            raise RuntimeError("five-minute probe bound reached")
        spec = {"dataset": "history", "operation": f"operation-{number}", "input": str(source),
                "expected_digest": identity["digest"], "sql": "SELECT * FROM input", "definition": "probe-v1"}
        request = output / f"request-{number}.json"
        request.write_text(json.dumps(spec, indent=2) + "\n")
        release, metrics = run.command(output, f"operation-{number}", "--workspace", workspace, "run", request)
        first = first or release
        assert release["dataset"]["inputs"] == first["dataset"]["inputs"]
        assert release["dataset"]["groups"] == first["dataset"]["groups"]
        entry = {"operations": number, "run": metrics}
        if number in (1, 10, 50):
            lookups = []
            for repeat in range(3):
                latest, timing = run.command(output, f"latest-{number}-{repeat}", "--workspace", workspace, "latest", "history")
                assert latest == release
                lookups.append(timing)
            entry["latest"] = lookups
            _, entry["plan"] = run.command(output, f"plan-{number}", "--workspace", workspace, "plan", request)
            _, entry["inspect"] = run.command(output, f"inspect-{number}", "--workspace", workspace, "inspect", "history")
            files = run.inventory(workspace)
            entry["retained_files"] = len(files)
            entry["retained_bytes"] = sum(f["size"] for f in files.values())
            entry["metadata_json_bytes"] = sum(f["size"] for name, f in files.items() if name.endswith(".metadata.json"))
            entry["budgets_pass"] = (statistics.median(t["elapsed_seconds"] for t in lookups) <= 1
                                      and entry["plan"]["elapsed_seconds"] <= 2
                                      and entry["inspect"]["elapsed_seconds"] <= 3
                                      and metrics["elapsed_seconds"] <= 3)
            print(json.dumps(entry), flush=True)
        results.append(entry)
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
except Exception as error:
    failure = repr(error)
finally:
    (output / "completion.json").write_text(json.dumps({"failure": failure,
        "elapsed_seconds": time.monotonic() - start, "completed_operations": len(results)}, indent=2) + "\n")
    print(f"Artifacts: {output}; failure: {failure}")
