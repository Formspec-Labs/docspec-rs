"""Resume the retained failure under the explicitly revised workload profile."""

import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

from run import BINARY, HERE, LIMITS, REPO, command, inventory, save, sha, totals


def main():
    original = HERE / "run-20261008T103609"
    workspace = original / "workspace"
    directory = HERE / time.strftime("resume-%Y%m%dT%H%M%S")
    directory.mkdir()
    limits = {**LIMITS, "max_batch_bytes": 768 << 20}
    save(directory / "limits.json", limits)
    spec = json.loads((original / "request.json").read_text())
    save(directory / "request.json", spec)
    input_identity = json.loads((HERE / "preparation-20261008.json").read_text())["identity"]
    prefix = [str(BINARY), "--workspace", str(workspace), "--limits", str(directory / "limits.json")]
    save(directory / "environment.json", {
        "platform": platform.platform(), "python": sys.version, "load_average": os.getloadavg(),
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        "status": subprocess.check_output(["git", "status", "--short"], cwd=REPO, text=True),
        "input_identity": input_identity, "workspace": str(workspace),
        "cache": "not cleared; retained input has been scanned by diagnostics; may be warm",
        "digests": {name: sha(path) for name, path in {"binary": BINARY, "driver": Path(__file__),
            "monitor": HERE / "run.py", "protocol": HERE / "resume-protocol.md",
            "lock": REPO / "Cargo.lock", "original_metrics": original / "fresh.metrics.json"}.items()}})
    before = totals(inventory(workspace))
    result = {"bounded_checks_passed": False, "resumed_run": True,
              "full_performance_acceptance_passed": False, "payload_read_avoidance_measured": False,
              "before": before}
    disk_bound = input_identity["bytes"] * 3 + limits["scratch_bytes"]
    print(f"Artifacts: {directory}", flush=True)
    try:
        inspection, result["preflight_inspection"] = command(directory, workspace, "inspect",
            [*prefix, "inspect", spec["dataset"]], disk_bound)
        assert inspection["authority"]["selected"] is None, "original operation already selected; this is no longer a processing resume"
        admissions = [v for k, v in inspection.items() if k.startswith("phase/") and v.get("phase") == "admission"]
        assert len(admissions) == 1 and len(admissions[0]["outputs"]) == 1
        admitted = admissions[0]["outputs"][0]
        assert admitted["digest"] == input_identity["digest"] and admitted["bytes"] == input_identity["bytes"]
        first, result["resume"] = command(directory, workspace, "resume",
            [*prefix, "resume", str(directory / "request.json")], disk_bound)
        assert first["dataset"]["inputs"]["input"] == admitted
        latest, result["selection"] = command(directory, workspace, "selection",
            [*prefix, "latest", spec["dataset"]], disk_bound)
        assert first == latest
        result["readback"], result["readback_metrics"] = command(directory, workspace, "readback",
            [sys.executable, str(HERE / "run.py"), "readback", str(directory / "resume.stdout.json"),
             admitted["path"], str(directory / "limits.json")], disk_bound)
        result["resume_with_readback_seconds"] = sum(result[name]["elapsed_seconds"]
            for name in ("resume", "selection", "readback_metrics"))
        result["original_failed_seconds"] = json.loads((original / "fresh.metrics.json").read_text())["elapsed_seconds"]
        result["cumulative_engine_and_readback_seconds"] = result["original_failed_seconds"] + result["resume_with_readback_seconds"]
        assert result["cumulative_engine_and_readback_seconds"] <= 1800
        spec["operation"] = "unchanged-zstd"
        save(directory / "unchanged-request.json", spec)
        second, result["unchanged"] = command(directory, workspace, "unchanged",
            [*prefix, "run", str(directory / "unchanged-request.json")], disk_bound)
        assert first["dataset"]["groups"] == second["dataset"]["groups"]
        assert first["dataset"]["inputs"] == second["dataset"]["inputs"]
        before_retry = inventory(workspace)
        third, result["retry"] = command(directory, workspace, "retry",
            [*prefix, "run", str(directory / "unchanged-request.json")], disk_bound)
        assert second == third
        assert before_retry == inventory(workspace), "completed retry changed retained files"
        result["selected_native_data_bytes"] = sum(obj["bytes"] for obj in first["dataset"]["groups"]["source"]["objects"]
                                                   if obj["path"].endswith(".parquet"))
        result["bounded_checks_passed"] = True
    except Exception as error:
        result["failure"] = repr(error)
    result["final_workspace"] = totals(inventory(workspace))
    result["marginal_retained_bytes"] = result["final_workspace"]["total"]["bytes"] - before["total"]["bytes"]
    result["cumulative_retained_amplification_vs_input"] = result["final_workspace"]["total"]["bytes"] / input_identity["bytes"]
    save(directory / "results.json", result)
    print(json.dumps(result), flush=True)
    return 0 if result["bounded_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
