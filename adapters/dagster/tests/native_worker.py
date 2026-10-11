"""One fresh native Dagster worker for the real-host recovery fixture."""

import hashlib
import json
import os
from pathlib import Path
import sys
import time

from docspec_dagster.definitions import DocSpecResource, docspec_job


class ObservedResource(DocSpecResource):
    """Record the actual submission boundary before Dagster handles its outcome."""

    report_path: str

    def submit(self, route, request_json):
        path = Path(self.report_path)
        state = json.loads(path.read_text())

        def record(**changes):
            state.update(changes)
            temporary = path.with_suffix(".tmp")
            temporary.write_text(json.dumps(state))
            temporary.replace(path)

        record(submission="waiting", submitted_at=time.monotonic())
        try:
            result = super().submit(route, request_json)
        except BaseException:
            record(submission="failed")
            raise
        record(submission="returned")
        return result


if __name__ == "__main__":
    config_path, url, report_path = sys.argv[1:]
    config = json.loads(Path(config_path).read_text())
    request = config["ops"]["docspec_operation"]["config"]
    identity = {
        "pid": os.getpid(),
        "route": request["route"],
        "request_sha256": hashlib.sha256(request["request_json"].encode()).hexdigest(),
    }
    # The interrupted fixture must distinguish a killed waiting worker from a
    # worker that completed or learned an uncertain outcome before termination.
    Path(report_path).write_text(json.dumps({**identity, "started": True}))
    result = docspec_job.execute_in_process(
        run_config=config,
        resources={"docspec": ObservedResource(base_url=url, uncertain_retries=0, timeout_seconds=60, report_path=report_path)},
        raise_on_error=False,
    )
    if result.success:
        report = {"success": True, "summary": result.output_for_node("docspec_operation")}
    else:
        failure = result.failure_data_for_node("docspec_operation").user_failure_data
        report = {"success": False, "outcome": failure.metadata["outcome"].value}
    report.update(identity)
    Path(report_path).write_text(json.dumps(report))
