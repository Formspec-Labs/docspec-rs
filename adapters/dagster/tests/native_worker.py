"""One fresh native Dagster worker for the real-host recovery fixture."""

import json
import os
from pathlib import Path
import sys

from docspec_dagster.definitions import DocSpecResource, docspec_job


if __name__ == "__main__":
    config_path, url, report_path = sys.argv[1:]
    result = docspec_job.execute_in_process(
        run_config=json.loads(Path(config_path).read_text()),
        resources={"docspec": DocSpecResource(base_url=url, uncertain_retries=0, timeout_seconds=30)},
        raise_on_error=False,
    )
    if result.success:
        report = {"success": True, "summary": result.output_for_node("docspec_operation")}
    else:
        failure = result.failure_data_for_node("docspec_operation").user_failure_data
        report = {"success": False, "outcome": failure.metadata["outcome"].value}
    report["pid"] = os.getpid()
    Path(report_path).write_text(json.dumps(report))
