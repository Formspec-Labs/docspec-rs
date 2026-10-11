"""Opt-in application reader; real MCP HTTP and real local-file connection only."""

import json
import os
from pathlib import Path
import signal
import sys

# Bound fixture failures; no helper child may outlive a failed test indefinitely.
signal.alarm(120)
repository, directory = map(Path, sys.argv[1:])
assert repository.joinpath("src/spicy_regs/mcp_server.py").is_file()
assert Path(os.environ["SPICY_REGS_DATA_DIR"]) == directory
sys.path.insert(0, str(repository / "src"))

from starlette.testclient import TestClient
from spicy_regs import mcp_server


def call(client, sql):
    response = client.post(
        "/mcp",
        headers={"Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-06-18"},
        json={"jsonrpc": "2.0", "id": "qualification", "method": "tools/call", "params": {
            "name": "query_sql", "arguments": {"sql": sql, "max_rows": 50},
        }},
    )
    assert response.status_code == 200, response.text
    messages = [json.loads(line[5:]) for line in response.text.splitlines() if line.startswith("data:")]
    result = messages[-1]["result"]
    assert result["isError"] is False, result
    reply = result["structuredContent"]
    assert reply["source"] == "local", reply
    assert reply["truncated"] is False, reply
    assert reply["truncated_cells"] == [], reply
    assert "private-canary" not in json.dumps(reply), reply
    return reply


with TestClient(mcp_server.build_app()) as client:
    for command in sys.stdin:
        if command.strip() == "close":
            break
        assert command.strip() == "query"
        reply = call(client, "SELECT id, public FROM comments ORDER BY id NULLS LAST, public NULLS FIRST")
        assert reply["columns"] == ["id", "public"], reply
        # Honest application limit: these explicit files have no application release/evidence binding.
        assert reply["publication"]["comments"]["status"] == "local_unversioned", reply
        schema = call(client, "DESCRIBE comments")
        assert [row[:2] for row in schema["rows"]] == [["public", "VARCHAR"], ["id", "BIGINT"]], schema
        print(json.dumps({"rows": reply["rows"], "scope": "MCP values; separate DocSpec evidence proof"}, ensure_ascii=False), flush=True)
