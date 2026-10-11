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

trusted = "SPICY_REGS_DOCSPEC_EXPORTS" in os.environ
admissions = {"metadata": 0, "hash": 0}
if trusted:
    from spicy_regs import docspec_exports
    original_record, original_hash = docspec_exports._record, docspec_exports._hash

    def record(*args, **kwargs):
        admissions["metadata"] += 1
        return original_record(*args, **kwargs)

    def hashed(*args, **kwargs):
        admissions["hash"] += 1
        return original_hash(*args, **kwargs)

    docspec_exports._record, docspec_exports._hash = record, hashed



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
        binding = reply["publication"]["comments"]
        assert binding["status"] == ("docspec_immutable_export" if trusted else "local_unversioned"), reply
        if trusted:
            assert binding["evidence"]["row_mapping"] is False
            assert binding["evidence"]["manifest_blake3"] == binding["export"]["source"]["manifest"]["digest"]
        schema = call(client, "DESCRIBE comments")
        assert [row[:2] for row in schema["rows"]] == [["public", "VARCHAR"], ["id", "BIGINT"]], schema
        if trusted:
            assert admissions == {"metadata": 2, "hash": 1}, admissions
        print(json.dumps({"rows": reply["rows"], "publication": binding, "admissions": admissions,
                          "scope": "MCP values and exact release reference; separate DocSpec receipt proof"}, ensure_ascii=False), flush=True)
