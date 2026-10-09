"""Frozen full-width Comments workload; no payload work occurs on import."""

from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
PREPARATION = REPO / "experiments/comments-input/preparation-20261008.json"
DOCKET_ROOT = Path(
    "/Users/mikewolfd/Work/corpora/session-reports-2026-10-06/"
    "bulk-current-resume/hosted-37449048296/native-family-dockets/"
    "dockets/build/generation"
)
COMMENTS_DIGEST = "bf8027bc72c270b958c1a51e96b1f8d0d950c2a1ea13cd52fdaa9d5d77fae875"
COMMENTS_BYTES = 2670490619
ROWS = 10_000_000
GENERATION = "6890875d7aaf081b6974df133d307bddc2b5e4c9fb9793dcfc33a49707a2a9d2"
DOCKET_INPUTS = {
    "dockets": ("dockets.parquet", 15480748, 279719,
                "81dcc636e24666cab8ccf4b215e24e80d74a13e5a0780ad7690efaaaa6316a44"),
    "docket-receipts": ("etl_receipts.parquet", 58573148, 279720,
                        "1bfc2b0d05b2f81bc39ee49633fd6560255652ec34f2567b02e45846b96c24c0"),
}
COMMENTS = (
    "comment_id docket_id agency_code first_name last_name organization category title "
    "comment document_type posted_date modify_date receive_date attachments_json "
    "text_content text_extraction_status pdf_extraction_results_json "
    "comment_on_document_id comment_on_object_id original_document_id "
    "comment_reference_values_json subtype duplicate_comments"
).split()
LENGTHS = ["comment_bytes", "text_content_bytes"]
DOCKETS = "docket_id agency_code title docket_type modify_date abstract rin".split()
JOIN_COLUMNS = COMMENTS + LENGTHS + ["docket_" + name for name in DOCKETS]
TUPLE = ["__docspec_left_file", "__docspec_left_row",
         "__docspec_right_file", "__docspec_right_row"]
SUMMARY_COLUMNS = ["docket_id", "comment_rows", "matched_rows", "known_comment_texts",
                   "comment_bytes", "text_content_bytes", "largest_comment_bytes"]
LIMITS = {"compute_threads": 12, "memory_bytes": 24 << 30, "scratch_bytes": 32 << 30,
          "batch_rows": 4096, "max_batch_bytes": 768 << 20, "max_metadata_bytes": 32 << 20}
COUPLED = {"kind": "coupled", "family": "docket-evidence", "generation": "g6890875d"}
SHAPING = ("SELECT *, CAST(octet_length(comment) AS BIGINT) AS comment_bytes, "
           "CAST(octet_length(text_content) AS BIGINT) AS text_content_bytes FROM input")


def version(release):
    return {"dataset": release["dataset"]["dataset"], "version": release["id"],
            "manifest": release["manifest"]}


def source(name, selected):
    return {"dataset": name, "operation": "initial", "input": selected["path"],
            "expected_digest": selected["blake3"], "definition": "comments-e2e-v1",
            "sql": SHAPING if name == "comments" else "SELECT * FROM input",
            "relationship": None if name == "comments" else COUPLED}


def seal(releases):
    return {"family": COUPLED["family"], "generation": COUPLED["generation"],
            "members": {name: version(releases[name]) for name in DOCKET_INPUTS}}


def refresh(dataset, transform):
    return {"dataset": dataset, "operation": "initial", "base": None,
            "definition": dataset + "-v1", "policy": "captured", "mode": "full",
            "relationship": None, "transform": transform}


def join(releases):
    fields = [{"side": "left", "field": name, "name": name}
              for name in COMMENTS + LENGTHS]
    fields += [{"side": "right", "field": name, "name": "docket_" + name}
               for name in DOCKETS]
    return refresh("comments-dockets", {
        "kind": "join", "left": version(releases["comments"]),
        "right": version(releases["dockets"]),
        "join": {"keys": [{"left": "docket_id", "right": "docket_id"}],
                 "kind": "left", "nulls": "never", "cardinality": "many_to_one",
                 "fields": fields}})


def summary(release):
    definitions = [("comment_rows", "count_rows", None),
                   ("matched_rows", "count", "docket_docket_id"),
                   ("known_comment_texts", "count", "comment"),
                   ("comment_bytes", "sum", "comment_bytes"),
                   ("text_content_bytes", "sum", "text_content_bytes"),
                   ("largest_comment_bytes", "max", "comment_bytes")]
    aggregates = [{"name": name, "function": {"kind": kind, **({"field": field} if field else {})}}
                  for name, kind, field in definitions]
    return refresh("docket-summary", {"kind": "aggregate", "input": version(release),
        "grouping": {"keys": ["docket_id"], "aggregates": aggregates}})


def export(release):
    dataset = release["dataset"]["dataset"]
    columns = JOIN_COLUMNS if dataset == "comments-dockets" else SUMMARY_COLUMNS
    return {"dataset": dataset, "operation": "flat", "source": version(release),
            "channel": "download", "expected": None,
            "policy": {"group": "source", "rows": "all", "columns": columns}}
