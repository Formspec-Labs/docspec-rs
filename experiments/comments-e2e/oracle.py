"""Independent full-value qualification over exact native snapshots and flat files.

PyIceberg plans metadata only. DuckDB performs duplicate-preserving exact comparisons
with bounded engine memory/spill; the parent monitor bounds aggregate process RSS.
No result table is collected into Python and no hash or sample substitutes for equality.
"""

import base64
import json
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import StaticTable

from integrity import file_digests
from workload import (COMMENTS, COUPLED, DOCKETS, JOIN_COLUMNS, LENGTHS, ROWS,
                      SUMMARY_COLUMNS, TUPLE, export, join, seal, summary, version)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def q(name):
    return '"' + name.replace('"', '""') + '"'


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def columns(names, alias=None):
    return ", ".join((q(alias) + "." if alias else "") + q(name) for name in names)


def same_schema(actual, expected, label):
    # Arrow equality without metadata still checks nested field names/types/nullability.
    require(actual.equals(expected, check_metadata=False),
            f"{label}: schema differs: {actual} != {expected}")


def native_field(actual, expected):
    """Check native IDs and interpretation; Iceberg has no Arrow offset-width type."""
    require(actual.name == expected.name and actual.nullable == expected.nullable,
            f"native field name/nullability differs: {actual.name}")
    require((actual.metadata or {}).get(b"PARQUET:field_id") ==
            (expected.metadata or {}).get(b"PARQUET:field_id"),
            f"native field ID differs: {actual.name}")
    a, e = actual.type, expected.type
    if pa.types.is_struct(e):
        require(pa.types.is_struct(a) and a.num_fields == e.num_fields, "native struct differs")
        for af, ef in zip(a, e):
            native_field(af, ef)
    elif pa.types.is_list(e) or pa.types.is_large_list(e):
        require(pa.types.is_list(a) or pa.types.is_large_list(a), "native list differs")
        native_field(a.value_field, e.value_field)
    elif pa.types.is_string(e) or pa.types.is_large_string(e):
        require(pa.types.is_string(a) or pa.types.is_large_string(a), "native string differs")
    elif pa.types.is_binary(e) or pa.types.is_large_binary(e):
        require(pa.types.is_binary(a) or pa.types.is_large_binary(a), "native binary differs")
    else:
        require(a == e, f"native type differs: {a} != {e}")


def export_field_type(data_type):
    """Describe the exact primitive types in this frozen workload's ExportPin."""
    names = {pa.string(): "Utf8", pa.int64(): "Int64"}
    if data_type in names:
        return names[data_type]
    if pa.types.is_decimal128(data_type):
        return f"Decimal128({data_type.precision}, {data_type.scale})"
    raise ValueError(f"unqualified export type in the frozen workload: {data_type}")


def flat_schema(actual, native, descriptions, names, label):
    """Bind exported field IDs and serialized mappings to the selected native fields."""
    selected = pa.schema([native.field(name) for name in names])
    same_schema(actual, selected, label)
    require(not actual.metadata, f"{label}: unexpected exported schema metadata")
    require(len(descriptions) == len(selected), f"{label}: export field mapping length differs")
    for exported, original, description in zip(actual, selected, descriptions):
        field_id = (original.metadata or {}).get(b"PARQUET:field_id")
        require(field_id is not None and int(field_id) > 0, f"{label}: native field ID missing")
        require(exported.metadata == {b"PARQUET:field_id": field_id},
                f"{label}: exported field ID/metadata differs: {original.name}")
        require(description == {"id": int(field_id), "name": original.name,
                "data_type": export_field_type(original.type), "nullable": original.nullable},
                f"{label}: ExportPin.fields mapping differs: {original.name}")


def exact_equal(connection, left, right, names, label):
    """Compare all values and multiplicities; typed schemas are checked separately."""
    selected = columns(names)
    different = connection.execute(
        f"SELECT EXISTS (SELECT {selected} FROM ("
        f"SELECT {selected}, 1::BIGINT AS __oracle_weight FROM {q(left)} UNION ALL "
        f"SELECT {selected}, -1::BIGINT AS __oracle_weight FROM {q(right)}) counts "
        f"GROUP BY {selected} HAVING sum(__oracle_weight) <> 0)"
    ).fetchone()[0]
    require(not different, f"{label}: exact multiset mismatch")


class Oracle:
    def __init__(self, run):
        self.run = run
        self.checks = []
        self.schemas = {}
        self.tables = {}
        self.connection = duckdb.connect(config={"enable_external_access": True})
        scratch = run / "oracle-scratch"
        scratch.mkdir(exist_ok=False)
        self.connection.execute("SET memory_limit='18GiB'")
        self.connection.execute("SET threads=12")
        self.connection.execute("SET max_temp_directory_size='32GiB'")
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute(f"SET temp_directory={literal(scratch)}")

    def record(self, label, started, **details):
        item = {"check": label, "seconds": time.monotonic() - started, **details}
        self.checks.append(item)
        with (self.run / "oracle-checks.jsonl").open("a") as stream:
            stream.write(json.dumps(item) + "\n")
        print(json.dumps(item), file=sys.stderr, flush=True)

    def view(self, name, sql):
        self.connection.execute(f"CREATE VIEW {q(name)} AS {sql}")

    def input(self, name, selected, admitted):
        started = time.monotonic()
        require(admitted["digest"] == selected["blake3"] and admitted["bytes"] == selected["bytes"],
                "admission does not bind the selected whole input")
        path = Path(admitted["path"]).resolve(strict=True)
        require(path.is_relative_to((self.run / "workspace").resolve()),
                "fresh admission is not retained in its workspace")
        require(path.stat().st_size == selected["bytes"], f"admitted input size changed: {name}")
        identity = file_digests(path)
        require(identity == {"bytes": selected["bytes"], "blake3": selected["blake3"]},
                f"admitted input bytes differ from frozen identity: {name}")
        self.record(name + " admitted byte identity", started, path=str(path), **identity)
        parquet = pq.ParquetFile(path)
        require(parquet.metadata.num_rows == selected["rows"], f"input row count: {name}")
        frozen_schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(selected["schema_ipc_base64"])))
        same_schema(parquet.schema_arrow, frozen_schema, name + " prepared schema")
        self.schemas[name] = parquet.schema_arrow
        self.view(name, f"SELECT * FROM read_parquet({literal(path)}, hive_partitioning=false)")

    def native(self, name, pin):
        started = time.monotonic()
        metadata_path = Path(pin["metadata"]["path"])
        require(metadata_path.stat().st_size == pin["metadata"]["bytes"], "metadata size differs")
        metadata = json.loads(metadata_path.read_text())
        require(metadata["current-snapshot-id"] == pin["snapshot"], "native snapshot differs")
        require(metadata["current-schema-id"] == pin["schema_id"], "native schema ID differs")
        table = StaticTable.from_metadata(str(metadata_path))
        native_schema = schema_to_pyarrow(table.schema())
        objects = {item["path"]: item for item in pin["objects"]}
        require(len(objects) == len(pin["objects"]), "duplicate retained object paths")
        paths, rows, physical_schema = [], 0, None
        for task in table.scan(snapshot_id=pin["snapshot"]).plan_files():
            require(not task.delete_files, "oracle does not support native delete files")
            path = Path(task.file.file_path)
            require(path.is_absolute() and str(path) in objects, "unretained native file")
            require(str(path) not in paths, "repeated native file")
            size = path.stat().st_size
            require(size == task.file.file_size_in_bytes == objects[str(path)]["bytes"],
                    "native file size differs")
            # PyIceberg 0.12 FileScanTask represents a complete file and has no
            # start/length properties. Refuse a future explicit partial task.
            require(getattr(task, "start", 0) == 0 and getattr(task, "length", size) == size,
                    "partial native file task")
            parquet = pq.ParquetFile(path)
            require(parquet.metadata.num_rows == task.file.record_count, "native file rows differ")
            actual = parquet.schema_arrow
            require(len(actual) == len(native_schema), "native column count differs")
            for af, nf in zip(actual, native_schema):
                native_field(af, nf)
            if physical_schema is not None:
                same_schema(actual, physical_schema, name + " file schemas")
            physical_schema = actual
            rows += parquet.metadata.num_rows
            paths.append(str(path))
        require(set(paths) == {p for p in objects if p.endswith(".parquet")},
                "native snapshot membership differs from retained pin")
        require(rows == pin["rows"], "native pin row count differs")
        require(paths, "this frozen workload requires nonempty native tables")
        public = [field for field in physical_schema
                  if field.name not in ["__docspec_origin_file", "__docspec_origin_row"]]
        self.schemas[name] = pa.schema(public)
        self.tables[name] = {"files": paths, "rows": rows, "snapshot": pin["snapshot"],
                             "schema_id": pin["schema_id"]}
        files = "[" + ",".join(literal(path) for path in paths) + "]"
        scan = f"read_parquet({files}, filename=true, file_row_number=true, hive_partitioning=false)"
        self.view(name, f"SELECT {columns([f.name for f in public])} FROM {scan}")
        origins = [field.name for field in physical_schema if field.name.startswith("__docspec_origin_")]
        require(not origins or set(origins) == {"__docspec_origin_file", "__docspec_origin_row"},
                "incomplete native occurrence identity")
        file_expr = "filename"
        row_expr = "file_row_number"
        if origins:
            # Fresh qualified files may contain fallback null pairs; use physical positions then.
            self.view(name + "_physical", f"SELECT * FROM {scan}")
            invalid = self.connection.execute(
                f"SELECT EXISTS (SELECT 1 FROM {q(name + '_physical')} WHERE "
                "(__docspec_origin_file IS NULL) <> (__docspec_origin_row IS NULL) OR "
                "__docspec_origin_file = '' OR __docspec_origin_row < 0)"
            ).fetchone()[0]
            require(not invalid, "invalid native occurrence identity")
            file_expr = "coalesce(__docspec_origin_file, filename)"
            row_expr = "coalesce(__docspec_origin_row, file_row_number)"
        self.view(name + "_located", f"SELECT {columns([f.name for f in public])}, "
                  f"{file_expr} AS __oracle_file, {row_expr} AS __oracle_row FROM {scan}")
        if origins:
            duplicates = self.connection.execute(
                f"SELECT EXISTS (SELECT 1 FROM {q(name + '_located')} GROUP BY "
                "__oracle_file, __oracle_row HAVING count(*) <> 1)"
            ).fetchone()[0]
            require(not duplicates, "duplicate native occurrence identities")
        # Otherwise distinct complete files and zero-based physical row numbers
        # establish uniqueness without a population-sized qualification scan.
        self.record(name + " native membership/types", started, rows=rows, files=len(paths))

    def equal(self, left, right, names, label):
        started = time.monotonic()
        exact_equal(self.connection, left, right, names, label)
        self.record(label, started)

    def release(self, name):
        release = json.loads((self.run / (name + ".stdout.json")).read_text())
        require(release["verified"], "release lacks routine completion")
        manifest = Path(release["manifest"]["path"])
        require(manifest.stat().st_size == release["manifest"]["bytes"], "release manifest size")
        require(json.loads(manifest.read_text()) == release["dataset"], "serialized release differs")
        require(release["dataset"]["version"] == release["id"], "release version differs")
        return release

    def run_checks(self):
        selected = json.loads((self.run / "inputs.json").read_text())["inputs"]
        releases = {name: self.release(name) for name in
                    ["comments", "dockets", "docket-receipts", "comments-dockets", "docket-summary"]}
        for name, item in selected.items():
            admitted = releases[name]["dataset"]["inputs"]["input"]
            self.input("i_" + name, item, admitted)
        for name, release in releases.items():
            self.native("n_" + name, release["dataset"]["groups"]["source"])
        self.view("expected_comments", "SELECT *, CAST(octet_length(encode(comment)) AS BIGINT) "
                  "AS comment_bytes, CAST(octet_length(encode(text_content)) AS BIGINT) "
                  'AS text_content_bytes FROM "i_comments"')
        source_schema = self.schemas["i_comments"]
        require(source_schema.names == COMMENTS, "frozen Comments columns differ")
        expected_schema = pa.schema(list(source_schema) + [pa.field(name, pa.int64()) for name in LENGTHS])
        same_schema(self.schemas["n_comments"], expected_schema, "shaped Comments")
        require(self.tables["n_comments"]["rows"] == ROWS, "Comments population differs")
        self.equal("expected_comments", "n_comments", COMMENTS + LENGTHS, "all shaped Comment values")
        for name in ["dockets", "docket-receipts"]:
            require(releases[name]["dataset"]["relationship"] == COUPLED,
                    "docket source did not retain its generation declaration")
            same_schema(self.schemas["n_" + name], self.schemas["i_" + name], name)
            self.equal("i_" + name, "n_" + name, self.schemas["i_" + name].names,
                       name + " complete values including nested evidence")

        joined = releases["comments-dockets"]["dataset"]["related"]
        summarized = releases["docket-summary"]["dataset"]["related"]
        require(joined["request"] == join(releases), "retained join request differs")
        require(summarized["request"] == summary(releases["comments-dockets"]),
                "retained summary request differs")
        require(joined["coverage"]["inputs"] ==
                {name: version(releases[name]) for name in ["comments", "dockets"]},
                "join input coverage differs")
        require(joined["coverage"]["generations"] == {"docket-evidence": seal(releases)},
                "docket generation association differs")
        require(joined["coverage"]["families"] ==
                {"docket-evidence": sorted(["dockets", "docket-receipts"])},
                "docket generation member set differs")
        require(summarized["coverage"]["inputs"] ==
                {"comments-dockets": version(releases["comments-dockets"])},
                "summary input coverage differs")
        fields = [f'l.__oracle_file AS {q(TUPLE[0])}', f'l.__oracle_row AS {q(TUPLE[1])}',
                  f'r.__oracle_file AS {q(TUPLE[2])}', f'r.__oracle_row AS {q(TUPLE[3])}']
        fields += [f'l.{q(name)} AS {q(name)}' for name in COMMENTS + LENGTHS]
        fields += [f'r.{q(name)} AS {q("docket_" + name)}' for name in DOCKETS]
        self.view("expected_join", f"SELECT {', '.join(fields)} FROM n_comments_located l "
                  'LEFT JOIN n_dockets_located r ON l.docket_id = r.docket_id')
        joined_fields = [pa.field(TUPLE[0], pa.string(), nullable=False),
                         pa.field(TUPLE[1], pa.int64(), nullable=False),
                         pa.field(TUPLE[2], pa.string()), pa.field(TUPLE[3], pa.int64())]
        joined_fields += list(expected_schema)
        joined_fields += [pa.field("docket_" + f.name, f.type, nullable=True)
                          for f in self.schemas["i_dockets"]]
        same_schema(self.schemas["n_comments-dockets"], pa.schema(joined_fields), "joined values")
        require(self.tables["n_comments-dockets"]["rows"] == ROWS, "join changed Comment multiplicity")
        self.equal("expected_join", "n_comments-dockets", TUPLE + JOIN_COLUMNS, "exact native join")
        self.native("join_evidence", joined["evidence"])
        same_schema(self.schemas["join_evidence"], pa.schema(joined_fields[:4]), "join evidence")
        self.equal("expected_join", "join_evidence", TUPLE, "complete join contributor membership")

        self.view("expected_summary", "SELECT docket_id, count(*) AS comment_rows, "
                  "count(docket_docket_id) AS matched_rows, count(comment) AS known_comment_texts, "
                  "sum(CAST(comment_bytes AS DECIMAL(38,0))) AS comment_bytes, "
                  "sum(CAST(text_content_bytes AS DECIMAL(38,0))) AS text_content_bytes, "
                  "max(comment_bytes) AS largest_comment_bytes FROM expected_join GROUP BY docket_id")
        summary_schema = pa.schema([pa.field("docket_id", pa.string()),
            *[pa.field(name, pa.int64(), nullable=False) for name in SUMMARY_COLUMNS[1:4]],
            pa.field("comment_bytes", pa.decimal128(38, 0)),
            pa.field("text_content_bytes", pa.decimal128(38, 0)),
            pa.field("largest_comment_bytes", pa.int64())])
        same_schema(self.schemas["n_docket-summary"], summary_schema, "summary")
        self.equal("expected_summary", "n_docket-summary", SUMMARY_COLUMNS, "exact docket summaries")
        self.native("summary_evidence", summarized["evidence"])
        evidence_columns = ["docket_id", "__docspec_input_file", "__docspec_input_row"]
        same_schema(self.schemas["summary_evidence"], pa.schema([
            pa.field("docket_id", pa.string()), pa.field(evidence_columns[1], pa.string(), nullable=False),
            pa.field(evidence_columns[2], pa.int64(), nullable=False)]), "summary evidence")
        self.view("expected_summary_evidence", "SELECT docket_id, __oracle_file AS __docspec_input_file, "
                  '__oracle_row AS __docspec_input_row FROM "n_comments-dockets_located"')
        self.equal("expected_summary_evidence", "summary_evidence", evidence_columns,
                   "complete summary contributor membership")

        for name in ["comments-dockets", "docket-summary"]:
            pin = json.loads((self.run / (name + "-export.stdout.json")).read_text())
            wanted = export(releases[name])
            require(pin["source"] == wanted["source"] and pin["policy"] == wanted["policy"],
                    "export source/policy differs")
            path = Path(pin["object"]["path"])
            require(path.stat().st_size == pin["object"]["bytes"], "export size differs")
            parquet = pq.ParquetFile(path)
            fields = wanted["policy"]["columns"]
            flat_schema(parquet.schema_arrow, self.schemas["n_" + name], pin["fields"], fields,
                        name + " flat export")
            require(parquet.metadata.num_rows == pin["rows"] == self.tables["n_" + name]["rows"],
                    "export row count differs")
            self.view("flat_" + name, f"SELECT * FROM read_parquet({literal(path)}, hive_partitioning=false)")
            self.equal("n_" + name, "flat_" + name, fields, name + " complete flat output")
        counts = self.connection.execute(
            'SELECT count(*), count(docket_docket_id) FROM "n_comments-dockets"').fetchone()
        require(counts[1] > 0, "real docket join matched no Comments")
        return {"exact_readback_passed": True, "comment_rows": counts[0], "matched_rows": counts[1],
                "unmatched_rows": counts[0] - counts[1], "checks": self.checks, "native_tables": self.tables,
                "evidence_scope": "whole-input Comments provenance; native nested docket receipts bound "
                                  "by generation; exact join and summary contributor relations; "
                                  "no per-docket receipt decoding or unavailable source-body claim"}


def main():
    oracle = Oracle(Path(sys.argv[1]).resolve())
    try:
        result = oracle.run_checks()
    finally:
        oracle.connection.close()
    print(json.dumps(result))


if __name__ == "__main__":
    main()
