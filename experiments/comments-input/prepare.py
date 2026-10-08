"""Freeze full-width physical Comments occurrences using the preregistered rule."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[2]
TARGET_ROWS = 10_000_000
GROUP_ROWS = 4096


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    capture = json.loads(Path(__file__).with_name("local-asset-check.json").read_text())
    source = Path(capture["source"])
    before = source.stat()
    assert before.st_size == capture["bytes"] and capture["matches"]
    original = pq.ParquetFile(source)
    population = original.metadata.num_rows
    assert population == capture["rows_reported"] == 26_418_078
    # The prior capture check supplies identity; do not hash shared source bytes again.
    # Preserve fresh size/mtime observations to detect a change during this preparation.
    record = {
        "source": capture,
        "source_mtime_ns": before.st_mtime_ns,
        "selection": "floor((i+1)*10000000/26418078) > floor(i*10000000/26418078)",
        "target_rows": TARGET_ROWS,
        "row_group_rows": GROUP_ROWS,
        "pyarrow": pa.__version__,
        "source_schema": str(original.schema_arrow),
        "input_bytes": before.st_size,
        "free_disk_before": shutil.disk_usage(args.output).free,
        "complete": False,
    }
    log = args.output / "preparation.json"
    log.write_text(json.dumps(record, indent=2) + "\n")
    output = args.output / "comments.parquet"
    started = time.monotonic()
    writer = None
    try:
        if record["free_disk_before"] < 16 << 30:
            raise RuntimeError("insufficient disk for input preparation and subsequent probe")
        writer = pq.ParquetWriter(output, original.schema_arrow, compression="zstd")
        pending = pa.Table.from_batches([], schema=original.schema_arrow)
        offset = 0
        selected_rows = 0
        for batch in original.iter_batches(batch_size=GROUP_ROWS, use_threads=False):
            positions = np.arange(offset, offset + batch.num_rows, dtype=np.int64)
            mask = ((positions + 1) * TARGET_ROWS // population) > (positions * TARGET_ROWS // population)
            selected = batch.filter(pa.array(mask))
            pending = pa.concat_tables([pending, pa.Table.from_batches([selected])])
            selected_rows += selected.num_rows
            offset += batch.num_rows
            while pending.num_rows >= GROUP_ROWS:
                writer.write_table(pending.slice(0, GROUP_ROWS), row_group_size=GROUP_ROWS)
                pending = pending.slice(GROUP_ROWS)
            if offset % (GROUP_ROWS * 256) == 0:
                if shutil.disk_usage(args.output).free < 8 << 30:
                    raise RuntimeError("disk reserve reached; partial preparation retained")
                print(json.dumps({"source_rows": offset, "selected_rows": selected_rows}), flush=True)
        if pending.num_rows:
            writer.write_table(pending, row_group_size=GROUP_ROWS)
        writer.close()
        writer = None
        with output.open("rb") as stream:
            os.fsync(stream.fileno())
        after = source.stat()
        assert (after.st_size, after.st_mtime_ns) == (before.st_size, before.st_mtime_ns)
        assert offset == population and selected_rows == TARGET_ROWS
        frozen = pq.ParquetFile(output)
        assert frozen.metadata.num_rows == TARGET_ROWS
        assert frozen.schema_arrow.equals(original.schema_arrow, check_metadata=True)
        identity = subprocess.run(
            [str(ROOT / "target/release/docspec"), "hash", str(output)],
            check=True, capture_output=True, text=True, timeout=120,
        )
        record.update({
            "complete": True,
            "path": str(output.resolve()),
            "identity": json.loads(identity.stdout),
            "row_groups": frozen.metadata.num_row_groups,
            "footer_bytes": frozen.metadata.serialized_size,
            "source_rows_scanned": offset,
            "rows_selected": selected_rows,
            "free_disk_after": shutil.disk_usage(args.output).free,
        })
    except Exception as error:
        record["failure"] = repr(error)
        raise
    finally:
        if writer is not None:
            writer.close()
        record["preparation_seconds"] = time.monotonic() - started
        log.write_text(json.dumps(record, indent=2) + "\n")
        print(log, flush=True)


if __name__ == "__main__":
    main()
