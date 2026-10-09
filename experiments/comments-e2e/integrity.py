"""One bounded read establishes retained byte identity with maintained hash code."""

import hashlib
import io
import os
import stat
from pathlib import Path

import blake3


def file_stat(value):
    return {key: getattr(value, "st_" + key)
            for key in ["dev", "ino", "size", "mtime_ns", "ctime_ns", "mode"]}


def capture_bytes(path, output):
    """Copy and hash one selected source, refusing observed replacement/mutation."""
    path = Path(path).resolve(strict=True)
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        before = file_stat(os.fstat(stream.fileno()))
        if not stat.S_ISREG(before["mode"]):
            raise ValueError("capture requires a regular file")
        for block in iter(lambda: stream.read(1 << 20), b""):
            if output.write(block) != len(block):
                raise OSError("short write while capturing selected source")
            digest.update(block)
            size += len(block)
        after = file_stat(os.fstat(stream.fileno()))
    current = file_stat(path.stat())
    if before != after or after != current or size != after["size"]:
        raise ValueError("source changed during capture")
    return {"source": str(path), "source_stat": before, "bytes": size,
            "sha256": digest.hexdigest(), "payload_passes": 1}


def read_source(path):
    """Keep the exact small source bytes that will be compiled in this process."""
    output = io.BytesIO()
    identity = capture_bytes(path, output)
    return output.getvalue(), identity


def copy_frozen_file(source, destination):
    """Create a series-owned read-only copy and its identity in a single pass."""
    destination = Path(destination)
    with destination.open("xb") as output:
        identity = capture_bytes(source, output)
        output.flush()
        os.fsync(output.fileno())
    destination.chmod(0o555 if identity["source_stat"]["mode"] & 0o111 else 0o444)
    return {**identity, "path": str(destination), "captured_stat": file_stat(destination.stat())}


def save_loaded_source(destination, source, identity):
    """Retain already captured bytes; a changed worktree cannot replace them."""
    destination = Path(destination)
    if len(source) != identity["bytes"]:
        raise ValueError("loaded source size differs from its capture")
    with destination.open("xb") as output:
        if output.write(source) != len(source):
            raise OSError("short write while retaining loaded source")
        output.flush()
        os.fsync(output.fileno())
    destination.chmod(0o444)
    return {**identity, "path": str(destination), "captured_stat": file_stat(destination.stat()),
            "source_was_loaded_before_series": True}


def file_digests(path, *, sha256=False):
    """Compute BLAKE3 and optionally SHA-256 together, refusing observed mutation."""
    path = Path(path)
    digest = blake3.blake3()
    source_sha = hashlib.sha256() if sha256 else None
    size = 0
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
            if source_sha is not None:
                source_sha.update(block)
            size += len(block)
        after = os.fstat(stream.fileno())
    current = path.stat()
    identity = lambda stat: (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
    if identity(before) != identity(after) or identity(after) != identity(current) or size != after.st_size:
        raise ValueError("file changed while its byte identity was established")
    result = {"bytes": size, "blake3": digest.hexdigest()}
    if source_sha is not None:
        result["sha256"] = source_sha.hexdigest()
    return result
