#!/usr/bin/env python3
"""Qualify R2 create-only primitives in a fresh, retained synthetic prefix.

Protocol: race two distinct conditional PUTs; retry the winner; complete one
multipart upload; attempt a delayed competing multipart completion. Each accepted
object receives one exact readback after its competing writes. Expect exactly one
PUT winner, precondition refusals, and unchanged winner bytes. This establishes
provider behavior through boto3, not Rust integration or throughput. No policy,
catalog, production object, or deletion is involved. Incomplete uploads are retained.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from threading import Barrier
import uuid

import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import dotenv_values


def run(args):
    suffix = args.allow_synthetic_writes.removeprefix("docspec-rs-qualification-")
    if str(uuid.UUID(suffix)) != suffix or not args.allow_synthetic_writes.startswith(
        "docspec-rs-qualification-"
    ):
        raise ValueError("canonical synthetic qualification UUID required")
    # Exclusive evidence creation precedes any remote write; do not repeat an
    # unknown attempt by reusing its local report path.
    with args.output.open("x", encoding="utf-8") as log:
        def record(event, **fields):
            log.write(json.dumps({"event": event, **fields}, sort_keys=True) + "\n")
            log.flush()

        config = dotenv_values(args.config)
        bucket = config["R2_BUCKET_NAME"]
        prefix = f"docspec-qualification/{args.allow_synthetic_writes}/conditional-create/"
        client = boto3.client(
            "s3", endpoint_url=config["R2_ENDPOINT"], region_name="auto",
            aws_access_key_id=config["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=config["R2_SECRET_ACCESS_KEY"],
            config=Config(connect_timeout=15, read_timeout=30,
                          retries={"total_max_attempts": 1},
                          s3={"addressing_style": "path"},
                          request_checksum_calculation="when_required",
                          response_checksum_validation="when_required"),
        )
        record("start", bucket=bucket, prefix=prefix, boto3=boto3.__version__,
               botocore=botocore.__version__, retry_attempts=0,
               started_utc=datetime.now(timezone.utc).isoformat(),
               script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               scope="provider primitives only; synthetic objects retained")
        existing = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        pending = client.list_multipart_uploads(Bucket=bucket, Prefix=prefix, MaxUploads=1)
        if existing.get("Contents") or pending.get("Uploads"):
            record("refused", reason="synthetic prefix already contains an attempt")
            raise ValueError("fresh synthetic prefix required")

        def call(method, **kwargs):
            try:
                result = getattr(client, method)(Bucket=bucket, **kwargs)
                return {"status": result["ResponseMetadata"]["HTTPStatusCode"],
                        "code": "ok"}, result
            except ClientError as error:
                # No error bodies/headers or request configuration in retained logs.
                return {"status": error.response["ResponseMetadata"]["HTTPStatusCode"],
                        "code": error.response["Error"]["Code"]}, None

        def readback(key, expected):
            response = client.get_object(Bucket=bucket, Key=key)
            digest = hashlib.sha256()
            count = 0
            with response["Body"] as body:
                for chunk in iter(lambda: body.read(1 << 20), b""):
                    count += len(chunk)
                    digest.update(chunk)
            matches = count == len(expected) and digest.digest() == hashlib.sha256(expected).digest()
            record("readback", key=key, bytes=count, sha256=digest.hexdigest(), exact=matches)
            if not matches:
                raise AssertionError("conditional write changed winner bytes")

        key = prefix + "single.bin"
        bodies = [b"synthetic-first-writer\n" * 32, b"synthetic-second-writer\n" * 32]
        barrier = Barrier(2)

        def put(index):
            barrier.wait(timeout=30)
            outcome, _ = call("put_object", Key=key, Body=bodies[index], IfNoneMatch="*")
            return index, outcome

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(put, range(2)))
        record("concurrent_put", key=key, outcomes=outcomes)
        winners = [i for i, outcome in outcomes if outcome["status"] == 200]
        if len(winners) != 1 or any(
            outcome["status"] not in (200, 412) for _, outcome in outcomes
        ):
            raise AssertionError("conditional PUT did not produce one winner and one refusal")
        winner = winners[0]
        repeated, _ = call("put_object", Key=key, Body=bodies[winner], IfNoneMatch="*")
        record("repeat_conditional_put", key=key, **repeated)
        readback(key, bodies[winner])
        if repeated["status"] != 412:
            raise AssertionError("conditional retry was not refused")

        key = prefix + "multipart.bin"
        uploads = []
        # Both uploads start before selection of the winning bytes. Their final
        # completions then model a delayed competing writer. This does not inject
        # transport loss. Two parts exercise actual multipart storage.
        for writer in range(2):
            created = client.create_multipart_upload(Bucket=bucket, Key=key)
            upload = created["UploadId"]
            record("multipart_started", key=key, writer=writer, upload_id=upload)
            parts = []
            for number, payload in enumerate((bytes([65 + writer]) * (5 << 20),
                                               f"synthetic-tail-{writer}".encode()), 1):
                result = client.upload_part(Bucket=bucket, Key=key, UploadId=upload,
                                            PartNumber=number, Body=payload)
                parts.append({"ETag": result["ETag"], "PartNumber": number})
            uploads.append((upload, parts))
        statuses = []
        for writer, (upload, parts) in enumerate(uploads):
            outcome, _ = call("complete_multipart_upload", Key=key, UploadId=upload,
                              MultipartUpload={"Parts": parts}, IfNoneMatch="*")
            record("multipart_completion", key=key, writer=writer,
                   upload_id=upload, **outcome)
            statuses.append(outcome["status"])
        # Read even if the provider ignores the condition, so failure evidence
        # identifies whether the selected bytes changed.
        readback(key, b"A" * (5 << 20) + b"synthetic-tail-0")
        if statuses != [200, 412]:
            raise AssertionError("conditional multipart completion was not preserved")
        record("complete", passed=True, retained_incomplete_upload=uploads[1][0])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-synthetic-writes", required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        run(arguments)
    except Exception as error:
        # Provider exception strings may contain sensitive configuration.
        print(f"conditional-create probe failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
