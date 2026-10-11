#!/usr/bin/env python3
"""Trusted, opt-in exact-key R2 JWT issuer/probe for the synthetic fixture only."""
import base64
import hashlib
import hmac
import json
import os
import re
import signal
import sys
import time
from urllib.parse import urlsplit


def fail():
    raise RuntimeError("scoped authority qualification refused")


def encoded(value):
    return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).rstrip(b"=").decode()


def key(uri, bucket):
    path = urlsplit(uri)
    if path.scheme != "s3" or path.netloc != bucket or path.query or path.fragment:
        fail()
    value = path.path.removeprefix("/")
    if not value or any(p in ("", ".", "..") for p in value.split("/")):
        fail()
    if not re.match(r"^__r2_data_catalog/docspec-qualification/docspec-rs-qualification-[0-9a-f-]{36}/maintenance/", value):
        fail()
    return value


def endpoint(value):
    parsed = urlsplit(value)
    if parsed.scheme != "https" or parsed.path not in ("", "/") or parsed.query or parsed.fragment or parsed.username or parsed.port:
        fail()
    if not re.fullmatch(r"[0-9a-f]{32}\.r2\.cloudflarestorage\.com", parsed.hostname or ""):
        fail()
    return parsed.hostname


def mint(request):
    host = endpoint(request["endpoint"])
    current = int(time.time())
    expires = request["expires"]
    if not current < expires <= current + 900 or not 0 < len(request["objects"]) <= 8:
        fail()
    paths = [key(item["path"], request["bucket"]) for item in request["objects"]]
    if len(set(paths)) != len(paths):
        fail()
    access = os.environ["DOCSPEC_S3_ACCESS_KEY_ID"]
    secret = os.environ["DOCSPEC_S3_SECRET_ACCESS_KEY"]
    payload = {"sub": host.split(".")[0], "iss": access, "aud": host, "iat": current,
               "exp": expires, "bucket": request["bucket"],
               "actions": ["HeadObject", "DeleteObject"],
               "paths": {"prefixPaths": [], "objectPaths": paths}}
    unsigned = encoded({"alg": "HS256", "typ": "JWT"}) + "." + encoded(payload)
    signature = base64.urlsafe_b64encode(hmac.new(secret.encode(), unsigned.encode(), hashlib.sha256).digest()).rstrip(b"=").decode()
    token = unsigned + "." + signature
    return {"access_key_id": access, "secret_access_key": hashlib.sha256(token.encode()).hexdigest(),
            "session_token": base64.b64encode(("jwt/" + token).encode()).decode()}


def probe(request):
    # This process receives temporary credentials only; parent/catalog secrets
    # are absent from its environment. It cannot issue a new session itself.
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    endpoint(request["endpoint"])
    credentials = request["credentials"]
    client = boto3.client("s3", endpoint_url=request["endpoint"], region_name="auto",
        aws_access_key_id=credentials["access_key_id"],
        aws_secret_access_key=credentials["secret_access_key"],
        aws_session_token=credentials["session_token"],
        config=Config(connect_timeout=5, read_timeout=10, retries={"total_max_attempts": 1}, s3={"addressing_style": "path"}))
    bucket = request["bucket"]
    allowed, wrong, canary = [key(request[name], bucket) for name in ("allowed", "wrong", "canary")]
    if len({allowed, wrong, canary}) != 3 or "/issuer-canary-" not in canary:
        fail()
    def status(method, **args):
        try:
            getattr(client, method)(Bucket=bucket, **args)
            return 200
        except ClientError as error:
            return int(error.response["ResponseMetadata"]["HTTPStatusCode"])
    before = status("head_object", Key=allowed)
    outside = status("head_object", Key=wrong)
    payload = status("get_object", Key=allowed)
    write = status("put_object", Key=canary, Body=b"")
    if (before, outside, payload, write) != (404, 403, 403, 403):
        fail()
    still_live = status("head_object", Key=allowed)
    if still_live != 404:
        fail()
    delay = request["expires"] + 2 - time.time()
    if not 0 < delay <= 22:
        fail()
    time.sleep(delay)
    expired = status("head_object", Key=allowed)
    if expired != 403:
        fail()
    return {"allowed_absent_head": before, "outside_head": outside,
            "payload_get": payload, "new_key_put": write, "still_live_absent_head": still_live, "expired_head": expired}


def main():
    signal.signal(signal.SIGALRM, lambda *_: fail())
    signal.alarm(55)
    raw = sys.stdin.buffer.read(16385)
    if len(raw) > 16384:
        fail()
    request = json.loads(raw)
    mode = sys.argv[1]
    value = mint(request) if mode == "mint" else probe(request) if mode == "probe" else fail()
    print(json.dumps(value, separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # No endpoint, upstream body or secret traceback enters retained logs.
        print("scoped authority qualification refused", file=sys.stderr)
        sys.exit(1)
