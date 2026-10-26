import json
import logging
import os
from pathlib import Path

import boto3

from src.load import FILENAME_TS_FORMAT
from src.s3 import s3_key, upload_bytes

logger = logging.getLogger(__name__)


def make_s3_client():
    # No keys here. boto3 resolves credentials itself: environment variables on
    # the laptop and in Docker, the instance role on EC2.
    return boto3.client("s3", region_name=_required_env("AWS_REGION"))


def bucket_name() -> str:
    return _required_env("S3_BUCKET")


def _required_env(name: str) -> str:
    # Fail loudly. S3 is the path into Snowflake, so a missing setting must stop
    # the run rather than quietly skip the upload.
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not set; it is required for the S3 upload")
    return value


def upload_raw_payloads(client, bucket, payloads, run_ts) -> list[str]:
    ts = run_ts.strftime(FILENAME_TS_FORMAT)
    keys = []

    for payload in payloads:
        key = s3_key("raw", run_ts, f"{payload['city']['id']}_{ts}.json")
        upload_bytes(client, bucket, key, json.dumps(payload).encode("utf-8"), "application/json")
        keys.append(key)

    logger.info("Uploaded %d raw payloads", len(keys))
    return keys


def upload_flat_csv(client, bucket, csv_path, run_ts) -> str:
    path = Path(csv_path)
    key = s3_key("flat", run_ts, path.name)
    upload_bytes(client, bucket, key, path.read_bytes(), "text/csv")
    return key
