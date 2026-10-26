import logging

logger = logging.getLogger(__name__)

KINDS = ("raw", "flat")


def s3_key(kind, dt, name) -> str:
    # Pure: no client, no I/O. The tests and tomorrow's COPY both depend on
    # this layout, so it lives in one place.
    if kind not in KINDS:
        raise ValueError(f"Unknown kind {kind!r}, expected one of {KINDS}")

    return f"weather/{kind}/dt={dt.strftime('%Y-%m-%d')}/{name}"


def upload_bytes(client, bucket, key, body, content_type) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    logger.info("Uploaded %d bytes to s3://%s/%s", len(body), bucket, key)
