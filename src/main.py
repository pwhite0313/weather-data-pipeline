import logging
import time
import sys
import json
from datetime import datetime, timezone

from src.logging_config import setup_logging
from src.extract import extract_records
from src.transform import transform_records
from src.load import load_records, output_file_name, stamp_lineage
from src.landing import bucket_name, make_s3_client, upload_flat_csv, upload_raw_payloads
from src.postgres_loader import load_all_files
from src.utils import RAW_DATA_DIR

logger = logging.getLogger(__name__)

def main():

    start_time = time.time()

    # One moment for the whole run: file names, the dt= partition, ingested_at
    # and dag_run_id all derive from start_time. time.time() is epoch seconds,
    # so this is UTC on the laptop and in the container alike.
    run_ts = datetime.fromtimestamp(start_time, tz=timezone.utc).replace(tzinfo=None)
    dag_run_id = f"manual_{int(start_time)}"

    logger.info("=== Starting ingestion pipeline ===")

    # Start API client and extract
    try:
        # Before extract, so a missing S3 setting fails before any API calls
        s3 = make_s3_client()
        bucket = bucket_name()

        raw_records = extract_records()
        logger.info("Extracted %s raw records", len(raw_records))

        # Log empty response and exit
        if not raw_records:
            logger.warning("No records returned")
            return

        # Raw lands before transform, so it survives a transform bug
        upload_raw_payloads(s3, bucket, raw_records, run_ts)

        # Transform
        clean_records = transform_records(raw_records)
        logger.info("Transformed %s records", len(clean_records))

        # Load
        file_name = output_file_name(run_ts)
        stamped = stamp_lineage(clean_records, file_name, run_ts, dag_run_id)
        output_path = load_records(stamped, RAW_DATA_DIR / file_name)
        logger.info("Loaded records to %s", output_path)

        upload_flat_csv(s3, bucket, output_path, run_ts)

        load_all_files(dag_run_id=dag_run_id)
        logger.info("Files in data/raw loaded to Postgres")


        # Pipline run time
        elapsed = time.time() - start_time

        logger.info("=== Pipeline completed successfully in %.2f second s===", elapsed)

    except Exception as e:
        logger.exception("Pipline failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    setup_logging()
    main()
