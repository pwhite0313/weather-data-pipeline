import logging
import os
from datetime import datetime
from pathlib import Path

from src.utils import RAW_DATA_DIR

logger = logging.getLogger(__name__)

# Microseconds, not just seconds. Two DAG runs starting within the same second
# used to resolve to the same path and interleave their writes. A uuid suffix
# was added for this in ad61922 and removed again in 193121a to keep the
# filename parseable, which reintroduced the collision. Sub-second precision
# keeps names unique without giving up a parseable timestamp.
FILENAME_TS_FORMAT = "%Y%m%d_%H%M%S_%f"


def load_records(records, output_path=None) -> str:
    if output_path is None:
        timestamp = datetime.now().strftime(FILENAME_TS_FORMAT)
        output_path = RAW_DATA_DIR / f"output_{timestamp}.csv"

    output_path = Path(output_path)

    logger.info("Starting load")
    logger.info("Writing records to %s", output_path)

    # Write to a temp file alongside the target, then rename. os.replace is
    # atomic within a filesystem, so a reader never observes a half-written CSV
    # and an interrupted run leaves a stray temp file rather than a truncated
    # one that would later load as garbage rows.
    tmp_path = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")

    try:
        with open(tmp_path, "w", encoding="utf-8", newline="") as f:
            records.to_csv(f, index=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, output_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    logger.info("%s loaded", len(records))

    return str(output_path)
