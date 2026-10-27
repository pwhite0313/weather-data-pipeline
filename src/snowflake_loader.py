"""Load the flat CSVs from the S3 stage into RAW.WEATHER_FORECAST.

Replaces src/postgres_loader.py in the DAG. One job: a COPY INTO for one date
partition, with a result the caller can trust. Retries belong to Airflow, not
here; nothing in this module retries or swallows an error.
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date

import snowflake.connector
from snowflake.connector.errors import ProgrammingError

logger = logging.getLogger(__name__)

STAGE = "RAW.S3_FLAT_STAGE"
TABLE = "RAW.WEATHER_FORECAST"
FILE_FORMAT = "RAW.CSV_PARSE_HEADER"

# Every connection value comes from the environment. The load warehouse has a
# default because .env's SNOWFLAKE_WAREHOUSE is dbt's TRANSFORM_WH.
REQUIRED_ENV = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_DATABASE",
)
LOAD_WAREHOUSE_ENV = "SNOWFLAKE_LOAD_WAREHOUSE"
LOAD_WAREHOUSE_DEFAULT = "LOAD_WH"

# A hung login or a stalled network call fails the task instead of holding it.
LOGIN_TIMEOUT_SECONDS = 30
NETWORK_TIMEOUT_SECONDS = 120


class LoadError(Exception):
    """A COPY that Snowflake rejected. The message carries the date, the stage
    path and the query id, so the task log alone is enough to open the query
    in Snowsight."""


@dataclass(frozen=True)
class LoadResult:
    files_loaded: int
    rows_loaded: int
    rows_parsed: int
    errors_seen: int


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not set; the Snowflake loader needs it")
    return value


@contextmanager
def snowflake_connection():
    """Key pair connection as the service user, closed on every path."""
    settings = {name: _required_env(name) for name in REQUIRED_ENV}
    warehouse = os.getenv(LOAD_WAREHOUSE_ENV, LOAD_WAREHOUSE_DEFAULT)

    conn = snowflake.connector.connect(
        account=settings["SNOWFLAKE_ACCOUNT"],
        user=settings["SNOWFLAKE_USER"],
        private_key_file=settings["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=settings["SNOWFLAKE_ROLE"],
        warehouse=warehouse,
        database=settings["SNOWFLAKE_DATABASE"],
        schema="RAW",
        login_timeout=LOGIN_TIMEOUT_SECONDS,
        network_timeout=NETWORK_TIMEOUT_SECONDS,
    )
    session_id = conn.session_id
    logger.info(
        "Snowflake session %s opened as %s, role %s, warehouse %s",
        session_id, settings["SNOWFLAKE_USER"], settings["SNOWFLAKE_ROLE"], warehouse,
    )
    try:
        yield conn
    finally:
        conn.close()
        logger.info("Snowflake session %s closed", session_id)


def stage_path(dt: str) -> str:
    """The stage path for one date partition.

    The path cannot be a bind variable, so this validation is the injection
    guard: anything that is not an ISO date never reaches the SQL string.
    """
    day = date.fromisoformat(dt)
    return f"@{STAGE}/dt={day.isoformat()}/"


def copy_sql(dt: str) -> str:
    return (
        f"COPY INTO {TABLE} "
        f"FROM {stage_path(dt)} "
        f"FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT}) "
        f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
        f"ON_ERROR = ABORT_STATEMENT"
    )


def copy_flat(conn, dt: str) -> LoadResult:
    """COPY one date partition of flat CSVs into the raw table.

    Load metadata makes this safe to call again for the same date: files
    already loaded are skipped, and the result then says zero files and zero
    rows. Raises LoadError when Snowflake rejects the COPY, ValueError when dt
    is not an ISO date.
    """
    sql = copy_sql(dt)
    path = stage_path(dt)
    logger.info("COPY from %s", path)

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        # By name, never by position: when nothing is new the result is one
        # status row with no rows_loaded column at all.
        columns = [d[0].lower() for d in cursor.description]
        rows = [dict(zip(columns, values)) for values in cursor.fetchall()]
    except ProgrammingError as e:
        raise LoadError(
            f"COPY for dt={dt} from {path} failed, query id {cursor.sfqid}: {e.msg}"
        ) from e
    finally:
        query_id = cursor.sfqid
        cursor.close()

    if "rows_loaded" not in columns:
        logger.info("COPY query %s: nothing new under %s", query_id, path)
        return LoadResult(files_loaded=0, rows_loaded=0, rows_parsed=0, errors_seen=0)

    loaded = [r for r in rows if r.get("status") == "LOADED"]
    result = LoadResult(
        files_loaded=len(loaded),
        rows_loaded=sum(r["rows_loaded"] for r in loaded),
        rows_parsed=sum(r["rows_parsed"] for r in loaded),
        errors_seen=sum(r["errors_seen"] for r in rows),
    )
    logger.info(
        "COPY query %s: %d file(s), %d rows loaded of %d parsed, %d errors",
        query_id, result.files_loaded, result.rows_loaded, result.rows_parsed, result.errors_seen,
    )
    return result


if __name__ == "__main__":
    import argparse

    from src.logging_config import setup_logging

    parser = argparse.ArgumentParser(description="COPY one dt= partition into RAW.WEATHER_FORECAST")
    parser.add_argument("--dt", required=True, help="partition date, YYYY-MM-DD")
    args = parser.parse_args()

    setup_logging()
    with snowflake_connection() as connection:
        print(copy_flat(connection, args.dt))
