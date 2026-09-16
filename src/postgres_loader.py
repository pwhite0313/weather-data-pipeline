from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone
import logging

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.logging_config import setup_logging
from src.schema import conform_to_schema
from src.utils import RAW_DATA_DIR


RAW_SCHEMA = "raw"
RAW_TABLE = "weather_forecast"

setup_logging()
logger = logging.getLogger(__name__)


def get_engine() -> Engine:
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("weather_warehouse")
        db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        logger.info("Built database URL from Airflow connection: weather_warehouse")
        return create_engine(db_url)
    except Exception:
        logger.info("Airflow connection not available — falling back to environment variables")

    db_url = os.getenv("POSTGRES_DBT_URL") or os.getenv("DATABASE_URL")

    if not db_url:
        user = os.getenv("WAREHOUSE_USER")
        password = os.getenv("WAREHOUSE_PASSWORD")
        host = os.getenv("WAREHOUSE_HOST")
        port = os.getenv("WAREHOUSE_PORT", "5432")
        db = os.getenv("WAREHOUSE_DB")

        if not all([user, password, host, db]):
            logger.error("Missing one or more warehouse connection environment variables")
            raise ValueError(
                "Missing one or more warehouse connection environment variables: "
                "WAREHOUSE_USER, WAREHOUSE_PASSWORD, WAREHOUSE_HOST, WAREHOUSE_DB"
            )

        db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        logger.info("Built database URL from individual environment variables")

    logger.info("Creating database engine")
    return create_engine(db_url)


def parse_source_file_ts(file_name: str) -> datetime:
    stem = Path(file_name).stem

    if not stem.startswith("output_"):
        raise ValueError(f"Unexpected file name format: {file_name}")

    ts_part = stem.replace("output_", "", 1)
    return datetime.strptime(ts_part, "%Y%m%d_%H%M%S")


def ensure_raw_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))


def align_columns_to_table(engine: Engine, df: pd.DataFrame) -> pd.DataFrame:
    with engine.connect() as conn:
        table_exists = conn.execute(text("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :table
            LIMIT 1
        """), {"schema": RAW_SCHEMA, "table": RAW_TABLE}).first()

        if not table_exists:
            return df

        known_columns = {
            row[0] for row in conn.execute(text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
            """), {"schema": RAW_SCHEMA, "table": RAW_TABLE})
        }

    new_columns = [c for c in df.columns if c not in known_columns]
    if new_columns:
        logger.warning(
            "Dropping %d unknown column(s) not present in %s.%s: %s — "
            "run ALTER TABLE to start capturing this data",
            len(new_columns), RAW_SCHEMA, RAW_TABLE, new_columns,
        )
        df = df.drop(columns=new_columns)

    return df


def file_already_loaded(engine: Engine, source_file_name: str) -> bool:
    logger.info("Checking if file was already loaded: %s", source_file_name)

    with engine.connect() as conn:
        table_exists = conn.execute(text("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :table
            LIMIT 1
        """), {"schema": RAW_SCHEMA, "table": RAW_TABLE}).first()

        if not table_exists:
            logger.info("Table does not exist yet — treating as not loaded")
            return False

        result = conn.execute(text(
            f"SELECT 1 FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE source_file_name = :source_file_name LIMIT 1"
        ), {"source_file_name": source_file_name}).first()

    already_loaded = result is not None
    logger.info("File already loaded: %s", already_loaded)
    return already_loaded


def format_load_result(result: dict) -> str:
    if result["status"] == "skipped":
        return f"Skipped already loaded file: {result['source_file']}"

    return (
        f"Loaded {result['rows']} rows from {result['source_file']} "
        f"into {result['table']}"
    )


def load_file(
    file_path: str,
    dag_run_id: str | None = None,
    skip_if_loaded: bool = True,
) -> dict:
    logger.info("Starting raw table load")
    logger.info("Input file path: %s", file_path)
    logger.info("DAG run ID: %s", dag_run_id)

    path = Path(file_path)

    if not path.exists():
        logger.error("File not found: %s", file_path)
        raise FileNotFoundError(f"File not found: {file_path}")

    engine = get_engine()
    ensure_raw_schema(engine)
    source_file_name = path.name
    source_file_ts = parse_source_file_ts(source_file_name)

    logger.info("Source file name: %s", source_file_name)
    logger.info("Source file timestamp: %s", source_file_ts)

    if skip_if_loaded and file_already_loaded(engine, source_file_name):
        logger.warning("Skipped already loaded file: %s", source_file_name)
        return {
            "status": "skipped",
            "rows": 0,
            "source_file": source_file_name,
            "table": f"{RAW_SCHEMA}.{RAW_TABLE}",
        }

    logger.info("Reading CSV")
    df = pd.read_csv(path)
    logger.info("Rows read from CSV: %s", len(df))

    # Applied here as well as in transform so that CSVs written before the
    # schema contract existed still create a complete raw table. Runs before the
    # lineage columns below, which are deliberately not part of the contract.
    df = conform_to_schema(df)

    df["source_file_name"] = source_file_name
    df["source_file_ts"] = source_file_ts
    df["ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    df["dag_run_id"] = dag_run_id

    df = align_columns_to_table(engine, df)
    logger.info("Writing rows to %s.%s", RAW_SCHEMA, RAW_TABLE)

    df.to_sql(
        name=RAW_TABLE,
        con=engine,
        schema=RAW_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    result = {
        "status": "loaded",
        "rows": len(df),
        "source_file": source_file_name,
        "table": f"{RAW_SCHEMA}.{RAW_TABLE}",
    }
    logger.info(format_load_result(result))

    return result


def load_all_files(dag_run_id: str | None = None) -> list[dict]:
    if dag_run_id == "":
        dag_run_id = None

    csv_files = sorted(RAW_DATA_DIR.glob("output_*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No output CSV files found in {RAW_DATA_DIR}")

    results = []

    for file_path in csv_files:
        try:
            logger.info("Loading file: %s", file_path)
            result = load_file(file_path=str(file_path), dag_run_id=dag_run_id)
            results.append(result)
        except Exception:
            logger.exception("Failed on file: %s", file_path)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    single = subparsers.add_parser("file", help="Load a single file")
    single.add_argument("--file-path", required=True)
    single.add_argument("--dag-run-id", required=False, default=None)

    subparsers.add_parser("all", help="Load all files in data/raw")

    args = parser.parse_args()

    if args.command == "file":
        print(format_load_result(
            load_file(file_path=args.file_path, dag_run_id=args.dag_run_id)
        ))
    elif args.command == "all":
        for r in load_all_files():
            print(format_load_result(r))
    else:
        parser.print_help()
