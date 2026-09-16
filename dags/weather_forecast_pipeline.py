import logging
import re
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

from src.extract import extract_records
from src.transform import transform_records
from src.load import load_records
from src.postgres_loader import load_file as load_weather_csv_to_raw_table, get_engine

DBT_DIR = "/opt/airflow/dbt"

logger = logging.getLogger(__name__)


def on_failure_callback(context):
    ti = context["task_instance"]
    logger.error(
        "TASK FAILED | dag: %s | task: %s | run_id: %s | execution_date: %s | error: %s",
        ti.dag_id,
        ti.task_id,
        context.get("run_id"),
        context.get("execution_date"),
        context.get("exception"),
    )


@dag(
    dag_id="weather_forecast_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
    },
    tags=["etl", "api", "csv", "postgres", "dbt"],
)
def weather_forecast_pipeline():

    @task
    def extract():
        return extract_records()

    @task
    def transform(raw_data):
        return transform_records(raw_data)

    @task
    def load(clean_data):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/opt/airflow/data/raw/output_{timestamp}.csv"
        load_records(clean_data, output_path)
        return output_path

    @task
    def load_raw_table(file_path: str):
        context = get_current_context()
        dag_run_id = context["dag_run"].run_id if context.get("dag_run") else None
        return load_weather_csv_to_raw_table(
            file_path=file_path,
            dag_run_id=dag_run_id,
            skip_if_loaded=True,
        )

    @task
    def validate_row_count(load_result: str):
        logger = logging.getLogger(__name__)

        if load_result.startswith("Skipped"):
            logger.info("File was already loaded — skipping row count check")
            return

        match = re.search(r"Loaded (\d+) rows", load_result)
        if not match:
            raise ValueError(f"Unexpected load result format: {load_result}")

        row_count = int(match.group(1))
        logger.info("Rows loaded this run: %d", row_count)

        if row_count == 0:
            raise ValueError("Load task reported 0 rows — aborting before dbt run")

    @task
    def volume_anomaly_check(load_result: str):
        from sqlalchemy import text
        task_logger = logging.getLogger(__name__)

        if load_result.startswith("Skipped"):
            task_logger.info("File was already loaded — skipping volume anomaly check")
            return

        context = get_current_context()
        current_run_id = context["dag_run"].run_id

        engine = get_engine()
        with engine.connect() as conn:
            current_count = conn.execute(text("""
                SELECT COUNT(*) FROM raw.weather_forecast
                WHERE dag_run_id = :run_id
            """), {"run_id": current_run_id}).scalar()

            recent_runs = conn.execute(text("""
                SELECT dag_run_id, COUNT(*) as row_count
                FROM raw.weather_forecast
                WHERE dag_run_id != :run_id
                  AND dag_run_id IS NOT NULL
                GROUP BY dag_run_id
                ORDER BY MIN(ingested_at) DESC
                LIMIT 7
            """), {"run_id": current_run_id}).fetchall()

        if len(recent_runs) < 3:
            task_logger.info(
                "Not enough run history for anomaly check (%d previous runs found) — skipping",
                len(recent_runs)
            )
            return

        rolling_avg = sum(r.row_count for r in recent_runs) / len(recent_runs)
        threshold = rolling_avg * 0.5

        task_logger.info(
            "Volume check: current=%d, rolling_avg=%.1f, threshold=%.1f",
            current_count, rolling_avg, threshold
        )

        if current_count < threshold:
            raise ValueError(
                f"Volume anomaly detected: current run ({current_run_id}) loaded {current_count} rows, "
                f"more than 50% below rolling average of {rolling_avg:.1f}"
            )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"dbt source freshness --project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR} --target ${{DBT_TARGET:-dev}}"
        ),
    )

    # `dbt build` rather than a `dbt run` -> `dbt test` pair: build interleaves
    # run and test per node in DAG order, so a failing test on a staging model
    # stops the marts and reports from being built on data already known to be bad.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"dbt build --project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR} --target ${{DBT_TARGET:-dev}}"
        ),
    )

    raw = extract()
    clean = transform(raw)
    file_path = load(clean)
    load_result = load_raw_table(file_path)
    validate_row_count(load_result) >> volume_anomaly_check(load_result) >> dbt_source_freshness >> dbt_build


dag = weather_forecast_pipeline()
