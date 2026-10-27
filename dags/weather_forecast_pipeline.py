import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

from src.extract import extract_records
from src.transform import transform_records
from src.load import load_records, output_file_name, stamp_lineage
from src.landing import bucket_name, make_s3_client, upload_flat_csv, upload_raw_payloads
from src.postgres_loader import load_file as load_weather_csv_to_raw_table, get_engine
from src.quality_checks import check_volume_anomaly
from src.utils import RAW_DATA_DIR

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
    def start_run():
        # One timestamp per run, made once and passed to every task that needs
        # it. Each task runs in its own process, so tasks calling
        # datetime.now() themselves would disagree. ISO string for XCom.
        return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    @task
    def extract():
        return extract_records()

    @task
    def upload_raw(raw_data, run_ts: str):
        return upload_raw_payloads(
            make_s3_client(), bucket_name(), raw_data, datetime.fromisoformat(run_ts)
        )

    @task
    def transform(raw_data):
        return transform_records(raw_data)

    @task
    def load(clean_data, run_ts: str):
        # The file name comes from the shared run timestamp, in the same
        # collision-safe format load_records uses. RAW_DATA_DIR resolves to
        # /opt/airflow/data/raw inside the container.
        context = get_current_context()
        dag_run_id = context["dag_run"].run_id if context.get("dag_run") else None
        ts = datetime.fromisoformat(run_ts)
        file_name = output_file_name(ts)
        stamped = stamp_lineage(clean_data, file_name, ts, dag_run_id)
        return load_records(stamped, RAW_DATA_DIR / file_name)

    @task
    def upload_flat(file_path: str, run_ts: str):
        return upload_flat_csv(
            make_s3_client(), bucket_name(), file_path, datetime.fromisoformat(run_ts)
        )

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
    def validate_row_count(load_result: dict):
        if load_result["status"] == "skipped":
            logger.info("File was already loaded, skipping row count check")
            return

        row_count = load_result["rows"]
        logger.info("Rows loaded this run: %d", row_count)

        if row_count == 0:
            raise ValueError("Load task reported 0 rows, aborting before dbt run")

    @task
    def volume_anomaly_check(load_result: dict):
        if load_result["status"] == "skipped":
            logger.info("File was already loaded, skipping volume anomaly check")
            return

        context = get_current_context()
        return check_volume_anomaly(get_engine(), context["dag_run"].run_id)

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

    # The S3 uploads branch off rather than sit in the chain: a failed upload
    # fails the run, but Postgres and dbt still get today's data.
    run_ts = start_run()
    raw = extract()
    upload_raw(raw, run_ts)
    clean = transform(raw)
    file_path = load(clean, run_ts)
    upload_flat(file_path, run_ts)
    load_result = load_raw_table(file_path)
    validate_row_count(load_result) >> volume_anomaly_check(load_result) >> dbt_source_freshness >> dbt_build


dag = weather_forecast_pipeline()
