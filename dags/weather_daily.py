"""weather_daily: OpenWeather to S3 to Snowflake, once a day at 06:00 UTC.

extract >> transform >> write_to_s3 >> copy_into_snowflake >> validate_row_count
>> volume_anomaly_check >> dbt_source_freshness >> dbt_build

Postgres is no longer on the path. The date a run works on comes from the run
(data_interval_end), never from the clock, so a retry an hour later still loads
the partition it wrote.
"""
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

from src.alerts import notify_failure
from src.extract import extract_records
from src.landing import bucket_name, make_s3_client, upload_flat_frame, upload_raw_payloads
from src.load import output_file_name, stamp_lineage
from src.quality_checks import check_volume_anomaly
from src.snowflake_loader import copy_flat, snowflake_connection
from src.transform import transform_records

DBT_DIR = "/opt/airflow/dbt"

logger = logging.getLogger(__name__)


def on_failure_callback(context):
    # Airflow calls this with the task's context once retries are exhausted.
    # A callback defined without the context parameter never fires; the only
    # trace is one line in the task log.
    ti = context["task_instance"]
    logger.error(
        "TASK FAILED | dag: %s | task: %s | run_id: %s | logical_date: %s | error: %s",
        ti.dag_id,
        ti.task_id,
        context.get("run_id"),
        context.get("logical_date"),
        context.get("exception"),
    )
    # The log line above is where nobody looks. This reaches a phone.
    notify_failure(ti.dag_id, ti.task_id, ti.try_number, getattr(ti, "log_url", None))


@dag(
    dag_id="weather_daily",
    # 06:00 UTC: the run happens while you sleep, and data_interval_end's
    # date is the day the API is answering for.
    schedule="0 6 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    # A whole run normally takes under two minutes. A run still going after an
    # hour is stuck, not slow; the timeout fails it so the alert fires and the
    # next day's run is not blocked by max_active_runs.
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        # A task that is still running after twenty minutes is hung (a stalled
        # API call, a Snowflake login that never returns). The timeout raises
        # AirflowTaskTimeout inside the task: it lands in up_for_retry while
        # retries remain, then failed, and the failure callback fires.
        "execution_timeout": timedelta(minutes=20),
        "on_failure_callback": on_failure_callback,
    },
    tags=["etl", "api", "s3", "snowflake", "dbt"],
)
def weather_daily():

    @task
    def extract():
        return extract_records()

    @task
    def transform(raw_data):
        return transform_records(raw_data)

    @task
    def write_to_s3(raw_data, clean_data) -> dict:
        """Land the raw payloads and the stamped CSV under today's partition.

        The partition date is data_interval_end, from the run. The file
        timestamp is the write time, so every attempt writes a new object name
        and COPY's load metadata never sees the same name with new contents.
        """
        context = get_current_context()
        dt = context["data_interval_end"].date()
        run_id = context["run_id"]
        run_ts = datetime.now(timezone.utc).replace(tzinfo=None)

        client = make_s3_client()
        bucket = bucket_name()
        raw_keys = upload_raw_payloads(client, bucket, raw_data, run_ts, dt=dt)

        file_name = output_file_name(run_ts)
        stamped = stamp_lineage(clean_data, file_name, run_ts, run_id)
        flat_key = upload_flat_frame(client, bucket, stamped, file_name, dt)

        logger.info("dt=%s: %d raw objects, flat %s", dt, len(raw_keys), flat_key)
        return {"dt": dt.isoformat(), "raw_keys": raw_keys, "flat_key": flat_key, "rows": len(stamped)}

    @task
    def copy_into_snowflake(s3_result: dict) -> dict:
        # One connection, one COPY, for the date the S3 task wrote.
        with snowflake_connection() as conn:
            result = copy_flat(conn, s3_result["dt"])
        logger.info("dt=%s: %s", s3_result["dt"], result)
        return asdict(result)

    @task
    def validate_row_count(load_result: dict):
        rows = load_result["rows_loaded"]
        logger.info("Rows loaded this run: %d", rows)
        if rows == 0:
            raise ValueError(
                "COPY loaded 0 rows. Either nothing new landed under this partition "
                "(the file was already loaded; clear write_to_s3 to produce a new one) "
                "or the S3 write did not happen. Aborting before dbt."
            )

    @task
    def volume_anomaly_check(load_result: dict):
        run_id = get_current_context()["run_id"]
        with snowflake_connection() as conn:
            return check_volume_anomaly(conn, run_id)

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
    #
    # No retries: a failed dbt test is a fact about the data, and the data does
    # not change between attempts. Retrying would re-run every model to fail the
    # same way five minutes later. A person has to look.
    dbt_build = BashOperator(
        task_id="dbt_build",
        retries=0,
        bash_command=(
            f"dbt build --project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR} --target ${{DBT_TARGET:-dev}}"
        ),
    )

    raw = extract()
    clean = transform(raw)
    s3_result = write_to_s3(raw, clean)
    load_result = copy_into_snowflake(s3_result)
    validate_row_count(load_result) >> volume_anomaly_check(load_result) >> dbt_source_freshness >> dbt_build


dag = weather_daily()
