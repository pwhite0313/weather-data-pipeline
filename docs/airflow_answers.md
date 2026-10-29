# Airflow answers

Short answers to the questions the DAG's design raises, written when the
decision was made.

## Why the run's date is `data_interval_end`, not `ds` (2026-10-27)

`weather_daily` runs at 06:00 UTC on schedule `0 6 * * *`. Airflow names each
run by the *start* of the interval it covers, so the run that executes on the
28th at 06:00 has `logical_date` (and therefore `ds`) of the 27th. Its
`data_interval_end` is the 28th at 06:00.

The OpenWeather forecast endpoint only ever answers for now. The payload the
28th's run fetches is the 28th's forecast, so the partition it lands in must
be `dt=2026-10-28`. Using `ds` would file every run under the previous day.

`data_interval_end` is read once, in `write_to_s3`, and passed on to
`copy_into_snowflake` through XCom. A retry of the load an hour later, or the
next morning, still loads the partition the S3 task wrote, because the date
travels with the run rather than being read from the clock.

For a manual trigger on a DAG with a cron schedule, Airflow 2.8 does not use
the trigger time as the interval. It uses the most recent completed interval:
a trigger at 19:28 UTC on the 27th got `data_interval_start` 26th 06:00 and
`data_interval_end` 27th 06:00 (seen on the day 3 manual run). So a manual
run after 06:00 UTC lands in today's partition, and a manual run *before*
06:00 UTC lands in yesterday's. Trigger manual runs after 06:00 UTC, or pass
the date explicitly if that ever matters.

## Why `dbt_build` has no retries

A failed dbt test is a fact about the data in the warehouse, and the data
does not change between attempts. Retrying re-runs every model to fail the
same way five minutes later, twice, and delays the alert by ten minutes. The
tasks that can fail for a reason that goes away (a network blip on the API
call, an S3 throttle, a Snowflake login timeout) keep two retries five minutes
apart: `extract`, `write_to_s3`, `copy_into_snowflake`, and the checks that
open a connection.

## Why `validate_row_count` fails on zero rows

`copy_flat` returns zero rows in two cases that look the same from the
result: no file landed under the partition, or the file is already in COPY's
load metadata. Both mean the run did not add anything, so dbt should not run
as if it had. The message says what to do: clear `write_to_s3` to produce a
new file name, which COPY will then load.

## What crosses XCom

- `extract` to `transform`: the fifteen raw payloads, as a list of dicts.
- `transform` to `write_to_s3`: the cleaned frame (600 rows). Airflow 2.6+
  serialises a pandas DataFrame through its own serde, which is how the
  previous DAG passed it. This is larger than XCom is meant for and goes away
  when the DAG moves to the CLI (day 33): each step then writes a file and
  passes a path.
- `write_to_s3` to the load: a dict with the date, the object keys and the row
  count.
- The load to the checks: `LoadResult` as a dict.

## Timeouts, and what state a timed out task lands in (2026-10-29)

`execution_timeout=timedelta(minutes=20)` is set on every task through
`default_args`, and `dagrun_timeout=timedelta(hours=1)` on the DAG.

When a task passes its `execution_timeout`, Airflow raises
`AirflowTaskTimeout` inside the task process. The task is treated like any
other failure: it lands in `up_for_retry` while retries remain, and in
`failed` once they are used up, at which point `on_failure_callback` fires.
So a hung Snowflake login costs at most 20 minutes, three times, before the
phone buzzes, rather than holding a worker slot until someone notices.

When a run passes its `dagrun_timeout`, the run is marked `failed` and its
still-running tasks are stopped. With `max_active_runs=1`, this is what keeps
a stuck run from blocking the next morning's schedule.

## The failure alert (2026-10-29)

`on_failure_callback` used to write one line to the task log, which is the
one place nobody is looking when a run fails at 06:00. It now also posts one
line, `dag_id.task_id try N failed` plus the log URL, to an ntfy.sh topic
(`src/alerts.py`). The topic name is random and lives in `.env` as
`NTFY_TOPIC`; the ntfy app on a phone subscribes to it. No account, no SMTP.
A failed post is logged and swallowed: a broken alert must not turn one task
failure into two exceptions.

The callback fires after retries are exhausted, not on every attempt, so a
transient failure that succeeds on retry stays quiet.
