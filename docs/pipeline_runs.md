# Pipeline runs

A log of loads into Snowflake, by hand or by the DAG, with what COPY returned.

## 2026-10-27: first COPY from the S3 stage, run twice

`python -m src.snowflake_loader --dt 2026-10-27` from the laptop, as
`WEATHER_SVC` / `WEATHER_ENGINEER` on `LOAD_WH`.

| Run | Session | Query id | Result |
|---|---|---|---|
| 1 | 4649379408638062 | `01c80416-0209-596f-0010-8496001e5296` | `LoadResult(files_loaded=1, rows_loaded=600, rows_parsed=600, errors_seen=0)` |
| 2 | 4649379408638066 | `01c80416-0209-5267-0010-8496001e21ce` | `LoadResult(files_loaded=0, rows_loaded=0, rows_parsed=0, errors_seen=0)` |

The second run loaded nothing because of load metadata: COPY remembers every
file it has loaded into the table for 64 days and skips it. That is what makes
the task safe for Airflow to retry: a retry after a failure loads only the files
the failed attempt did not, and a retry after a success loads nothing.

`INFORMATION_SCHEMA.COPY_HISTORY` for the table, last two hours:

```
dt=2026-10-27/output_20261027_000002_398405.csv  Loaded  600 rows  600 parsed  0 errors  19:02:27 UTC
```

One file, once. The 600 rows carry `dag_run_id = scheduled__2026-10-26T00:00:00+00:00`,
the DAG run that landed the CSV at 00:00 UTC. The table now holds 1200 rows:
that run plus the manual backfill from September.

A dry run against the other partition,
`COPY ... FROM @RAW.S3_FLAT_STAGE/dt=2026-10-26/ FILE_FORMAT = (FORMAT_NAME = RAW.CSV_HEADER) VALIDATION_MODE = RETURN_ERRORS`,
returned no error rows and loaded nothing. See ADR 0001 for when to use it.

## 2026-10-27: first runs of weather_daily, laptop, S3 to Snowflake end to end

The DAG renamed and rewired (PR: `feat(airflow): load snowflake from s3 in
weather_daily`). Postgres is off the path. Unpausing created one scheduled run
for the latest interval on its own (`catchup=False` still runs the most recent
one), then a manual run was triggered 40 seconds later.

| Run | dag_run_id | Partition | File | COPY query id | Rows |
|---|---|---|---|---|---|
| scheduled, on unpause | `scheduled__2026-10-26T06:00:00+00:00` | `dt=2026-10-27` | `output_20261027_192816_751720.csv` | in its task log | 600 |
| manual | `manual__day3_a` | `dt=2026-10-27` | `output_20261027_192858_265318.csv` | `01c80431-0209-596f-0010-8496001e52d2` | 600 |

Manual run, task by task (all green, 37 seconds end to end):

```
write_to_s3            dt=2026-10-27: 15 raw objects, flat weather/flat/dt=2026-10-27/output_20261027_192858_265318.csv
copy_into_snowflake    LoadResult(files_loaded=1, rows_loaded=600, rows_parsed=600, errors_seen=0)
validate_row_count     Rows loaded this run: 600
volume_anomaly_check   Volume check: current=600, rolling_avg=600.0, threshold=300.0
dbt_source_freshness   1 of 1 PASS freshness of raw.weather_forecast
dbt_build              Done. PASS=60 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=60
```

`dbt_source_freshness` passed for the first time since 2026-10-15; the
thresholds are back to 26 and 50 hours. `RAW.WEATHER_FORECAST` now holds 2400
rows across four run ids. Three of them are the same day's forecasts, so the
staging dedupe has real overlap to remove; day 5 counts it.

## 2026-10-29: first run on the box, i-06974584beba5796b

`weather_daily` triggered by hand on the EC2 instance (`manual__box_1`), the
laptop's DAG paused at the same time. Same numbers as the laptop run of
2026-10-27, from a machine that is not the laptop and holds no AWS keys:

```
write_to_s3            dt=2026-10-29: 15 raw objects, flat weather/flat/dt=2026-10-29/output_20261029_162859_013621.csv
copy_into_snowflake    COPY query 01c80ebd-0209-6b57-0010-8496002001fe: 1 file(s), 600 rows loaded of 600 parsed, 0 errors
validate_row_count     Rows loaded this run: 600
volume_anomaly_check   Volume check: current=600, rolling_avg=600.0, threshold=300.0
dbt_source_freshness   1 of 1 PASS freshness of raw.weather_forecast
dbt_build              Done. PASS=60 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=60
```

All eight tasks green, 16:28:28 to 16:29:46 UTC. The S3 write authenticated
through the instance role `weather-ec2-role`. The DAG is unpaused on the box
and paused on the laptop from this point; the next run is the box's own at
06:00 UTC on 2026-10-30.

Between the laptop handover and this run, the laptop's scheduler had already
run the 06:00 UTC runs of 2026-10-28 and 2026-10-29 on its own (600 rows
each), so `RAW.WEATHER_FORECAST` holds 4200 rows across seven run ids.

## 2026-10-29: the deliberate failure, cleared alone (day 4B)

`src/transform.py` on the box was made to raise on entry (an uncommitted
edit, reverted afterwards), and `manual__break_1` was triggered at 16:53:53
UTC. `transform` failed at 16:54, 16:59 and 17:04 (two retries, five minutes
apart), the run went `failed`, and the failure callback posted to ntfy;
the notification arrived on the phone. Then the file was restored and only
`transform` was cleared, with its downstream:

```
airflow tasks clear weather_daily --task-regex '^transform$' --start-date 2026-10-29T16:53:52+00:00 --end-date 2026-10-29T16:53:52+00:00 --downstream --yes
```

Attempts per task afterwards: `extract` 1, `transform` 4, every downstream
task 1. The API was called once. The load log has exactly one `COPY`:

```
write_to_s3            dt=2026-10-29: 15 raw objects, flat weather/flat/dt=2026-10-29/output_20261029_170456_636087.csv
copy_into_snowflake    COPY query 01c80ee1-0209-6b57-0010-849600200322: 1 file(s), 600 rows loaded of 600 parsed, 0 errors
dbt_build              Done. PASS=60 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=60
```

That sentence is the Airflow half of the idempotency answer: clearing a task
re-runs it and what depends on it, not what it depends on, so a repaired
transform does not cost a second API call. The Snowflake half is load
metadata (ADR 0001): the re-run wrote a new file name, so COPY loaded it, and
the staging dedupe owns the overlap.
