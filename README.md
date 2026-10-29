# Weather Data ELT Pipeline

**Status (October 2026):** the dbt project runs on Snowflake. Loading into Snowflake through S3 and a storage integration, and hosting on EC2, are in progress.

ELT pipeline that pulls live weather forecast data from the OpenWeatherMap API across 15 cities, loads it into PostgreSQL, and transforms it into analytics-ready models using dbt — all orchestrated by Airflow running in Docker. The dbt project also runs against Snowflake, with account setup, RBAC, and a key-pair service user versioned in `setup/`. Built to practice the kind of reliability and operational detail that matters in production: idempotent loads, data quality gates, schema drift handling, and CI via GitHub Actions.

---

## Contents

- [Key Engineering Features](#key-engineering-features)
- [Architecture](#architecture)
- [Screenshots](#screenshots)
- [Quality and Reliability](#quality-and-reliability)
- [Tech Stack](#tech-stack)
- [dbt Models](#dbt-models)
- [Repository Structure](#repository-structure)
- [Database Schema](#database-schema)
- [Failure Handling](#failure-handling)
- [Testing](#testing)
- [Known Limitations](#known-limitations)
- [Incident Log](#incident-log)
- [Setup](#setup)
- [Running the Pipeline](#running-the-pipeline)
- [dbt](#dbt)
- [Snowflake](#snowflake)
- [Sample Output (raw CSV)](#sample-output-raw-csv)

---

## Key Engineering Features

- **REST API ingestion** — Pulls live 5-day forecast data for 15 cities with retry logic, timeout handling, and structured error logging
- **Idempotent loads** — Filename-keyed deduplication prevents duplicate ingestion across retries and backfills
- **Volume anomaly detection** — Each run's row count is compared against a 7-run rolling average; runs loading more than 50% fewer rows than the baseline fail before transformation begins
- **Schema contract enforcement** — A canonical column and dtype contract keeps the raw table's shape identical no matter which optional fields the API returned that day; unknown fields are dropped and logged rather than crashing the pipeline
- **Layered dbt architecture** — Staging, intermediate, mart, and report layers with clear separation of concerns and an incremental fact model that merges on `(city_id, local_dt)` behind an `ingested_at` load watermark
- **53 dbt tests** — 52 schema tests (uniqueness, not-null, range, accepted-value, referential integrity, and a custom `is_non_negative` generic) plus a singular test that catches forecasts outside their own min/max band
- **Source freshness gate** — `dbt source freshness` runs before `dbt build`, so stale raw data fails the run instead of producing stale marts
- **Deterministic staging deduplication** — Overlapping forecast windows across DAG runs resolved with `ROW_NUMBER()` ordered by `ingested_at` then `source_file_name`, with a `tie_count` column that surfaces rows the tie-breakers could not separate
- **City history snapshot** — A dbt snapshot tracks changes to city name and population over time (type 2)
- **Snowflake port** — The same dbt project runs unchanged against Postgres or Snowflake via a `DBT_TARGET` switch; account, warehouse, RBAC, and key-pair auth are scripted in `setup/`
- **CI/CD via GitHub Actions** — Automated pytest suite runs on every push to master
- **58 pytest unit tests** — Validates transform logic, the raw schema contract, volume anomaly thresholds, atomic CSV writes, and file timestamp parsing
- **Structured logging** — Python `logging` module throughout the ingestion layer; Airflow task logs capture full run context including `dag_run_id`
- **Dockerized deployment** — Airflow and PostgreSQL run in isolated containers via Docker Compose

---

## Architecture

```mermaid
graph TD
    A["OpenWeatherMap REST API\n15 cities · 5-day forecast · 3-hour intervals"] --> B["Python Ingestion Layer\nclient · extract · transform · load"]
    B --> C["CSV Landing Zone\ndata/raw/ · immutable source record"]
    C --> D["PostgreSQL · raw.weather_forecast\nrow count validation · volume anomaly check"]
    D --> D2["dbt Source Freshness\nwarn after 25h · error after 7d"]
    D2 --> E["dbt Staging\nstg_weather__forecast\ntype casting · deduplication · UTC normalization"]
    E --> E2["dbt Snapshot\nsnp_city\ncity name and population history"]
    E --> F["dbt Intermediate\nint_weather_enriched\nderived categories"]
    F --> G["dbt Marts\ndim_city · fct_weather_forecast · fct_weather_daily"]
    G --> H["dbt Reports\nrpt_current_conditions\ncurrent conditions per city"]
    H --> I["dbt Tests\n53 tests · schema, singular and custom generic"]
```

The warehouse is PostgreSQL in the Docker stack. The dbt layer is target-agnostic and also runs against Snowflake, see [Snowflake](#snowflake).

---

## Screenshots

**Airflow DAG**
![Airflow DAG](docs/airflow_dag.png)

**dbt Lineage Graph**
![dbt Lineage Graph](docs/dbt_lineage.png)

---

## Quality and Reliability

The pipeline runs four data quality gates in sequence: a row count validation, a volume anomaly check (current run compared against a 7-run rolling average), a `dbt source freshness` check on `raw.weather_forecast`, and dbt tests interleaved with every model build. If any of them fail, the run stops there.

Every row in the warehouse carries `source_file_name`, `ingested_at`, and `dag_run_id` — enough to trace any row back to the exact API call that produced it. Loads are idempotent, so backfilling a missed run or retrying a failed one is safe without cleanup. The 6 dbt models and 1 snapshot have 53 tests: 52 schema tests across uniqueness, not-null, range, accepted-value, and referential integrity checks (including a custom `is_non_negative` generic), plus one singular test asserting that `main_temp` sits within its own `main_temp_min` and `main_temp_max`. There are also 58 pytest unit tests covering transform logic, the raw schema contract, volume anomaly thresholds, atomic CSV writes, and timestamp parsing, run automatically on every push via GitHub Actions.

---

## Tech Stack

| Category | Tools |
|---|---|
| Orchestration | Apache Airflow |
| Transformation | dbt (dbt-postgres · dbt-snowflake) |
| Warehouse | PostgreSQL · Snowflake |
| Compute | Docker · Docker Compose |
| Language | Python · SQL |
| Libraries | pandas · SQLAlchemy · psycopg2 · pytest |
| CI/CD | GitHub Actions |

---

## dbt Models

| Layer | Model | Description |
|---|---|---|
| Staging | `stg_weather__forecast` | Type casting, deterministic deduplication, UTC normalization |
| Snapshot | `snp_city` | Type 2 history of city name and population, keyed on `city_id` |
| Intermediate | `int_weather_enriched` | Derived temp, wind, and precipitation categories |
| Mart | `dim_city` | City dimension, one row per city |
| Mart | `fct_weather_forecast` | Incremental 3-hour forecast fact table, merge on `(city_id, local_dt)` |
| Mart | `fct_weather_daily` | Daily aggregate (avg/min/max temp, precipitation totals) |
| Report | `rpt_current_conditions` | Current conditions view, one row per city |

Tests live in two places: generic tests are declared in each layer's `schema.yml`, and `dbt/tests/` holds the singular test `assert_temp_within_min_max.sql` and the custom generic `is_non_negative`.

---

## Repository Structure

```
weather_pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                     # GitHub Actions: pytest on push
├── dags/
│   └── weather_daily.py               # Airflow DAG definition
├── data/                              # contents gitignored; dirs ship empty
│   ├── raw/                           # CSV output files from each DAG run
│   └── processed/                     # unused, reserved
├── dbt/
│   ├── models/
│   │   ├── staging/weather/
│   │   │   ├── stg_weather__forecast.sql
│   │   │   ├── schema.yml
│   │   │   └── weather_sources.yml
│   │   ├── intermediate/
│   │   │   └── int_weather_enriched.sql   # Temp, wind, and precip categories
│   │   ├── marts/
│   │   │   ├── dim_city.sql               # City dimension
│   │   │   ├── fct_weather_forecast.sql   # Incremental 3-hour forecast fact
│   │   │   ├── fct_weather_daily.sql      # Daily aggregate fact
│   │   │   └── schema.yml
│   │   └── reports/
│   │       └── rpt_current_conditions.sql # Latest forecast per city
│   ├── snapshots/
│   │   └── snp_city.sql                   # Type 2 history of city name and population
│   ├── tests/
│   │   ├── assert_temp_within_min_max.sql # Singular test on fct_weather_forecast
│   │   └── generic/
│   │       └── is_non_negative.sql        # Custom generic test
│   ├── macros/
│   │   └── generate_schema_name.sql       # Prevents dbt from prepending target schema
│   ├── profiles.yml                       # dev, prod and snowflake targets
│   ├── dbt_project.yml
│   └── packages.yml
├── setup/                             # Snowflake account bootstrap, run once in order
│   ├── 00_account.sql                 # Timezone, resource monitor
│   ├── 01_warehouses.sql              # LOAD_WH and TRANSFORM_WH
│   ├── 02_database.sql                # WEATHER_DB and its schemas
│   ├── 03_roles_and_grants.sql        # WEATHER_ENGINEER role and grants
│   ├── 04_service_user.sql            # WEATHER_SVC with key-pair auth
│   ├── 05_raw_tables.sql              # RAW.WEATHER_FORECAST
│   ├── 06_stage.sql                   # CSV file format and internal stage
│   └── load/
│       └── copy_raw_tables.sql        # COPY INTO from the stage
├── keys/                              # RSA key pair for WEATHER_SVC; only the README is committed
│   └── README.md
├── tests/
│   ├── conftest.py                    # Shared pytest fixtures
│   ├── test_transform.py              # Transform logic unit tests
│   ├── test_schema.py                 # Raw column/dtype contract tests
│   ├── test_quality_checks.py         # Volume anomaly threshold tests
│   ├── test_load.py                   # Atomic CSV write tests
│   └── test_postgres_loader.py        # postgres_loader unit tests
├── src/
│   ├── client.py                      # OpenWeatherMap API client
│   ├── extract.py                     # Calls API and returns raw records
│   ├── transform.py                   # Cleans and flattens records
│   ├── schema.py                      # Canonical raw column and dtype contract
│   ├── load.py                        # Writes records to CSV
│   ├── postgres_loader.py             # Loads CSVs into raw.weather_forecast
│   ├── quality_checks.py              # Volume anomaly detection
│   ├── main.py                        # CLI entrypoint (runs full pipeline locally)
│   ├── logging_config.py
│   └── utils.py
├── docs/
│   ├── airflow_dag.png                # Airflow DAG graph
│   ├── dbt_lineage.png                # dbt lineage graph
│   ├── postgres_to_snowflake.md       # Working log of the Snowflake port
│   └── security.md                    # Secret scan findings and remediation
├── docker-compose.yml
├── Dockerfile
├── requirements.txt                   # Pipeline dependencies
├── requirements-airflow.txt           # Airflow dependencies
├── .env.example                       # Template for required environment variables
└── .env                               # Local environment variables (not committed)
```

---

## Database Schema

### raw.weather_forecast
Loaded directly from CSV files by Airflow. Includes all raw API fields plus pipeline metadata:

The 34 API columns are fixed by the contract in `src/schema.py`, so this shape does not change with the weather. Types below are what `to_sql` creates from the pinned pandas dtypes.

| Column | Type | Description |
|---|---|---|
| dt | bigint | Forecast Unix timestamp |
| dt_txt | text | Forecast timestamp as text |
| main_temp | double precision | Temperature (°F) |
| main_feels_like | double precision | Feels-like temperature |
| main_temp_min / max | double precision | Min/max temperature |
| main_temp_kf | double precision | Internal temperature adjustment from the API |
| main_dew_point | double precision | Dew point |
| main_humidity | bigint | Humidity % |
| main_pressure | bigint | Atmospheric pressure |
| main_sea_level / grnd_level | bigint | Pressure at sea and ground level |
| wind_speed | double precision | Wind speed |
| wind_deg | bigint | Wind direction (degrees) |
| wind_gust | double precision | Wind gust speed |
| rain_3h | double precision | Rain volume (last 3 hours), null when no rain |
| snow_3h | double precision | Snow volume (last 3 hours), null when no snow |
| clouds_all | bigint | Cloud coverage % |
| visibility | bigint | Visibility distance |
| pop | double precision | Probability of precipitation (0-1) |
| sys_pod | text | Part of day indicator (d / n) |
| weather_id | bigint | Weather condition ID |
| weather_main | text | High-level condition (Rain, Clouds, etc.) |
| weather_description | text | Detailed condition |
| weather_icon | text | Icon code |
| city_id | bigint | City identifier |
| city_name | text | City name |
| city_country | text | Country code |
| city_population | bigint | City population |
| city_timezone | bigint | UTC offset in seconds |
| city_sunrise / sunset | bigint | Sunrise/sunset Unix timestamps |
| city_coord_lat / lon | double precision | Coordinates |
| source_file_name | text | Source CSV filename |
| source_file_ts | timestamp | Timestamp parsed from filename |
| ingested_at | timestamp | UTC time of ingestion |
| dag_run_id | text | Airflow DAG run ID |

The last four are pipeline metadata added at load time, not API fields.

### staging.stg_weather__forecast
dbt staging model. Casts all columns to correct types, normalizes timestamps to UTC, trims strings, and deduplicates to one row per `city_id` + `dt_utc`. The winner is chosen by `ROW_NUMBER()` ordered by `ingested_at DESC NULLS LAST`, then `source_file_name DESC` as a tie-breaker, so two runs over the same raw data always pick the same row. A `tie_count` column records how many rows shared the winning `ingested_at`; anything above 1 means the tie-breaker had to decide, which is worth knowing when a value looks surprising.

### snapshots.snp_city
dbt snapshot with `strategy='check'` on `city_name` and `city_population`, keyed on `city_id`. Each time either value changes, the previous row is closed with `dbt_valid_to` and a new one is opened, giving a type 2 history of the city metadata the API returns.

### intermediate.int_weather_enriched
Adds derived categorical columns on top of the staging model: `temp_category` (freezing / cold / mild / warm / hot), `wind_category` (calm / light / moderate / strong / storm), and `precip_type` (none / rain / snow / rain_and_snow).

### analytics.dim_city
City dimension table. One row per city, deduplicated using `ROW_NUMBER()` ordered by most recent forecast.

### analytics.fct_weather_forecast
Incremental 3-hour forecast fact table using `incremental_strategy='merge'` on `(city_id, local_dt)`. Each run takes staging rows whose `ingested_at` is at or after the newest one already in the table, then merges them: existing keys are updated, new keys are inserted. That is why staging dedupe and the merge are both needed. Staging picks one winner per forecast slot from everything in raw, and the merge reconciles that winner with the version a previous run already stored. A forecast the API later revises replaces its earlier row instead of being discarded as a duplicate.

The watermark limits what gets written, not what gets read. Staging is a view, and Postgres cannot push the `ingested_at` filter below the window function, so every run still ranks all of raw. Fine at this volume; the fix if raw grows is to filter raw before ranking.

### analytics.fct_weather_daily
Daily aggregate fact. Rolls up the 3-hour forecasts to one row per `city_id` + calendar date: avg/min/max temp, avg humidity, avg wind speed, total rain and snow.

### reports.rpt_current_conditions
Reporting view. Returns the single most-recent forecast per city with all enriched columns from `int_weather_enriched`. Ready for BI tool consumption.

---

## Failure Handling

The pipeline is designed to fail loudly at the point of failure rather than pass bad data downstream.

**API or network failure** — the `extract` task raises an exception if the API call fails or returns an unexpected response. Airflow retries the task twice with a 5-minute delay before marking the DAG run as failed.

To simulate: set `OPENWEATHER_API_KEY=invalid` in `.env`, restart the containers, and trigger the DAG. The `extract` task will fail with a non-200 response error. Downstream tasks do not run.

**Schema validation failure** — the `transform` task validates the structure of the API response before processing. If required fields (`list`, `city`) are missing or malformed, the task fails immediately and downstream tasks do not run.

To simulate: temporarily add `del data[0]["city"]` in `transform.py` before `validate_response` is called. The `transform` task will raise a `ValueError` with a clear message identifying the missing field.

**Volume anomaly failure** — the `volume_anomaly_check` task compares the current run's row count against a 7-run rolling average. If the current run loads more than 50% fewer rows than the rolling average, the DAG fails before dbt runs. The error message includes the `dag_run_id` so the anomalous run can be identified and inspected in the warehouse. The check is skipped automatically if fewer than 3 prior runs exist.

To simulate: temporarily set `ANOMALY_THRESHOLD_RATIO = 1.1` in `src/quality_checks.py` and trigger the DAG. It will fail at `volume_anomaly_check` with a clear message including the run ID. Restore to `0.5` after verifying.

**Source freshness failure** — the `dbt_source_freshness` task checks `max(ingested_at)` on `raw.weather_forecast` against the thresholds in `weather_sources.yml`: warn after 25 hours, error after 7 days. A warning is logged and the DAG continues; an error fails the task before `dbt_build` runs, so models are never rebuilt on top of a raw table that stopped receiving data.

To simulate: temporarily set `error_after: {count: 1, period: minute}` in `dbt/models/staging/weather/weather_sources.yml` and run `make dbt-fresh`. Restore the 7-day value after verifying.

**Data quality failure** — the `dbt_build` task runs and tests each model in dependency order, enforcing uniqueness, not-null checks, and range validation as it goes. Because tests run interleaved with the models rather than after all of them, a failing test on the staging model stops the marts and reports from being built on data already known to be bad.

To simulate: run `UPDATE raw.weather_forecast SET weather_id = NULL WHERE city_id = 5128581;` in the warehouse, then trigger the DAG. The `dbt_build` task will fail on the `not_null` test for `weather_id` and the downstream models will be skipped. Restore with `UPDATE raw.weather_forecast SET weather_id = 500 WHERE weather_id IS NULL;`.

This was observed in practice when overlapping forecast data caused a uniqueness violation, which was resolved by deduplicating in the staging model using `ROW_NUMBER()`.

---

## Testing

The test suite runs outside Docker, so create a virtualenv first. `venv/` is gitignored, so a fresh clone will not have one:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m pytest tests/ -v
```

58 tests across five files:

- `test_transform.py` — validates `validate_response` (input shape, missing fields, wrong types) and `transform_records` (output schema, row count, city broadcast, bad date handling, weather field flattening)
- `test_schema.py` — validates the canonical column and dtype contract: optional fields absent from a payload are filled with nulls, unknown fields are dropped, column order is stable, and integer columns keep an integer rendering even when a null forces a float promotion
- `test_quality_checks.py` — validates the volume anomaly thresholds, including the boundary case, the insufficient-history skip, and an empty warehouse
- `test_load.py` — validates that CSV writes are atomic: no temp file is left behind on success, no partial CSV is left at the target path on failure, back-to-back writes do not collide, and the generated filename round-trips through the parser
- `test_postgres_loader.py` — validates `parse_source_file_ts` (both filename formats, full path, missing prefix, malformed timestamp) and the structured load result formatting

Shared fixtures live in `conftest.py`. The CI workflow in `.github/workflows/ci.yml` runs the full suite on every push and pull request to master.

---


## Known Limitations

**Schema drift from newly added API fields** — the `raw.weather_forecast` table schema is inferred from the first CSV loaded via pandas `to_sql`. When the OpenWeatherMap API returns a field not present in the original table (e.g., a newly added field like `main_dew_point`), the load task would previously fail with:

```
psycopg2.errors.UndefinedColumn: column "main_dew_point" of relation "weather_forecast" does not exist
```

**Current behavior** — `postgres_loader.py` now handles this gracefully. On each load, `align_columns_to_table` queries `information_schema` to compare the DataFrame columns against the actual table columns. Any unknown columns are dropped before the insert and a warning is logged:

```
WARNING - Dropping 1 unknown column(s) not present in raw.weather_forecast: ['main_dew_point'] — run ALTER TABLE to start capturing this data
```

The pipeline does not fail. The new field is ignored until a deliberate decision is made to capture it.

**To approve a new column** — once the warning is observed in the Airflow logs, run the appropriate `ALTER TABLE` in the warehouse:

```sql
ALTER TABLE raw.weather_forecast ADD COLUMN main_dew_point numeric;
```

The next DAG run will pick up the column automatically with no code changes required. At production scale this pattern would be replaced by a migration tool like Alembic that versions schema changes as auditable migration scripts.

**Conditionally-absent known fields** — a separate case from schema drift. The API omits optional keys entirely rather than sending nulls: `snow_3h` appears only when some city has snow forecast, `rain_3h` only when some city has rain. Because `to_sql` creates the raw table from the first file it sees, a snow-free first load used to produce a table with no `snow_3h` column, and the staging model selects it unconditionally. A clone started in summer therefore died on `column "snow_3h" does not exist`.

`src/schema.py` fixes this with a canonical contract. `conform_to_schema` reindexes every frame to a fixed 34-column list before it is written or loaded, filling absent columns with nulls and dropping unrecognized ones with a warning. It also pins dtypes, because pandas infers them per file: one null promotes an integer column to float, and `"10000.0"` is not valid input for a `BIGINT` column. The contract is applied in both `transform.py` and `postgres_loader.py`, so CSVs written before it existed still produce a complete table.

Adding a genuinely new API field is a deliberate one-line change to `FORECAST_COLUMNS` plus an `ALTER TABLE`, rather than something that happens silently based on the weather.

**dbt-core and apache-airflow-providers-postgres must stay pinned in requirements-airflow.txt** — `dbt/models/marts/schema.yml` uses the `arguments:` generic-test syntax (dbt-core >=1.9), so `requirements-airflow.txt` pins `dbt-core==1.11.8`/`dbt-postgres==1.10.0` to match `requirements.txt` (the local, no-Docker venv path) exactly. Left unpinned, a fresh build can resolve an incompatible dbt-core (too old for the test syntax, or a pre-release) purely based on whatever's newest on PyPI that day — this has broken from-scratch builds before without anyone noticing, since Docker layer caching usually reused an old, working resolution. `apache-airflow-providers-postgres` is pinned to `5.10.0` in its own separate `RUN` step in the `Dockerfile` for the same reason (left unpinned, it resolves a release built for Airflow 3.x) and to avoid pip backtracking for a very long time when resolved together with the dbt pins. See the comments in `Dockerfile` and `requirements-airflow.txt` before changing any of these.

---

## Incident Log

> Entries below describe the pipeline as it was at the time. The separate
> `dbt_run` and `dbt_test` tasks referenced here were later consolidated into a
> single `dbt_build` task.

### June 2026 — cascading failure from schema change and backfill

**What happened:**
3 consecutive DAG runs failed at `load_raw_table` because the OpenWeatherMap API began returning a new field (`main_dew_point`) that did not exist in `raw.weather_forecast`. The extract and transform tasks succeeded each time and CSVs were written to `data/raw/`, but the Postgres load failed before any rows were inserted.

**Fix 1 — schema:**
`ALTER TABLE raw.weather_forecast ADD COLUMN main_dew_point numeric` was run in the warehouse. `align_columns_to_table` was added to `postgres_loader.py` to handle future unknown columns gracefully rather than failing.

**Fix 2 — backfill:**
`make backfill` was run to load the 3 missed CSVs. Because `skip_if_loaded` uses the filename as the key, only the unloaded files were inserted — no duplicates.

**Second issue — null city metadata:**
The backfill also picked up older CSVs from before city columns were added to the extract logic. Those files loaded successfully but contributed 320 rows with null `city_id`, `city_name`, and related city columns.

**Caught by dbt tests:**
The `dbt_test` task failed on not-null constraints for `city_id`, preventing the mart from being updated with bad data. This was the intended behavior — the pipeline failed loudly rather than propagating nulls downstream.

**Fix 3 — data patch:**
The 320 rows were patched directly in `raw.weather_forecast` using a CTE UPDATE that sourced city values from valid rows with matching structure. The original CSVs were left untouched as the permanent record of what the API returned at that time. After patching, `dbt_run` and `dbt_test` both passed and the incremental model picked up all backfilled rows automatically.

### September 2026 — porting the dbt project to Snowflake

**What happened:**
The first `dbt run --target snowflake` against a freshly scripted account failed three times in a row, each for a different reason. Five of the six models compiled and ran unchanged. The failures were one configuration mismatch and two Postgres-only functions in a single report model. The full log, with each error as dbt printed it, is in `docs/postgres_to_snowflake.md`.

**Failure 1 — schema privileges:**
`Insufficient privileges to operate on database 'WEATHER_DB'. Your primary role WEATHER_ENGINEER must have CREATE SCHEMA granted`. The dbt project configures its schemas as `analytics`, `intermediate` and `reports`, but `setup/02_database.sql` had created `MARTS` and nothing for the other two. The custom `generate_schema_name` macro passes the custom schema through verbatim, so dbt looked for `ANALYTICS`, did not find it, and tried to create it. `WEATHER_ENGINEER` is deliberately not allowed to create schemas.

**Fix 1:**
`02_database.sql` was changed to create `ANALYTICS`, `INTERMEDIATE` and `REPORTS`, `MARTS` was dropped, and `03_roles_and_grants.sql` was updated to grant usage and create table/view on each. The role still cannot create schemas. That restriction is what surfaced the drift between the dbt config and the warehouse layout, which is the behaviour wanted from a least-privilege service role.

**Failure 2 — `now()`:**
`Unknown functions NOW, NOW` in `rpt_current_conditions`. Postgres has `now()`; Snowflake does not.

**Fix 2:**
Replaced both calls with `{{ dbt.current_timestamp() }}`, which dbt compiles to the correct function for whichever adapter is active.

**Failure 3 — `EXTRACT(EPOCH ...)`:**
`invalid value [EPOCH] for parameter 'EXTRACT date/time part'`, again in `rpt_current_conditions`. The model ranks forecasts by distance from the current time using `ABS(EXTRACT(EPOCH FROM (local_dt - now())))`. In Postgres subtracting two timestamps yields an interval and `EXTRACT(EPOCH)` converts it to seconds. Snowflake has no interval type, so there is nothing to extract from.

**Fix 3:**
Replaced with `{{ dbt.datediff("local_dt", dbt.current_timestamp(), "second") }}`. On Postgres this compiles back to the original epoch arithmetic; on Snowflake it becomes `DATEDIFF(second, ...)`.

**Outcome:**
All six models and the test suite pass on both targets from the same SQL. No model carries adapter-specific branches; the only `target.type` check in the project is the source schema name (`RAW` versus `raw`) in `weather_sources.yml`.

---

## Setup

### Prerequisites
- Docker and Docker Compose
- OpenWeatherMap API key — sign up free at [openweathermap.org/api](https://openweathermap.org/api) (the free tier covers this pipeline's 5-day/3-hour forecast calls). New keys can take up to a couple hours to activate.

### Environment Variables

Copy `.env.example` to `.env` and fill in real values:

```bash
cp .env.example .env
```

```
OPENWEATHER_API_KEY=<your key from openweathermap.org>
WAREHOUSE_USER=weather_user
WAREHOUSE_PASSWORD=weather_pass
WAREHOUSE_DB=weather_db
WAREHOUSE_HOST=postgres-warehouse
WAREHOUSE_PORT=5432
```

Only `OPENWEATHER_API_KEY` needs to be changed — all other values match the Docker Compose defaults and can be left as-is.

Also set `AIRFLOW_UID` to your host user's UID, so files the containers create (logs, dbt artifacts, raw CSVs) end up owned by you instead of a fixed container UID — this is Airflow's own documented setup step, not specific to this repo:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

If you skip this, everything still runs — `airflow-init` falls back to UID `50000` — but `data/`, `logs/`, and dbt's `target/`/`dbt_packages/` directories will end up owned by that UID on your host, and you'll need `sudo chown` to edit files under them afterward. `dags/`, `src/`, and the rest of `dbt/` are left alone either way.

### Start Airflow

```bash
docker compose up
```

Airflow UI available at `http://localhost:8080` (admin / admin).
The warehouse Postgres is available at `localhost:5432`.

If either port is already in use, set `AIRFLOW_WEBSERVER_PORT` / `WAREHOUSE_HOST_PORT` in `.env` to remap the host side only — internal service-to-service traffic is unaffected.

Airflow pauses every DAG on creation by default (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`), so nothing — including a fresh deploy of this pipeline — runs unattended before a human enables it. `weather_daily` will sit `queued` if triggered until you unpause it:

```bash
docker compose exec airflow-scheduler airflow dags unpause weather_daily
```

(Same command for any new DAG you add — swap in its `dag_id`.)

### Create the Airflow Connection

After the containers are healthy, register the warehouse connection so Airflow can resolve credentials at runtime. This is a one-time step — the connection persists until the containers are torn down with `docker compose down -v`.

```bash
docker compose exec airflow-scheduler airflow connections add weather_warehouse \
    --conn-type postgres \
    --conn-host postgres-warehouse \
    --conn-login weather_user \
    --conn-password weather_pass \
    --conn-schema weather_db \
    --conn-port 5432
```

On Windows PowerShell, use a backtick instead of `\` for line continuation:

```powershell
docker compose exec airflow-scheduler airflow connections add weather_warehouse `
    --conn-type postgres `
    --conn-host postgres-warehouse `
    --conn-login weather_user `
    --conn-password weather_pass `
    --conn-schema weather_db `
    --conn-port 5432
```

If the connection is not found at runtime, `postgres_loader.py` falls back to the `.env` variables automatically.

---

## Running the Pipeline

### Via Airflow (recommended)

Trigger the DAG from the UI or CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger weather_daily
```

### Via CLI (local, no Docker)

```bash
source venv/bin/activate
python -m src.main
```

`main()` doesn't just load the CSV it just extracted — it calls `load_all_files()`, which loads *every* unloaded file already sitting in `data/raw/`, tagged under one `dag_run_id`. If that directory has accumulated many historical files (e.g. from months of local dev), one run of this command bulk-loads all of them at once. Harmless against a scratch database, but be aware if `WAREHOUSE_HOST`/`WAREHOUSE_PORT` happen to point at the same warehouse an Airflow deployment is also using — a single oversized `dag_run_id` group like that will skew `volume_anomaly_check`'s rolling average for every run afterward until it ages out of the last-7-runs window (or is deleted).

### Backfill existing CSV files

```bash
make backfill
```

Equivalent to `docker compose exec airflow-scheduler python -m src.postgres_loader all`, which walks every `output_*.csv` in `data/raw/` and loads the ones not already ingested. `skip_if_loaded` keys on the filename, so re-running is safe and will not duplicate rows.

### Makefile shortcuts

The most common commands are wrapped in a `Makefile`:

| Target | What it does |
|---|---|
| `make up` / `make down` | Start or stop the stack |
| `make restart` | Recreate the stack |
| `make logs` | Tail the scheduler logs |
| `make trigger` | Trigger the DAG |
| `make backfill` | Load every unloaded CSV in `data/raw/` |
| `make dbt-run` / `make dbt-test` / `make dbt-fresh` | Run dbt phases individually in the container |
| `make psql` | Open a psql shell on the warehouse |

---

## dbt

dbt runs automatically as part of the Airflow DAG. To run manually from the host:

```bash
source venv/bin/activate
set -a; source .env; set +a        # from the repo root
cd dbt
dbt deps
WAREHOUSE_HOST=localhost dbt build
```

`WAREHOUSE_HOST` has to be overridden. `.env` sets it to `postgres-warehouse`, which is the Compose service name and only resolves inside the Docker network. From the host the warehouse is reachable on the published port at `localhost:5432`.

### Which profiles.yml dbt uses

dbt picks exactly one `profiles.yml`; it never merges several. The search order is:

1. the `--profiles-dir` flag
2. the `DBT_PROFILES_DIR` environment variable
3. the current working directory
4. `~/.dbt/`

The DAG and the `Makefile` always pass `--profiles-dir /opt/airflow/dbt`, so inside the container this project's `dbt/profiles.yml` is used and nothing else is consulted.

Running manually is where it gets subtle. Because `cd dbt` puts this project's `profiles.yml` in the working directory, rule 3 wins and any `~/.dbt/profiles.yml` you may have is ignored. That is why the env vars above are required. If you would rather use a personal profile with the connection hardcoded, point at it explicitly:

```bash
dbt build --profiles-dir ~/.dbt
```

`dbt build` runs and tests each model in dependency order. `dbt run` and `dbt test` still work if you want the two phases separately, but `build` is what the DAG uses, because it stops a failing test from letting downstream models be built on bad data.

`dbt deps` installs the packages listed in `packages.yml` into `dbt_packages/` (gitignored) — required once before the first `dbt run`/`dbt test`/`dbt source freshness`, and again after `packages.yml` changes.

### Target switching (dev / prod)

The `DBT_TARGET` environment variable controls which `profiles.yml` target is used. There are three: `dev` and `prod` are Postgres, `snowflake` is Snowflake. The profile defaults to `dev`, but `docker-compose.yml` sets `DBT_TARGET=snowflake` for the Airflow containers, so the DAG currently runs dbt against Snowflake. Override it in `.env` to point the DAG back at the local Postgres warehouse.

Models that differ between the two databases are handled with `target.type` checks rather than duplicate SQL, for example the source schema is `RAW` on Snowflake and `raw` on Postgres.

### Lineage

![dbt Lineage Graph](docs/dbt_lineage.png)

`raw.weather_forecast` → `stg_weather__forecast` → `int_weather_enriched` → `fct_weather_forecast` / `fct_weather_daily` / `rpt_current_conditions`

`stg_weather__forecast` also feeds the `snp_city` snapshot and `dim_city`.

To regenerate docs:

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs generate --profiles-dir /opt/airflow/dbt"
```

---

## Snowflake

The dbt project runs against a Snowflake account as well as the local Postgres warehouse. Everything needed to stand the account up is versioned in `setup/`, numbered in the order it has to run:

| Script | Role | What it does |
|---|---|---|
| `00_account.sql` | ACCOUNTADMIN | Sets the account timezone to UTC and creates a `TRIAL_GUARD` resource monitor with a credit quota |
| `01_warehouses.sql` | SYSADMIN | `LOAD_WH` for COPY INTO and `TRANSFORM_WH` for dbt, both XSMALL with auto-suspend |
| `02_database.sql` | SYSADMIN | `WEATHER_DB` with `RAW`, `STAGING`, `INTERMEDIATE`, `ANALYTICS` and `REPORTS` schemas; drops `PUBLIC` |
| `03_roles_and_grants.sql` | USERADMIN / SECURITYADMIN / SYSADMIN | `WEATHER_ENGINEER` role with usage on both warehouses, create table and view on every schema, and future select/insert grants on `RAW` |
| `04_service_user.sql` | USERADMIN | `WEATHER_SVC`, a `TYPE = SERVICE` user with key-pair auth and no password |
| `05_raw_tables.sql` | WEATHER_ENGINEER | `RAW.WEATHER_FORECAST` with the same 34 columns plus metadata as the Postgres table |
| `06_stage.sql` | WEATHER_ENGINEER | CSV and JSON file formats and the internal stages they attach to |
| `load/copy_raw_tables.sql` | WEATHER_ENGINEER | `COPY INTO RAW.WEATHER_FORECAST` from the stage, `ON_ERROR = ABORT_STATEMENT` |

Separate load and transform warehouses mean a long COPY cannot contend with dbt, and each shows up as its own line in cost reporting. The service user has no password, so the only way to authenticate as it is with the private key, which is never committed.

### Key-pair auth

Generate the pair once and register the public half on the service user. `keys/README.md` has the two `openssl` commands. Paste the body of `rsa_key.pub` into `RSA_PUBLIC_KEY` in `setup/04_service_user.sql` before running it, then point `SNOWFLAKE_PRIVATE_KEY_PATH` at the `.p8` file. `keys/` is gitignored except for its README, and `docker-compose.yml` mounts it read-only into the Airflow containers at `/opt/airflow/keys`.

### Environment variables

In addition to the Postgres variables, `.env.example` lists what the `snowflake` target reads:

```
SNOWFLAKE_ACCOUNT=your-org-your-account   # ORGNAME-ACCOUNTNAME, not the locator
SNOWFLAKE_USER=WEATHER_SVC
SNOWFLAKE_ROLE=WEATHER_ENGINEER
SNOWFLAKE_WAREHOUSE=TRANSFORM_WH
SNOWFLAKE_DATABASE=WEATHER_DB
SNOWFLAKE_PRIVATE_KEY_PATH=/absolute/path/to/keys/rsa_key.p8
DBT_TARGET=snowflake
```

`SNOWFLAKE_ACCOUNT` is the `ORGNAME-ACCOUNTNAME` form from `SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();`, with underscores replaced by hyphens. The account locator in the URL does not work with the dbt adapter.

### Running dbt against Snowflake

From the host:

```bash
set -a; source .env; set +a
cd dbt
dbt deps
dbt build --target snowflake
```

Inside the Airflow stack no override is needed, because `docker-compose.yml` already sets `DBT_TARGET=snowflake`.

### Current state of the port

The Airflow `load_raw_table` task still writes to Postgres. `RAW.WEATHER_FORECAST` in Snowflake was populated once from a Postgres export through the internal stage and `copy_raw_tables.sql`, and the dbt layer builds on top of that. Until the load task writes to Snowflake directly, the source freshness `error_after` is set to 7 days rather than something tighter, so a day without a reload does not fail the DAG. `docs/postgres_to_snowflake.md` is the working log of the port: every compile error encountered, what caused it, and how each model was changed to run on both databases.

---

## Sample Output (raw CSV)

Files land in `data/raw/` named `output_YYYYMMDD_HHMMSS_ffffff.csv`, for example `output_20260916_213806_623756.csv`. The microsecond component keeps two runs starting in the same second from resolving to the same path. Each file is written to a temp file and then atomically renamed into place, so a partially written CSV is never visible to the loader.

Columns are always the same 34 in the same order, per the contract in `src/schema.py`, regardless of which optional fields the API returned:

```
dt,visibility,pop,dt_txt,main_temp,main_feels_like,main_temp_min,main_temp_max,main_pressure,main_sea_level,main_grnd_level,main_humidity,main_temp_kf,main_dew_point,clouds_all,wind_speed,wind_deg,wind_gust,sys_pod,rain_3h,snow_3h,city_id,...
1789603200,10000,0.0,2026-09-17 00:00:00,75.83,76.44,74.12,75.83,1025,1025,1021,71,0.95,65.77,65,3.98,145,7.87,n,,,5128581,...
```

The two empty fields before `city_id` are `rain_3h` and `snow_3h`, absent from the API payload on a dry day and filled with nulls rather than dropped.
