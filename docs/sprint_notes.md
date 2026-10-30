# Sprint notes: S3 landing

Working log for the move from local CSVs to S3, in the order the work happened.
Dates are 2026.

## 2026-10-26: bucket, IAM writer, files in S3 by date

### Decisions that differ from the plan

- **Region is us-east-2, not us-east-1.** The Snowflake account reports
  `CURRENT_REGION() = AWS_US_EAST_2`. Keeping the bucket in the same region
  avoids cross-region transfer cost and latency when the storage integration
  reads from it.
- **The existing `weather-writer` user was reused.** It was left over from the
  earlier AWS work and had no policies (attached or inline) and no access keys,
  so it was treated as new.
- **`AIRFLOW__WEBSERVER__SECRET_KEY` and the Postgres path were left alone.**
  Out of scope here; see `security.md` for the former.

### Bucket

```
aws s3 mb s3://pw-weather-2026 --region us-east-2
aws s3api put-public-access-block --bucket pw-weather-2026 \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

Confirmed afterwards, all read-only:

| Setting | Result | Note |
|---|---|---|
| Public access block | all four `true` | |
| Versioning | `{}` | never enabled; nothing to turn off |
| Default encryption | `SSEAlgorithm: AES256` | SSE-S3, the default for new buckets |

All bucket and IAM commands ran in CloudShell under the console login, so no
admin credentials exist on the laptop. The laptop holds only the writer's keys.

### IAM writer

Policy: `infra/iam/extractor_write_policy.json`, registered as
`arn:aws:iam::870947613885:policy/weather-extractor-write`.

Two statements, because the two actions apply to different resources:

- `s3:PutObject` on `arn:aws:s3:::pw-weather-2026/weather/*`. Objects, so the
  ARN carries a path.
- `s3:ListBucket` on `arn:aws:s3:::pw-weather-2026` with
  `s3:prefix` like `weather/*`. Listing is a bucket action; the condition keeps
  the user from seeing names outside the prefix.

`ListBucket` written against `bucket/weather/*` would silently grant nothing.
That is the mistake to avoid when the same document moves to the instance role
on day 3.

User `weather-writer` has only this policy. Keys live in `.env` as
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, which boto3 reads on its own
(second item in its credential order). No key appears in code or compose.

### Fence test

From the laptop as `weather-writer` (the laptop has no `~/.aws`, so these are
the only credentials boto3 can find; the test cannot pass by accident with
admin rights):

```
put_object  Key=other/should_fail.json
AccessDenied: User: arn:aws:iam::870947613885:user/weather-writer is not
authorized to perform: s3:PutObject on resource:
"arn:aws:s3:::pw-weather-2026/other/should_fail.json" because no
identity-based policy allows the s3:PutObject action
```

### What lands per run

| Object | Key |
|---|---|
| One per city (15) | `weather/raw/dt=YYYY-MM-DD/{city_id}_{run_ts}.json` |
| One per run | `weather/flat/dt=YYYY-MM-DD/output_{run_ts}.csv` |

`run_ts` uses the microsecond `FILENAME_TS_FORMAT` from `src/load.py`, so
every run produces a new file name. COPY skips names it has already loaded, so
a reused name with new contents would load nothing; a new name per run plus the
staging dedupe handles the overlap instead.

### Code design

- **One UTC timestamp per run.** `main.py` derives it from the `start_time` it
  already records (`time.time()` is epoch seconds, so UTC everywhere). The DAG
  has a `start_run` task that makes it once and passes it by XCom, because each
  Airflow task is its own process and separate `datetime.now()` calls would
  disagree. File name, `dt=` partition, `source_file_ts`, `ingested_at` and
  (for manual runs) `dag_run_id` all come from that one value.
- **Raw lands before transform.** A transform bug then cannot lose the raw
  payloads.
- **The uploads branch off the DAG chain** (`upload_raw` beside `transform`,
  `upload_flat` beside `load_raw_table`). An S3 failure fails the run but
  Postgres and dbt still get the day's data.
- **`src/s3.py` has no boto3 import.** `s3_key` is pure; `upload_bytes` takes
  the client as an argument. Tests use a fake client and CI needs no AWS.
- **Missing `AWS_REGION` or `S3_BUCKET` fails the run** rather than skipping
  the upload, since S3 is about to be the path into Snowflake.

### Column comparison against setup/05_raw_tables.sql

No differences. `FORECAST_COLUMNS` in `src/schema.py` matches the first 34
columns of `RAW.WEATHER_FORECAST` by name and order, and `stamp_lineage`
appends the last four in table order:

```
source_file_name, source_file_ts, ingested_at, dag_run_id
```

Order matters, not just names: the stage's `CSV_HEADER` file format uses
`SKIP_HEADER = 1`, so COPY maps columns by position. The comparison is enforced
by `tests/test_load.py::TestCsvMatchesSnowflakeRawTable`, which parses the DDL,
so a change on either side fails the suite before it reaches COPY.

One meaning difference to remember for tomorrow: in the CSV (and so in
Snowflake) `ingested_at` is the run timestamp. In Postgres the loader still
re-stamps it with the load time, as before. Same name, two definitions; the
Snowflake one is the one that should win if they are ever reconciled.

### Other changes in this branch

- `requirements.txt` had drifted: it could not install `dbt-snowflake` as
  pinned (`dbt-adapters` and `certifi` were at incompatible versions) and did
  not list it at all. Regenerated from a venv that installs cleanly, proven in
  a fresh venv with `pip check` and the full test suite. Unused `keyring`
  packages removed. Separate commit `b926d8e`.
- boto3 added to both requirements files. The Airflow image needs a rebuild
  before the DAG can upload.

### Run result

Manual run from the laptop, `WAREHOUSE_HOST=localhost python -m src.main`,
with `.env` exported into the shell. Listing as `weather-writer` afterwards:

```
16 objects
192285 weather/flat/dt=2026-10-26/output_20261026_175722_485038.csv
 18796 weather/raw/dt=2026-10-26/1850144_20261026_175722_485038.json
 18768 weather/raw/dt=2026-10-26/2147714_20261026_175722_485038.json
 18749 weather/raw/dt=2026-10-26/2643743_20261026_175722_485038.json
 18626 weather/raw/dt=2026-10-26/2988507_20261026_175722_485038.json
 19050 weather/raw/dt=2026-10-26/3448439_20261026_175722_485038.json
 18851 weather/raw/dt=2026-10-26/3530597_20261026_175722_485038.json
 19098 weather/raw/dt=2026-10-26/4164138_20261026_175722_485038.json
 18896 weather/raw/dt=2026-10-26/4699066_20261026_175722_485038.json
 18796 weather/raw/dt=2026-10-26/4887398_20261026_175722_485038.json
 18580 weather/raw/dt=2026-10-26/4930956_20261026_175722_485038.json
 18563 weather/raw/dt=2026-10-26/5128581_20261026_175722_485038.json
 18644 weather/raw/dt=2026-10-26/5368361_20261026_175722_485038.json
 18568 weather/raw/dt=2026-10-26/5419384_20261026_175722_485038.json
 19033 weather/raw/dt=2026-10-26/5809844_20261026_175722_485038.json
 18666 weather/raw/dt=2026-10-26/6167865_20261026_175722_485038.json
```

Fifteen cities, one CSV, one run timestamp, one `dt=` partition.

The run then failed at the Postgres step with
`password authentication failed for user "postgres"`. Cause: a stale
`export POSTGRES_DBT_URL=...postgres@localhost:5432/weather_pipeline` in
`~/.bashrc` from earlier work. `get_engine()` prefers that variable over the
`WAREHOUSE_*` ones, so it beat `.env`. Unrelated to S3; the uploads had already
completed. Fix: remove the export from `.bashrc`.

The CSV was then loaded into Postgres on its own with
`python -m src.postgres_loader file --file-path ... --dag-run-id manual_1793037442`:
600 rows (15 cities x 40 forecast slots), `dag_run_id` matching the CSV. A
second attempt was refused with "Skipped already loaded file", which is the
dedupe on `source_file_name` working.

### Why the Airflow grid is red right now

- `upload_raw` and `upload_flat` fail: `dags/` is mounted from the working
  tree, so the scheduler picked the new tasks up as soon as the file was saved,
  but the running image has no boto3 and the containers predate the keys in
  `.env`. Fixed by a rebuild and `up -d`. The downstream Postgres tasks stayed
  green while the uploads failed, which is the branching design working.
- `dbt_source_freshness` errors (and `dbt_build` is upstream-failed): the DAG
  runs dbt with `DBT_TARGET=snowflake`, and the Snowflake raw table has not been
  loaded since the manual PUT more than 7 days ago. Nothing feeds it yet. Do
  not loosen `error_after`; it goes green when the COPY path lands.

After `docker compose build` and `up -d` (the containers were recreated, which
is what picks up new `.env` values), a triggered DAG run went green through
`upload_raw` and `upload_flat`. The bucket then held 32 objects: two runs of
16, same `dt=2026-10-26` partition, different run timestamps
(`20261026_175722` from the laptop, `20261026_181552` from the DAG). That is
the one-name-per-run rule working.

### Still to do after this block
- Day 3: attach `extractor_write_policy.json` to an instance role, delete
  `weather-writer` and its keys, remove the keys from `.env`.
- Storage integration (next block): read policy on `weather/*`, role trusting
  Snowflake, `S3_WEATHER_INT`. The account id above is needed for the role ARN.

## 2026-10-27: storage integration, two passes, one handshake

### Traps written down before starting

- **Never `CREATE OR REPLACE` the integration.** Replacing it makes Snowflake
  issue a new external id, and the role's trust policy still has the old one.
  Nothing errors at creation; the next `LIST` fails. After the first `CREATE`,
  every change is `ALTER STORAGE INTEGRATION`.
- **Check the two copied values before the first test.** The trust policy's
  `Principal.AWS` must equal `STORAGE_AWS_IAM_USER_ARN` and `sts:ExternalId`
  must equal `STORAGE_AWS_EXTERNAL_ID`, character for character. IAM takes up
  to a minute to propagate and Snowflake caches assumed credentials for up to
  an hour, so a wrong first try costs far more than a wrong second look.
- **If `LIST` fails:** wait one minute, retry once, re-check the two values.
  Only then change something, and change one thing.
- **The external id stays out of the repo.** `snowflake_read_trust_policy.json`
  is committed with placeholders; the real one is applied from CloudShell. The
  id is what stops any other Snowflake account from assuming the role, so a
  public repo is the wrong place for it.

### Pass one

- `infra/iam/snowflake_read_policy.json` registered as
  `arn:aws:iam::870947613885:policy/snowflake-weather-read`. Read-only twin of
  the writer policy: `GetObject`/`GetObjectVersion` under `weather/*`,
  `ListBucket`/`GetBucketLocation` on the bucket with the prefix condition.
- Role `snowflake-weather-read` created with a placeholder trust (own account,
  external id `0000`) so the integration had an ARN to point at. Read policy
  attached to it.
- `setup/07_storage_integration.sql` run as ACCOUNTADMIN: `CREATE STORAGE
  INTEGRATION IF NOT EXISTS S3_WEATHER_INT`, role ARN above, allowed location
  `s3://pw-weather-2026/weather/`.

### Pass two

- `DESC INTEGRATION` gave `STORAGE_AWS_IAM_USER_ARN =
  arn:aws:iam::183863150535:user/7fg62000-s` (Snowflake's own account) and an
  external id, kept out of the repo.
- Both pasted into CloudShell shell variables, the trust policy built from the
  variables (no retyping), bracket-printed to catch stray whitespace, applied
  with `update-assume-role-policy`, read back with `get-role` and compared to
  the `DESC` output. Matched first time.
- `GRANT USAGE ON INTEGRATION` to SYSADMIN, then WEATHER_ENGINEER.

### Proof

As WEATHER_ENGINEER, `CREATE STAGE RAW.S3_WEATHER_STAGE` on the integration
(now in `setup/06_stage.sql`) and `LIST @RAW.S3_WEATHER_STAGE`: 48 rows on the
first try, no propagation wait needed. 48, not 32, because the scheduled DAG
run at 00:00 UTC had already landed a third set of 16 on its own:

```
dt=2026-10-26  20261026_175722  16   laptop run
dt=2026-10-26  20261026_181552  16   triggered DAG run
dt=2026-10-27  20261027_000002  16   scheduled DAG run
```

Snowflake can now read the bucket with no keys anywhere. What is missing is
the COPY from `@RAW.S3_WEATHER_STAGE/flat/` into `RAW.WEATHER_FORECAST`, which
replaces the manual PUT path in `setup/load/copy_raw_tables.sql`. Until it
exists, `dbt_source_freshness` stays red.

### End of day, 2026-10-27

- **Where each block stopped.** Security: PRs #2 and #3 merged, nothing open.
  S3 landing: PR #4 merged; `weather-writer` keys live only in `.env` (no
  `~/.aws` credentials, nothing tracked), to be replaced by the instance role on
  the EC2 day. Storage integration: PR #5, `LIST @RAW.S3_WEATHER_STAGE` proven
  with 48 objects; no COPY from the external stage exists yet, and the manual
  PUT path in `setup/load/copy_raw_tables.sql` is still the only load.
- **Last error text.** `dbt_source_freshness`, scheduled run 2026-10-26,
  attempt 3: `1 of 1 ERROR STALE freshness of raw.weather_forecast`. Expected:
  nothing has loaded `RAW.WEATHER_FORECAST` on Snowflake since the manual PUT.
- **First ten minutes of tomorrow.** In Snowsight as WEATHER_ENGINEER:
  `LIST @RAW.S3_WEATHER_STAGE/flat/;` (expect 3 CSVs). Then one
  `COPY INTO RAW.WEATHER_FORECAST FROM @RAW.S3_WEATHER_STAGE/flat/
  FILE_FORMAT = (FORMAT_NAME = RAW.CSV_HEADER) PATTERN = '.*[.]csv'
  ON_ERROR = ABORT_STATEMENT;` with no PURGE (S3 keeps the files; COPY's own
  load history skips names it has seen). Check `COPY_HISTORY` for 3 files and
  1800 rows, then `dbt source freshness --target snowflake` should pass.

## Day 2: the stage and the Snowflake loader

### Pipeline A: the flat stage and two file formats (2026-10-27)

- `setup/08_s3_stage.sql` run as WEATHER_ENGINEER: `CSV_HEADER` altered to add
  `NULL_IF = ('', 'NA', '-999')` and `ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE`;
  new `CSV_PARSE_HEADER` (PARSE_HEADER = TRUE) for the loader's
  MATCH_BY_COLUMN_NAME; new stage `RAW.S3_FLAT_STAGE` at `weather/flat/`.
  `LIST @RAW.S3_FLAT_STAGE` returned 3 rows, one CSV per run so far.
- Created as WEATHER_ENGINEER rather than SYSADMIN plus a grant, to keep one
  owner for everything in RAW.
- TODO (Patrick): one line, in your own words, on why SKIP_HEADER cannot be
  set alongside PARSE_HEADER. Do not read the comment in 08_s3_stage.sql first.
- The fallback (PUT through EXPORT_STAGE if LIST failed at 3:00) was not
  needed; the handshake was proven on day 1.

### Pipeline B: the Snowflake loader (2026-10-27)

- `src/snowflake_loader.py`: `snowflake_connection()` as a context manager
  (key pair as WEATHER_SVC, role WEATHER_ENGINEER, LOAD_WH, every value from
  the environment with the missing name in the error, login and network
  timeouts, session id logged on open and close, closed on every path), and
  `copy_flat(conn, dt) -> LoadResult`. `dt` is validated with
  `date.fromisoformat` before it touches the SQL string; that is the injection
  guard, since a stage path cannot be a bind variable. The result is read by
  column name from `cursor.description`: when nothing is new the result is a
  single status row with no `rows_loaded` column.
- `LoadError` wraps `ProgrammingError` with the date, the stage path and the
  query id. Nothing else is caught; no retries in the module.
- `SNOWFLAKE_LOAD_WAREHOUSE` (default `LOAD_WH`) added to `.env.example`,
  because `SNOWFLAKE_WAREHOUSE` is dbt's TRANSFORM_WH.
- Run by hand for dt=2026-10-27: 600 rows, equal to the CSV. Session and query
  ids in `docs/pipeline_runs.md`.

### Pipeline C: the loader proven (2026-10-27)

- Second run for the same date: 0 files, 0 rows (load metadata).
  `COPY_HISTORY` shows the file once. Both in `docs/pipeline_runs.md`.
- `docs/decisions/0001-load-idempotency.md`: same name with new contents, the
  64 day metadata window and `LOAD_UNCERTAIN_FILES`, `FORCE = TRUE`, and dry
  runs. Found while writing it: `VALIDATION_MODE` cannot be combined with
  `MATCH_BY_COLUMN_NAME` (error 002353); the dry run uses the positional
  `CSV_HEADER` format instead, confirmed against dt=2026-10-26.
- `tests/test_snowflake_loader.py`: fake cursor and connection; one loaded
  file, two files summed, the zero-file status row, `ProgrammingError` to
  `LoadError` with the query id and cause, a bad date sends no SQL, the cursor
  is closed on both paths, and the SQL names the stage, format and date.
- `src/quality_checks.fetch_run_volumes` now takes a Snowflake connection:
  `%(run_id)s` binds, schema `RAW`. The pure `evaluate_volume_anomaly` and its
  tests are unchanged; the wrapper tests only renamed `engine=` to `conn=`.
- 84 tests pass.
- **Known break until day 3:** the live DAG still calls
  `check_volume_anomaly(get_engine(), ...)` with a SQLAlchemy engine. The
  ported function expects a Snowflake connection, so tonight's scheduled run
  fails at `volume_anomaly_check`. Postgres is still loaded before that task,
  and the S3 uploads still land. Day 3 rewires the DAG to the Snowflake loader
  and this check.

### End of day, 2026-10-27 (day 2)

- **Where each block stopped.** A: done, `S3_FLAT_STAGE` listed 3 CSVs. B:
  done, loader built and run. C: done, run twice, history read, tests green,
  ADR written. Your one line on SKIP_HEADER vs PARSE_HEADER is still owed.
- **Last error text.** `002353 (0A000): SQL compilation error:
  MATCH_BY_COLUMN_NAME cannot be used with the VALIDATION_MODE because
  VALIDATION_MODE does not support COPY with transform.` Handled in the ADR.
- **First ten minutes of tomorrow (day 3).** Open
  `dags/weather_forecast_pipeline.py`. Replace `load_raw_table` with a task
  that opens `snowflake_connection()` and calls `copy_flat(conn, run_ts date)`,
  and `volume_anomaly_check` with the same connection. Remove `load_records`
  to Postgres from the chain. Then trigger a run and watch `load_snowflake`.

## Day 3: the DAG on S3 and Snowflake; EC2; Docker on the box

### Pipeline A: weather_daily (2026-10-27)

- `dags/weather_forecast_pipeline.py` renamed to `dags/weather_daily.py`,
  `dag_id="weather_daily"`, schedule `0 6 * * *`, `start_date` 2026-10-01,
  `catchup=False`, `max_active_runs=1`. Chain: extract, transform,
  write_to_s3, copy_into_snowflake, validate_row_count, volume_anomaly_check,
  dbt_source_freshness, dbt_build. Nothing imports `postgres_loader` or
  `get_engine`.
- The partition date is `data_interval_end` from the run, passed on by XCom.
  The file timestamp is still the write time, so every attempt writes a new
  object name. `landing.py` gained a `dt` argument and `upload_flat_frame`
  (frame to CSV bytes to S3, no local file).
- Retries: two, five minutes apart, by default; none on `dbt_build`. Why, and
  why `ds` would be wrong, in `docs/airflow_answers.md`. Found on the manual
  trigger: Airflow 2.8 gives a manual run the most recent *completed*
  interval, so `data_interval_end` was 06:00 today, not the trigger time. A
  manual trigger before 06:00 UTC lands in yesterday's partition. Written down.
- Freshness back to 26 and 50 hours. `postgres-warehouse` marked unused in
  compose, kept for the dbt `dev` target until day 27.
- Manual run green end to end on the laptop; 600 rows under `manual__day3_a`.
  Details in `docs/pipeline_runs.md`. 86 tests pass.
- The old `dag_id` is gone from the DAG list; its run history is still in the
  metadata database under the old name.

### Pipeline B: the budget, the instance, the role, and the fence (2026-10-29)

- Done from CloudShell with `infra/ec2/create_box.sh` instead of the console,
  so it is repeatable and reviewable: one paste of three variables (laptop
  IP, SSH public key, alert email) and a `curl | bash` of the script from the
  branch. Budget `weather-hosting` at $40 a month (a t3.medium running all
  month is about $36; $20 or $30 would alert every month during normal
  running), alerts at 50, 80 and 100 percent.
- The SSH key pair was generated on the laptop (`~/.ssh/weather-ec2`, ed25519)
  and only the public half imported, so no private key was ever downloaded
  from AWS.
- Role `weather-ec2-role` with the day 1 `extractor_write_policy.json` and an
  instance profile, attached at launch. Security group `sg-00755a4be383bf5e7`:
  port 22 from `<home ip>/32`, nothing else.
- Instance `i-06974584beba5796b`, Ubuntu 24.04, t3.medium, 20 GB gp3, in
  us-east-2, public IP `<box public ip>`. `aws sts get-caller-identity` on the
  box: `assumed-role/weather-ec2-role/i-06974584beba5796b`.
- Not yet done: deleting the `weather-writer` user and its keys. The laptop's
  `.env` still holds them for laptop runs; they go once the laptop is no longer
  a place the pipeline runs from. The box owns the schedule now, so day 5 or 6.

### Pipeline C: Docker, the repo, the secrets, the image (2026-10-29)

- `infra/ec2/bootstrap_box.sh` on the box: Docker Engine and the compose
  plugin from Docker's apt repository, `usermod -aG docker ubuntu`, UTC, the
  AWS CLI (snap), the clone over HTTPS to `~/weather`.
- Secrets by `scp`: `keys/rsa_key.p8` and a `.env` built on the laptop with
  the AWS key lines removed, `AIRFLOW_UID=1000`, a fresh Fernet key, fresh
  webserver and API secrets, and a random `AIRFLOW_WWW_PASSWORD`. Both mode
  600; the laptop copy of the box `.env` was shredded after the copy.
- `docker-compose.yml`: the admin user's password now comes from
  `AIRFLOW_WWW_PASSWORD` (default `admin` for the laptop). `restart: always`,
  `LOAD_EXAMPLES=false` and the Fernet key from env were already in place
  since day 1, so the guide's `chore(airflow): restart policies...` commit had
  nothing left to do.
- No Snowflake network policy exists on `WEATHER_SVC` or the account, so
  nothing to add for the box's IP.
- `docker compose build` about 10 minutes, `up -d`, every service healthy,
  `list-import-errors` empty. Inside the scheduler container:
  `import src.snowflake_loader, src.s3` works and boto3's STS identity is the
  instance role. Done.

### Day 4, Pipeline A: the manual run on the box, and the handover (2026-10-29)

- `manual__box_1` green end to end on the box, same numbers as the laptop.
  Details in `docs/pipeline_runs.md`.
- Handover: unpaused on the box, paused on the laptop. The local stack is
  still running (dbt `dev` target, Postgres); it no longer runs the schedule
  and can be stopped with `docker compose stop` any time.
- `docs/hosting.md` written; the price line is still to fill in.
- Reboot test: `sudo systemctl reboot` at 16:31:17 UTC; the box was up at 16:31:31 and every container was healthy within a minute with no login, and the DAG was still unpaused. `restart: always` holds. Day 4A done.

### Day 4, Pipeline C: the Snowflake alert for the run that never happened (2026-10-29)

- Email verified on the Snowflake user profile; `WEATHER_EMAIL_INT` and the
  grants run as ACCOUNTADMIN from `setup/09_alerts.sql`. One grant the plan
  did not list was needed: `CREATE ALERT ON SCHEMA WEATHER_DB.RAW`, because
  `WEATHER_ENGINEER` does not own the schema. Added to the file.
- `RAW.WEATHER_STALE` created as `WEATHER_ENGINEER`: serverless (no
  warehouse), `USING CRON 0 12 * * * UTC`, condition `MAX(ingested_at)` null
  or older than 26 hours against `SYSDATE()`, action `SYSTEM$SEND_EMAIL`.
  Resumed; `SHOW ALERTS` says `started`.
- Two things learned while proving it. `ALTER ALERT ... MODIFY CONDITION`
  is refused unless the alert is suspended, so the proof is suspend, modify,
  resume, execute. And `EXECUTE ALERT` is asynchronous: the first attempt
  reset the threshold before the evaluation ran and got `CONDITION_FALSE`
  with no email. The second attempt waited for `ALERT_HISTORY` to show a
  completed evaluation before restoring the threshold.
- `ALERT_HISTORY`: `CONDITION_FALSE` at 16:57:28 UTC (26 hour threshold),
  `TRIGGERED` at 16:58:30 UTC (1 minute threshold), no error. The threshold
  is back at 26 hours and the next scheduled evaluation is 2026-10-30 12:00
  UTC. The test email is the one thing still to confirm from the inbox.
- Why serverless, and the cost, in `docs/decisions/0002-stale-data-alert.md`.

### Day 4, Pipeline B: break transform, clear it alone, the failure alert (2026-10-29)

- `src/alerts.py`: `notify_failure` posts one line (`dag_id.task_id try N
  failed` and the log URL) to an ntfy.sh topic from `NTFY_TOPIC`. No topic
  means log only; a failed post is logged and swallowed. Tested with a fake
  `post`. The callback calls it after the log line that nobody reads.
- `execution_timeout` 20 minutes on every task, `dagrun_timeout` 1 hour.
  What state a timed out task lands in: `docs/airflow_answers.md`.
- Test post from the laptop reached the phone. Then the real thing:
  transform broken on the box, three failures, the callback fired ("Failure
  sent to ntfy topic" in attempt 3's log), the notification arrived. Repaired,
  `transform` cleared alone with downstream, run green, `extract` still on one
  attempt, one `COPY`. Details in `docs/pipeline_runs.md`.
- The planted callback-without-context bug is on the deferred list (day 13).

### End of day, 2026-10-29 (days 3 and 4)

- **Where each block stopped.** Day 3 A, B, C and day 4 A, B, C all done and
  proven: the box runs the schedule, holds no AWS keys, survived a reboot;
  a task failure reaches the phone; a missing run reaches the inbox. Laptop
  DAG paused and the local stack stopped. Open: delete `weather-writer`
  (day 6, with "done"), the SKIP_HEADER line, the deferred readings.
- **Last error text.** `RuntimeError: deliberate break for day 4B: transform
  fails on purpose`, three times, on purpose. Before that, the real one:
  `392005 (55000): Unable to alter property for alert WEATHER_STALE since it
  is not suspended.` Handled: suspend before `MODIFY CONDITION`.
- **First ten minutes of tomorrow (day 5).** Do not touch the box. Open the
  tunnel, read the 06:00 UTC run's grid and each task's log; then in
  Snowflake, `COPY_HISTORY` for the file and `WAREHOUSE_METERING_HISTORY` for
  `LOAD_WH` and `TRANSFORM_WH` since 06:00; then the reconciliation (CSV rows,
  `rows_loaded`, RAW rows for the run, staging rows after the dedupe) into
  `docs/pipeline_runs.md`, with minutes and credits as the baseline.

## Day 5: the first unattended run, the README, the cleanup (2026-10-30)

### Pipeline A: the run read three ways

`scheduled__2026-10-29T06:00:00+00:00` ran at 06:00:00 UTC on the box with
the laptop off and finished green at 06:00:50. Read from the task log, from
`COPY_HISTORY` and `QUERY_HISTORY`, and from the bucket; all three agree on
one file, 600 rows, 16 objects. The full entry, with the per-task timings,
the warehouse seconds and the overlap count for day 8 (4408 of 6000 rows are
re-forecasts by `(city_id, dt)`, 1764 of them identical in every API column),
is in `docs/pipeline_runs.md`.

- **Trap hit.** `WAREHOUSE_METERING_HISTORY` returns no rows, not an error,
  for a role without `MONITOR` on the warehouse. The credits baseline is
  blocked on a grant (below). Time on each warehouse stands in.
- `RAW.WEATHER_STALE` evaluated at 12:00 UTC: `CONDITION_FALSE`. Correct.

### Pipeline B: the README and the stale files

- README rewritten for the pipeline as it is: S3 to Snowflake, the box, the
  alerts, a Mermaid diagram instead of the old DAG screenshot, the numbers
  from the first unattended run, the decisions with their ADRs, and a "Where
  Postgres Fits" section in place of the Postgres instructions. The Status
  line is gone.
- Removed: `setup/load/copy_raw_tables.sql` and the internal stages in
  `06_stage.sql` (superseded by `src/snowflake_loader.py` and the S3 stage;
  the `DROP STAGE` lines are in a comment at the end of `06_stage.sql`),
  `docs/postgres_to_snowflake.md` (the three errors are in the README's
  incident log), `docs/airflow_dag.png` (the old DAG), `data/processed/`.
- `Makefile`: `trigger` points at `weather_daily`, `backfill` and `psql` are
  gone, `runs`, `load DT=` and `test` are new.
- Still here until day 27: `src/postgres_loader.py`, its test, the Postgres
  load in `src/main.py`, the `postgres-warehouse` service.

### For Patrick (console steps, in order)

1. In Snowflake as `ACCOUNTADMIN`, so the credits baseline can be read:
   `GRANT MONITOR ON WAREHOUSE LOAD_WH TO ROLE WEATHER_ENGINEER;` and the same
   for `TRANSFORM_WH`. Optional: `DROP STAGE IF EXISTS RAW.EXPORT_STAGE;` and
   `RAW.JSON_STAGE`.
2. Resource monitor on the account (ACCOUNTADMIN): check `TRIAL_GUARD` from
   `00_account.sql` still exists and its quota is sensible for about 0.05
   credits a day; `SHOW RESOURCE MONITORS;`.
3. CloudWatch alarm on the instance: `StatusCheckFailed` >= 1 for 2 periods
   of 5 minutes, action: email through an SNS topic. EC2 console, the
   instance, Monitoring tab, "Create alarm".
4. Repo link on the CV and LinkedIn.
5. Optional: through the tunnel, a screenshot of the green grid for
   `docs/airflow_grid.png`, if a picture is wanted next to the diagram.

### End of day, 2026-10-30 (day 5)

- **Where each block stopped.** 5A done and written up. 5B done: README,
  Makefile, four files removed. Patrick's list above is open.
- **Last error text.** None today. The one surprise was the empty metering
  result, which is a grant, not an error.
- **First ten minutes of tomorrow (day 6).** Read the second unattended run
  the same three ways, shorter: `list-runs`, `COPY_HISTORY` for the file,
  the S3 count. Then the four-part "done" evidence into the README's
  opening paragraph and `pipeline_runs.md`. Then, in CloudShell, delete the
  `weather-writer` access keys and user, and remove the AWS key lines from
  the laptop `.env`.
