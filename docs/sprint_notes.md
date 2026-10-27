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
