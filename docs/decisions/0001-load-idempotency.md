# ADR 0001: how a load into RAW.WEATHER_FORECAST stays idempotent

Date: 2026-10-27. Status: accepted.

## Context

`src/snowflake_loader.copy_flat` runs one `COPY INTO RAW.WEATHER_FORECAST`
from `@RAW.S3_FLAT_STAGE/dt=<date>/`. Airflow retries a failed task, so the
same COPY will sometimes run twice for the same date. The table must end up
with each file's rows exactly once.

## What Snowflake gives us

COPY keeps load metadata per table: the name, ETag and last-modified time of
every file it has loaded, for 64 days. A COPY that sees a file already in that
metadata skips it. A second run for the same date therefore returns one status
row, `Copy executed with 0 files processed.`, loads nothing, and succeeds.
That is what makes the task safe to retry: a retry after a partial failure
loads only what the first attempt did not.

## Decisions for the cases load metadata does not cover

1. **Same file name, new contents.** Load metadata matches on name, ETag and
   last-modified. A rewritten file with the same name gets a new ETag, so COPY
   would load it again, and the table would hold both versions. The pipeline
   never produces that case: every run writes `output_<run timestamp to the
   microsecond>.csv`, so a second run on the same date is a new name, loaded
   once, and `stg_weather__forecast`'s dedupe keeps one row per forecast slot
   across runs. The timestamp in the file name exists for this reason.

2. **A file older than 64 days.** Load metadata expires. COPY then does not
   know whether an old file was loaded and, by default, skips it
   (`LOAD_UNCERTAIN_FILES = FALSE`). A backfill of a partition older than 64
   days must set `LOAD_UNCERTAIN_FILES = TRUE` and rely on the staging dedupe.
   The loader does not set it; a backfill is a deliberate, separate command.

3. **A forced reload.** `FORCE = TRUE` loads every file in the path again,
   metadata or not, and the raw table then holds duplicates. Allowed only by
   hand, for a known-bad load, and only because the staging model dedupes with
   deterministic tie-breakers (PR #1). Never in the DAG.

## Dry runs

`COPY ... VALIDATION_MODE = RETURN_ERRORS` parses the files against the table
without loading and returns the rows that would fail. One constraint, found on
2026-10-27: it cannot be combined with `MATCH_BY_COLUMN_NAME` (Snowflake error
002353, "VALIDATION_MODE does not support COPY with transform"). A dry run
therefore uses the positional format:

```sql
COPY INTO RAW.WEATHER_FORECAST FROM @RAW.S3_FLAT_STAGE/dt=<date>/
  FILE_FORMAT = (FORMAT_NAME = RAW.CSV_HEADER)
  VALIDATION_MODE = RETURN_ERRORS;
```

That checks parsing, types and the column count (`ERROR_ON_COLUMN_COUNT_MISMATCH`),
which is most of what a changed file shape breaks. Use it when the shape
changes: a column added by the API, a changed file format, a new `NULL_IF`.
Do not run it on every load; it is a second full scan of the files, and the
loader's `ON_ERROR = ABORT_STATEMENT` already refuses a bad file.

## Consequences

- The DAG can retry the load task with no special handling.
- Rows in RAW can repeat across runs; uniqueness is the staging layer's job.
- Backfills older than 64 days and forced reloads are manual and documented
  in `docs/pipeline_runs.md` when they happen.
