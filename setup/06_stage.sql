-- File formats and stages in RAW. Safe to rerun: everything is IF NOT EXISTS.
USE ROLE WEATHER_ENGINEER;
USE SCHEMA WEATHER_DB.RAW;


-- FILE FORMATS

-- SKIP_HEADER = 1 means COPY maps CSV columns to table columns by position,
-- so the CSV's column order must match RAW.WEATHER_FORECAST exactly.
-- tests/test_load.py::TestCsvMatchesSnowflakeRawTable enforces that.
-- NULL_IF and ERROR_ON_COLUMN_COUNT_MISMATCH were added on 2026-10-27; on an
-- account where the format already exists, 08_s3_stage.sql ALTERs it to match.
CREATE FILE FORMAT IF NOT EXISTS RAW.CSV_HEADER
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NA', '-999')
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;

CREATE FILE FORMAT IF NOT EXISTS RAW.JSON_FORMAT
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;


-- EXTERNAL STAGE ON S3 (2026-10-27)
-- Authenticates through S3_WEATHER_INT (setup/07_storage_integration.sql), so
-- no AWS keys live in Snowflake. Objects land under weather/raw/dt=.../ (one
-- JSON per city per run) and weather/flat/dt=.../ (one CSV per run).
CREATE STAGE IF NOT EXISTS RAW.S3_WEATHER_STAGE
  STORAGE_INTEGRATION = S3_WEATHER_INT
  URL = 's3://pw-weather-2026/weather/';

-- Proof the integration handshake works: lists the bucket prefix via the role.
LIST @RAW.S3_WEATHER_STAGE;

-- The internal stages RAW.EXPORT_STAGE and RAW.JSON_STAGE (the manual PUT
-- path used for the first loads, before S3) were removed from this script on
-- 2026-10-30. On an account where they still exist:
--   DROP STAGE IF EXISTS RAW.EXPORT_STAGE;
--   DROP STAGE IF EXISTS RAW.JSON_STAGE;
