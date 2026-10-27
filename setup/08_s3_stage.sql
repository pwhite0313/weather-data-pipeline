-- Flat CSV stage on S3 and the two CSV file formats the loader uses.
-- Run as WEATHER_ENGINEER, which owns RAW and has USAGE on S3_WEATHER_INT.
USE ROLE WEATHER_ENGINEER;
USE SCHEMA WEATHER_DB.RAW;


-- CSV_HEADER was created in 06_stage.sql with SKIP_HEADER = 1. Two options
-- added here, with ALTER so the existing stages keep pointing at it:
--   NULL_IF: pandas writes NaN as '', the other two are defensive.
--   ERROR_ON_COLUMN_COUNT_MISMATCH: positional loads fail loudly when the CSV
--   has a different column count from the table, instead of loading shifted
--   values.
ALTER FILE FORMAT RAW.CSV_HEADER SET
  NULL_IF = ('', 'NA', '-999')
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;


-- Second format, for the loader: PARSE_HEADER reads the header row as column
-- names so COPY can MATCH_BY_COLUMN_NAME. A column added or reordered in the
-- frame then lands in the right table column instead of shifting every value
-- one place. SKIP_HEADER cannot be set alongside PARSE_HEADER: the header row
-- is either data to skip or names to read, not both.
CREATE FILE FORMAT IF NOT EXISTS RAW.CSV_PARSE_HEADER
  TYPE = CSV
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NA', '-999');


-- One stage per prefix. S3_WEATHER_STAGE (06_stage.sql) sits at weather/ for
-- listing and the raw JSON side; this one is bound to the CSV path and format.
CREATE STAGE IF NOT EXISTS RAW.S3_FLAT_STAGE
  STORAGE_INTEGRATION = S3_WEATHER_INT
  URL = 's3://pw-weather-2026/weather/flat/'
  FILE_FORMAT = RAW.CSV_HEADER;

-- Handshake proven for this stage: expect one CSV per run, under dt=... paths.
LIST @RAW.S3_FLAT_STAGE;
