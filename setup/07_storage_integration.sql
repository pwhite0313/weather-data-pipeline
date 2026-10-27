-- Storage integration: lets Snowflake assume an IAM role to read the bucket.
-- Run as ACCOUNTADMIN; only that role can create integrations.
--
-- Never CREATE OR REPLACE this integration. Replacing it issues a new
-- STORAGE_AWS_EXTERNAL_ID, the role's trust policy keeps the old one, and the
-- next LIST fails with no error at creation time. Use ALTER STORAGE INTEGRATION
-- for any change after the first CREATE.
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_WEATHER_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::870947613885:role/snowflake-weather-read'
  STORAGE_ALLOWED_LOCATIONS = ('s3://pw-weather-2026/weather/');

-- Pass two: copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from this
-- output into the role's trust policy (infra/iam/snowflake_read_trust_policy.json
-- is the template; the real values are applied from CloudShell, not committed).
DESC INTEGRATION S3_WEATHER_INT;

-- After the trust policy is updated:
GRANT USAGE ON INTEGRATION S3_WEATHER_INT TO ROLE SYSADMIN;
GRANT USAGE ON INTEGRATION S3_WEATHER_INT TO ROLE WEATHER_ENGINEER;
