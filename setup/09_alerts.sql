-- The alert for the run that never happened.
--
-- The ntfy callback in the DAG fires when a task fails. If the box is off,
-- the scheduler is stuck, or the DAG got paused, no task runs, nothing fails,
-- and nothing pages. This check lives where the data lands, so it asks the
-- only question that matters: has anything new arrived?
--
-- Part one is ACCOUNTADMIN work and needs a verified email address on the
-- Snowflake user profile first (Snowsight: your profile, email, verify).
-- Unverified addresses are refused by the notification integration.

USE ROLE ACCOUNTADMIN;

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS WEATHER_EMAIL_INT
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('<your email>');

GRANT USAGE ON INTEGRATION WEATHER_EMAIL_INT TO ROLE WEATHER_ENGINEER;
-- Serverless alerts (no WAREHOUSE clause) need this one; warehouse-backed
-- alerts need EXECUTE ALERT instead. Both granted so either form works.
GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE WEATHER_ENGINEER;
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE WEATHER_ENGINEER;
-- WEATHER_ENGINEER does not own the RAW schema, so creating an alert in it
-- needs this too (found on 2026-10-29: "must have CREATE ALERT granted on SCHEMA").
GRANT CREATE ALERT ON SCHEMA WEATHER_DB.RAW TO ROLE WEATHER_ENGINEER;


-- Part two, as the pipeline's own role.
USE ROLE WEATHER_ENGINEER;
USE SCHEMA WEATHER_DB.RAW;

-- Serverless (no WAREHOUSE): the condition is one aggregate over one table
-- once a day, which is seconds of compute. A warehouse would resume for it
-- and bill the sixty second minimum every day. See docs/decisions/0002.
--
-- Two things the condition gets right on purpose:
--   ingested_at is TIMESTAMP_NTZ in UTC, so it is compared to SYSDATE()
--   (UTC), not CURRENT_TIMESTAMP() (session time zone).
--   An empty table has no MAX to compare, so a NULL counts as stale.
CREATE ALERT IF NOT EXISTS RAW.WEATHER_STALE
  SCHEDULE = 'USING CRON 0 12 * * * UTC'   -- six hours after the 06:00 run is due
  IF (EXISTS (
    SELECT 1
    FROM (SELECT MAX(ingested_at) AS last_load FROM RAW.WEATHER_FORECAST)
    WHERE last_load IS NULL OR last_load < DATEADD(hour, -26, SYSDATE())))
  THEN CALL SYSTEM$SEND_EMAIL(
    'WEATHER_EMAIL_INT',
    '<your email>',
    'weather_daily: no new rows in RAW.WEATHER_FORECAST for 26 hours',
    'Nothing has landed since the last ingested_at. Check the box first: ssh in, docker compose ps, then the scheduler log (docker compose logs --since 24h airflow-scheduler). Then the Airflow grid through the tunnel, then S3 for today''s dt= prefix.');

-- Alerts are created suspended.
ALTER ALERT RAW.WEATHER_STALE RESUME;


-- Proof, once: make it fire, see the email, put it back.
-- ALTER ALERT RAW.WEATHER_STALE MODIFY CONDITION EXISTS (
--   SELECT 1 FROM (SELECT MAX(ingested_at) AS last_load FROM RAW.WEATHER_FORECAST)
--   WHERE last_load IS NULL OR last_load < DATEADD(minute, -1, SYSDATE()));
-- EXECUTE ALERT RAW.WEATHER_STALE;
-- ALTER ALERT RAW.WEATHER_STALE MODIFY CONDITION EXISTS (
--   SELECT 1 FROM (SELECT MAX(ingested_at) AS last_load FROM RAW.WEATHER_FORECAST)
--   WHERE last_load IS NULL OR last_load < DATEADD(hour, -26, SYSDATE()));

-- Both evaluations, one triggered and one not:
-- SELECT name, state, scheduled_time, completed_time, sql_error_message
-- FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
--   SCHEDULED_TIME_RANGE_START => DATEADD(hour, -24, CURRENT_TIMESTAMP())))
-- ORDER BY scheduled_time DESC;
