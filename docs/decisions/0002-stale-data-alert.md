# ADR 0002: the stale data alert runs serverless, outside the box

Date: 2026-10-29. Status: accepted, pending the first test email.

## Context

The DAG's failure callback (ADR-adjacent: `docs/airflow_answers.md`) posts to
a phone when a task fails. It cannot report the case where nothing runs: the
instance stopped, the scheduler hung, the DAG was paused, the laptop that
used to own the schedule was shut. In every one of those, no task fails and
nothing pages.

The check therefore has to live somewhere the box is not. Snowflake is where
the data lands, so the question it can ask is the one that matters: did
anything new arrive?

## Decision

`RAW.WEATHER_STALE`, a Snowflake alert (`setup/09_alerts.sql`) on a daily
cron at 12:00 UTC, six hours after the 06:00 run is due. Its condition is
`MAX(ingested_at)` over `RAW.WEATHER_FORECAST` older than 26 hours, or null
(an empty table has no maximum, and that is stale too). Its action is one
email through `WEATHER_EMAIL_INT` naming the table and what to check first.

**Serverless, not warehouse-backed.** The condition is a single aggregate
over one small table, once a day: a few seconds of compute. With
`WAREHOUSE = LOAD_WH` the alert would resume the warehouse every day and pay
the sixty second minimum: an XSMALL is 1 credit an hour, so 60 seconds is
about 0.017 credits a day, about 0.5 credits a month, for maybe two seconds
of work. Serverless compute is billed per second at the serverless rate; for
this condition that is on the order of 0.001 credits a day. Either figure is
small, but the serverless one is the right shape: the cost tracks the work.
The real number comes from `ALERT_HISTORY` and the account's serverless
credit usage after a week, and goes here when it is known.

## Two traps, not planted

- `ingested_at` is `TIMESTAMP_NTZ` written in UTC. `CURRENT_TIMESTAMP()` is
  in the session's time zone, so a comparison against it is off by the zone
  offset. `SYSDATE()` is UTC and is what the condition uses.
- `CREATE ALERT` creates it suspended. Without `ALTER ALERT ... RESUME` it
  never evaluates and never says so.

## Consequences

- Two independent alert paths: the callback for "a task failed", the alert
  for "nothing happened". Done's fourth clause needs both.
- `WEATHER_ENGINEER` needs `EXECUTE MANAGED ALERT` (serverless) and usage on
  the email integration; both are ACCOUNTADMIN grants.
- The email address is on the Snowflake user profile and in the integration's
  allowed recipients, not in the repository; the committed SQL has a
  placeholder.
