dbt run --target snowflake
15:32:17  Running with dbt=1.11.8
15:32:17  Registered adapter: snowflake=1.12.1
15:32:17  Encountered an error:
Compilation Error
  dbt found 1 package(s) specified in packages.yml, but only 0 package(s) installed in dbt_packages. Run "dbt deps" to install package dependencies.

  ---------------

dbt run --target snowflake
15:44:27  Running with dbt=1.11.8
15:44:28  Registered adapter: snowflake=1.12.1
15:44:28  Unable to do partial parsing because saved manifest not found. Starting full parse.
15:44:28  [WARNING][PropertyMovedToConfigDeprecation]: Deprecated functionality
Found `freshness` as a top-level property of `sources[0].tables[0]` in file
`models/staging/weather/weather_sources.yml`. The `freshness` top-level property
should be moved into the `config` of `sources[0].tables[0]`.
15:44:29  [WARNING][MissingArgumentsPropertyInGenericTestDeprecation]: Deprecated
functionality
Found top-level arguments to test `accepted_values` defined on
'int_weather_enriched' in package 'weather_pipeline'
(models/intermediate/schema.yml). Arguments to generic tests should be nested
under the `arguments` property.
15:44:29  Found 6 models, 51 data tests, 1 source, 679 macros
15:44:29  
15:44:29  Concurrency: 4 threads (target='snowflake')
15:44:29  
15:44:33  
15:44:33  Finished running  in 0 hours 0 minutes and 3.70 seconds (3.70s).
15:44:33  Encountered an error:
Database Error
  003001 (42501): SQL access control error:
  Insufficient privileges to operate on database 'WEATHER_DB'. Your primary role WEATHER_ENGINEER must have CREATE SCHEMA granted on DATABASE WEATHER_DB.
15:44:33  [WARNING][DeprecationsSummary]: Deprecated functionality
Summary of encountered deprecations:
- PropertyMovedToConfigDeprecation: 2 occurrences
- MissingArgumentsPropertyInGenericTestDeprecation: 5 occurrences
To see all deprecation instances instead of just the first occurrence of each,
run command again with the `--show-all-deprecations` flag. You may also need to
run with `--no-partial-parse` as some deprecations are only encountered during
parsing.



### Schema creation (configuration, not dialect)
- Error: Insufficient privileges ... must have CREATE SCHEMA granted on DATABASE WEATHER_DB
- Why it differs: the project's generate_schema_name macro passes the custom schema
  through verbatim, so dbt expected ANALYTICS, INTERMEDIATE and REPORTS. The setup
  files had created MARTS and neither of the other two. On Postgres the dev user could
  create the missing schemas itself; on Snowflake the service role can only use the
  schemas the setup files create, so the mismatch surfaced as a privilege error.
- Change: setup/02_database.sql now creates ANALYTICS, INTERMEDIATE and REPORTS and
  MARTS was dropped. setup/03_roles_and_grants.sql grants USAGE, CREATE TABLE and
  CREATE VIEW on each. CREATE SCHEMA was deliberately not granted to the role.


-------------------
dbt run --target snowflake
16:02:51  Running with dbt=1.11.8
16:02:51  Registered adapter: snowflake=1.12.1
16:02:52  Found 6 models, 51 data tests, 1 source, 679 macros
16:02:52  
16:02:52  Concurrency: 4 threads (target='snowflake')
16:02:52  
16:02:55  
16:02:55  Finished running  in 0 hours 0 minutes and 2.75 seconds (2.75s).
16:02:55  Encountered an error:
Database Error
  003001 (42501): SQL access control error:
  Insufficient privileges to operate on database 'WEATHER_DB'. Your primary role WEATHER_ENGINEER must have CREATE SCHEMA granted on DATABASE WEATHER_DB.


  #####
  dbt and Snowflake schemas were nto 1:1. DBT had analytics, snowflake had MARTS.
  Created new schema, dropped the old, and granted privileges

----------
(venv) pwhit@PWDS:~/weather_pipeline_snowflake/dbt/models$ dbt run --target snowflake
16:35:17  Running with dbt=1.11.8
16:35:17  Registered adapter: snowflake=1.12.1
16:35:18  Found 6 models, 51 data tests, 1 source, 679 macros
16:35:18  
16:35:18  Concurrency: 4 threads (target='snowflake')
16:35:18  
16:35:20  1 of 6 START sql view model staging.stg_weather__forecast ...................... [RUN]
16:35:21  1 of 6 OK created sql view model staging.stg_weather__forecast ................. [SUCCESS 1 in 0.50s]
16:35:21  2 of 6 START sql table model analytics.dim_city ................................ [RUN]
16:35:21  3 of 6 START sql incremental model analytics.fct_weather_forecast .............. [RUN]
16:35:21  4 of 6 START sql view model intermediate.int_weather_enriched .................. [RUN]
16:35:21  4 of 6 OK created sql view model intermediate.int_weather_enriched ............. [SUCCESS 1 in 0.62s]
16:35:21  5 of 6 START sql view model reports.rpt_current_conditions ..................... [RUN]
16:35:21  5 of 6 ERROR creating sql view model reports.rpt_current_conditions ............ [ERROR in 0.30s]
16:35:22  2 of 6 OK created sql table model analytics.dim_city ........................... [SUCCESS 1 in 1.67s]
16:35:22  3 of 6 OK created sql incremental model analytics.fct_weather_forecast ......... [SUCCESS 1 in 1.79s]
16:35:22  6 of 6 START sql table model analytics.fct_weather_daily ....................... [RUN]
16:35:23  6 of 6 OK created sql table model analytics.fct_weather_daily .................. [SUCCESS 1 in 0.91s]
16:35:24  
16:35:24  Finished running 1 incremental model, 2 table models, 3 view models in 0 hours 0 minutes and 6.08 seconds (6.08s).
16:35:24  
16:35:24  Completed with 1 error, 0 partial successes, and 0 warnings:
16:35:24  
16:35:24  Failure in model rpt_current_conditions (models/reports/rpt_current_conditions.sql)
16:35:24    Database Error in model rpt_current_conditions (models/reports/rpt_current_conditions.sql)
  002139 (02000): SQL compilation error:
  Unknown functions NOW, NOW.
  compiled code at target/run/weather_pipeline/models/reports/rpt_current_conditions.sql
16:35:24  
16:35:24    compiled code at target/compiled/weather_pipeline/models/reports/rpt_current_conditions.sql
16:35:24  
16:35:24  Done. PASS=5 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=6


######
DIALECT ISSUE "NOW" is unknown functio for snowflake
dbt has a macro that works for both {{ dbt.current_timestamp() }}


------------------
dbt run --target snowflake --select rpt_current_conditions
16:43:34  Running with dbt=1.11.8
16:43:35  Registered adapter: snowflake=1.12.1
16:43:36  Found 6 models, 51 data tests, 1 source, 679 macros
16:43:36  
16:43:36  Concurrency: 4 threads (target='snowflake')
16:43:36  
16:43:38  1 of 1 START sql view model reports.rpt_current_conditions ..................... [RUN]
16:43:38  1 of 1 ERROR creating sql view model reports.rpt_current_conditions ............ [ERROR in 0.24s]
16:43:39  
16:43:39  Finished running 1 view model in 0 hours 0 minutes and 3.33 seconds (3.33s).
16:43:39  
16:43:39  Completed with 1 error, 0 partial successes, and 0 warnings:
16:43:39  
16:43:39  Failure in model rpt_current_conditions (models/reports/rpt_current_conditions.sql)
16:43:39    Database Error in model rpt_current_conditions (models/reports/rpt_current_conditions.sql)
  001008 (22023): SQL compilation error:
  invalid value [EPOCH] for parameter 'EXTRACT date/time part'
  compiled code at target/run/weather_pipeline/models/reports/rpt_current_conditions.sql
16:43:39  
16:43:39    compiled code at target/compiled/weather_pipeline/models/reports/rpt_current_conditions.sql
16:43:39  
16:43:39  Done. PASS=0 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=1


CODE: ABS(EXTRACT(EPOCH FROM (local_dt - now())))

### rpt_current_conditions: EXTRACT(EPOCH FROM interval)
- Error: invalid value [EPOCH] for parameter 'EXTRACT date/time part'
- Why the dialect differs: Postgres subtracts timestamps into an interval and EXTRACT(EPOCH)
  turns it into seconds. Snowflake has no interval result to extract from; time differences
  are measured with DATEDIFF(unit, start, end).
- Change: ABS({{ dbt.datediff("local_dt", dbt.current_timestamp(), "second") }})