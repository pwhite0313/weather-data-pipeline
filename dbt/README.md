# weather_pipeline (dbt)

Transformation layer for the weather pipeline. Six models across staging,
intermediate, marts and reports, plus a city snapshot and 53 tests. Runs
against Postgres (`dev`, `prod`) or Snowflake (`snowflake`) via `DBT_TARGET`.

Run from the host, with `.env` sourced from the repo root:

    cd dbt
    dbt deps
    WAREHOUSE_HOST=localhost dbt build            # Postgres
    dbt build --target snowflake                  # Snowflake

Model descriptions, the test suite, how dbt picks a `profiles.yml`, and the
Snowflake setup are all documented in the
[root README](../README.md#dbt-models).
