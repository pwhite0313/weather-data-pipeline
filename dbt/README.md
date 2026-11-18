# weather_pipeline (dbt)

Transformation layer for the weather pipeline. Six models across staging,
intermediate, marts and reports, plus a city snapshot and 53 tests. Runs
against Snowflake (`snowflake`, the default) or Postgres (`dev`, `prod`) via
`DBT_TARGET` or `--target`.

Run from the host, with `.env` sourced from the repo root:

    cd dbt
    dbt deps
    dbt build                                           # Snowflake, the default
    WAREHOUSE_HOST=localhost dbt build --target dev     # Postgres

Model descriptions, the test suite, how dbt picks a `profiles.yml`, and the
Snowflake setup are all documented in the
[root README](../README.md#dbt-models).
