{{ config(materialized='incremental',
    incremental_strategy='merge',
    unique_key=['city_id', 'local_dt'],
    on_schema_change='append_new_columns') }}

select
    local_dt,
    city_country,
    city_name,
    city_id,
    main_temp,
    main_temp_min,
    main_temp_max,
    main_feels_like,
    main_humidity,
    clouds_all,
    rain_3h,
    snow_3h,
    wind_speed,
    wind_gust,
    weather_main,
    weather_description,
    weather_id,
    ingested_at
from {{ ref('stg_weather__forecast') }}

{% if is_incremental() %}
    -- Only scan batches at or after the newest one already loaded, rather than
    -- re-reading all of staging on every run.
    --
    -- `>=` and not `>`: every row from a single CSV load shares one ingested_at,
    -- so a strict comparison would skip an entire batch whose timestamp happened
    -- to equal the stored maximum. Reprocessing the boundary batch costs nothing
    -- because the unique_key config upserts rather than appends.
    --
    -- The coalesce guards the case where the table exists but is empty, where a
    -- bare max() returns null and the filter would silently match no rows.
    where ingested_at >= coalesce(
        (select max(ingested_at) from {{ this }}),
        '1900-01-01'::timestamp
    )
{% endif %}
