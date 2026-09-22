{{ config(materialized='view', schema='reports') }}

with candidates as (
    select *
    from {{ ref('int_weather_enriched') }}
    where local_dt between {{ dbt.current_timestamp() }} - interval '4 hours' and {{ dbt.current_timestamp() }} + interval '4 hours'
),

ranked as (
    select
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city_id
            ORDER BY ABS({{ dbt.datediff("local_dt", dbt.current_timestamp(), "second") }})
        ) as rn
    from candidates
)

select
    local_dt,
    city_id,
    city_name,
    city_country,
    main_temp,
    main_feels_like,
    main_humidity,
    wind_speed,
    wind_gust,
    clouds_all,
    rain_3h,
    snow_3h,
    weather_main,
    weather_description,
    pop,
    temp_category,
    wind_category,
    precip_type
from ranked
where rn = 1
