{% snapshot snp_city %}
{{ config(
    target_schema='snapshots',
    unique_key='city_id',
    strategy='check',
    check_cols=['city_name', 'city_population']
) }}
select distinct city_id, city_name, city_population
from {{ ref('stg_weather__forecast') }}
{% endsnapshot %}