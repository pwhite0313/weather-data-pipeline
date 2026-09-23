select city_id, local_dt, main_temp, main_temp_min, main_temp_max
from {{ ref('fct_weather_forecast') }}
where main_temp < main_temp_min
   or main_temp > main_temp_max