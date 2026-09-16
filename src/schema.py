"""Canonical column contract for the weather forecast dataset.

The OpenWeatherMap forecast payload omits keys entirely rather than sending
nulls: `snow.3h` is absent unless snow is forecast, `rain.3h` unless rain is,
and fields like `main.sea_level` and `main.dew_point` have come and gone with
API revisions. `pd.json_normalize` faithfully reproduces that, so the shape of
a CSV depends on the weather on the day it was pulled.

That mattered because `df.to_sql(..., if_exists="append")` CREATES the raw
table from the first file it sees, permanently freezing the warehouse schema to
whatever conditions happened to prevail on the first run. A clone started in
summer produced a raw table with no `snow_3h` column, and every downstream dbt
model that referenced it failed on `column "snow_3h" does not exist`.

Conforming every frame to this list makes the CSVs, and therefore the raw
table, structurally identical regardless of the weather.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

FORECAST_COLUMNS = [
    "dt",
    "visibility",
    "pop",
    "dt_txt",
    "main_temp",
    "main_feels_like",
    "main_temp_min",
    "main_temp_max",
    "main_pressure",
    "main_sea_level",
    "main_grnd_level",
    "main_humidity",
    "main_temp_kf",
    "main_dew_point",
    "clouds_all",
    "wind_speed",
    "wind_deg",
    "wind_gust",
    "sys_pod",
    "rain_3h",
    "snow_3h",
    "city_id",
    "city_name",
    "city_country",
    "city_population",
    "city_timezone",
    "city_sunrise",
    "city_sunset",
    "city_coord_lat",
    "city_coord_lon",
    "weather_id",
    "weather_main",
    "weather_description",
    "weather_icon",
]


# Pandas infers dtypes per file, so a column is int64 in a payload where every
# city reported it and float64 in one where a single city did not. `to_sql` then
# creates the raw column as BIGINT from the first file and later rejects the
# float rendering of the same value ("10000.0" is not valid bigint input).
# Pinning the dtype makes that inference deterministic.
#
# Latitude and longitude are kept as floats here so the raw layer stays faithful
# to the API. Note that stg_weather__forecast currently casts both to ::bigint,
# which truncates them to whole degrees.
INTEGER_COLUMNS = [
    "dt",
    "visibility",
    "main_pressure",
    "main_sea_level",
    "main_grnd_level",
    "main_humidity",
    "clouds_all",
    "wind_deg",
    "city_id",
    "city_population",
    "city_timezone",
    "city_sunrise",
    "city_sunset",
    "weather_id",
]

FLOAT_COLUMNS = [
    "pop",
    "main_temp",
    "main_feels_like",
    "main_temp_min",
    "main_temp_max",
    "main_temp_kf",
    "main_dew_point",
    "wind_speed",
    "wind_gust",
    "rain_3h",
    "snow_3h",
    "city_coord_lat",
    "city_coord_lon",
]


def _pin_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce numeric columns to pandas nullable types.

    Nullable Int64/Float64 rather than numpy int64/float64 so that a column can
    hold nulls without the whole column being promoted to float, which is the
    promotion that produced "10000.0" for an integer field.
    """
    for col in INTEGER_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    for col in FLOAT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

    return df


def conform_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Return df with exactly FORECAST_COLUMNS, in that order.

    Columns the API did not send are added as nulls. Columns the API sent that
    are not part of the contract are dropped, with a warning naming them so a
    genuinely new upstream field is visible rather than silent.
    """
    unexpected = [c for c in df.columns if c not in FORECAST_COLUMNS]
    if unexpected:
        logger.warning(
            "Dropping %d column(s) outside the canonical schema: %s. "
            "Add them to FORECAST_COLUMNS to start capturing this data.",
            len(unexpected),
            unexpected,
        )

    missing = [c for c in FORECAST_COLUMNS if c not in df.columns]
    if missing:
        logger.info(
            "Filling %d column(s) absent from this payload with nulls: %s",
            len(missing),
            missing,
        )

    return _pin_dtypes(df.reindex(columns=FORECAST_COLUMNS))
