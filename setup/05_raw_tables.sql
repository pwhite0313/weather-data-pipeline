USE ROLE WEATHER_ENGINEER;
USE SCHEMA WEATHER_DB.RAW;


CREATE TABLE IF NOT EXISTS RAW.WEATHER_FORECAST (
    dt NUMBER,
    visibility NUMBER,
    pop FLOAT,
    dt_txt VARCHAR,
    main_temp FLOAT,
    main_feels_like FLOAT,
    main_temp_min FLOAT,
    main_temp_max FLOAT,
    main_pressure NUMBER,
    main_sea_level NUMBER,
    main_grnd_level NUMBER,
    main_humidity NUMBER,
    main_temp_kf FLOAT,
    main_dew_point FLOAT,
    clouds_all NUMBER,
    wind_speed FLOAT,
    wind_deg NUMBER,
    wind_gust FLOAT,
    sys_pod VARCHAR,
    rain_3h FLOAT,
    snow_3h FLOAT,
    city_id NUMBER,
    city_name VARCHAR,
    city_country VARCHAR,
    city_population NUMBER,
    city_timezone NUMBER,
    city_sunrise NUMBER,
    city_sunset NUMBER,
    city_coord_lat FLOAT,
    city_coord_lon FLOAT,
    weather_id NUMBER,
    weather_main VARCHAR,
    weather_description VARCHAR,
    weather_icon VARCHAR,
    source_file_name VARCHAR,
    source_file_ts TIMESTAMP_NTZ,
    ingested_at TIMESTAMP_NTZ,
    dag_run_id VARCHAR
);

DESC TABLE RAW.WEATHER_FORECAST;

USE ROLE WEATHER_ENGINEER;
USE SCHEMA WEATHER_DB.RAW;


CREATE TABLE IF NOT EXISTS WEATHER_DB.RAW.FORCAST_JSON(
    payload VARIANT,
    loaded_at TIMESTAMP_NTZ,
    source_file VARCHAR
);

ALTER TABLE WEATHER_DB.RAW.FORCAST_JSON RENAME TO WEATHER_DB.RAW.FORECAST_JSON;


-- CREATE TABLE IF NOT EXISTS RAW.WEATHER_JSON (
--     dt NUMBER(38,0),
--     visibility NUMBER(38,0),
--     pop NUMBER(38,0),
--     dt_txt TIMESTAMP_NTZ,
--     main_temp NUMBER(38,2),
--     main_feels_like NUMBER(38,2),
--     main_temp_min NUMBER(38,2),
--     main_temp_max NUMBER(38,2),
--     main_pressure NUMBER(38,0),
--     main_sea_level NUMBER(38,0),
--     main_grnd_level NUMBER(38,0),
--     main_humidity NUMBER(38,0),
--     main_temp_kf NUMBER(38,2),
--     clouds_all NUMBER(38,0),
--     wind_speed NUMBER(38,2),
--     wind_deg NUMBER(38,0),
--     wind_gust NUMBER(38,2),
--     sys_pod VARCHAR,
--     rain_3h NUMBER(38,2),
--     snow_3h NUMBER(38,2),
--     city_id NUMBER(38,0),
--     city_name VARCHAR,
--     city_country VARCHAR,
--     city_population NUMBER(38,0),
--     city_timezone NUMBER(38,0),
--     city_sunrise TIMESTAMP_NTZ,
--     city_sunset TIMESTAMP_NTZ,
--     city_coord_lat NUMBER(38,2),
--     city_coord_lon NUMBER(38,2),
--     weather_id NUMBER(38,0),
--     weather_main VARCHAR,
--     weather_description VARCHAR,
--     weather_icon VARCHAR,
--     ingested_at TIMESTAMP_NTZ
-- );