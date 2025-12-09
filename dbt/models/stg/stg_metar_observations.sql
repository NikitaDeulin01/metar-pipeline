{{ config(
    materialized='incremental',
    schema='analytics_stg',
    tags=['staging', 'metar'],
    unique_key='id',
    incremental_strategy='merge'
) }}

WITH raw AS (
    SELECT
        id,
        payload
    FROM public.metar_raw_json
    {% if is_incremental() %}
        WHERE (payload ->> 'observed')::timestamptz > (
            SELECT COALESCE(MAX(observed), '2000-01-01'::timestamptz)
            FROM {{ this }}
        )
    {% endif %}
)

SELECT
    id,
    payload ->> 'icao'                              AS icao,
    (payload ->> 'observed')::timestamptz           AS observed,
    payload ->> 'flight_category'                   AS flight_category,
    (payload ->> 'temperature_c')::float            AS temperature_c,
    (payload ->> 'dewpoint_c')::float               AS dewpoint_c,
    (payload ->> 'wind_dir_deg')::float             AS wind_dir_deg,
    (payload ->> 'wind_speed_kt')::float            AS wind_speed_kt,
    (payload ->> 'wind_gust_kt')::float             AS wind_gust_kt,
    (payload ->> 'visibility_m')::float             AS visibility_m,
    (payload ->> 'barometer_hpa')::float            AS barometer_hpa,
    (payload ->> 'humidity_percent')::float         AS humidity_percent,
    payload ->> 'station_name'                      AS station_name,
    payload ->> 'station_location'                  AS station_location,
    (payload ->> 'station_lon')::double precision   AS station_lon,
    (payload ->> 'station_lat')::double precision   AS station_lat,
    payload ->> 'raw_text'                          AS raw_text,
    (payload ->> 'inserted_at')::timestamptz        AS inserted_at

FROM raw
