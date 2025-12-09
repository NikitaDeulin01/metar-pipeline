{{ config(
    materialized          = 'incremental',
    schema                = 'int',
    unique_key            = 'icao',
    incremental_strategy  = 'merge',
    tags                  = ['intermediate', 'metar']
) }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY icao
            ORDER BY observed DESC
        ) AS rn
    FROM {{ ref('stg_metar_observations') }}
)

SELECT
    id,
    icao,
    observed,
    visibility_m,
    temperature_c,
    dewpoint_c,
    wind_dir_deg,
    wind_speed_kt,
    wind_gust_kt,
    barometer_hpa,
    humidity_percent,
    station_name,
    station_location,
    station_lon,
    station_lat,
    raw_text,
    inserted_at
FROM ranked
WHERE rn = 1

{% if is_incremental() %}
  AND observed > (
      SELECT max(observed)
      FROM {{ this }}
  )
{% endif %}
