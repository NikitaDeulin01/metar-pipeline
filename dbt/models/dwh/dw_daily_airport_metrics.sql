{{ config(
    materialized = 'incremental',
    unique_key   = 'icao_date',
    schema       = env_var('DBT_SCHEMA', 'dwh')
) }}

WITH base AS (
    SELECT
        icao,
        date_trunc('day', observed)        AS observed_date,
        AVG(temperature_c)                 AS avg_temp_c,
        MAX(wind_speed_kt)                 AS max_wind_kt,
        MIN(visibility_m)                  AS min_visibility_m,
        COUNT(*)                           AS observations_count
    FROM {{ ref('ods_metar_latest') }}
    GROUP BY
        icao,
        date_trunc('day', observed)
)

SELECT
    concat(icao, '_', to_char(observed_date, 'YYYYMMDD')) AS icao_date,
    icao,
    observed_date,
    avg_temp_c,
    max_wind_kt,
    min_visibility_m,
    observations_count
FROM base

{% if is_incremental() %}
WHERE observed_date >= (
    SELECT
        COALESCE(MAX(observed_date), '1970-01-01'::date)
    FROM {{ this }}
)
{% endif %}
