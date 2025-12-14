{{ config(
    materialized='incremental',
    schema='analytics_ods',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
) }}

with source_data as (
    select
        id,  -- Mongo ObjectId

        {{ dbt_utils.star(
            from=ref('stg_metar_observations'),
            except=['id']
        ) }}
    from {{ ref('stg_metar_observations') }}
)

{% if not is_incremental() %}

    select *
    from source_data

{% else %}

    -- Инкрементальный: добавляем только новые observed
    select *
    from source_data
    where observed > (
        select coalesce(max(observed), '1970-01-01'::timestamptz)
        from {{ this }}
    )

{% endif %}
