{{ config(
    materialized='incremental',
    unique_key='id',
    schema='ods',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
) }}

with source_data as (

    select
        -- Берём только те id, которые полностью состоят из цифр
        case
            when id ~ '^[0-9]+$' then id::integer
            else null
        end as id,

        {{ dbt_utils.star(
            from=ref('stg_metar_observations'),
            except=['id']
        ) }}
    from {{ ref('stg_metar_observations') }}
    where id ~ '^[0-9]+$'
)

{% if not is_incremental() %}

    select *
    from source_data

{% else %}

    -- Инкрементальный: только новые observed
    select *
    from source_data
    where observed >= (
        select coalesce(max(observed), '1970-01-01'::timestamptz)
        from {{ this }}
    )

{% endif %}
