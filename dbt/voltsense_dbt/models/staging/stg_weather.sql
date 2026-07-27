{{
    config(
        materialized='incremental',
        unique_key='weather_reading_id',
        incremental_strategy='merge'
    )
}}

with raw as (

    select
        record_content:region_id::string              as region_id,
        record_content:timestamp::timestamp_ntz       as reading_timestamp,
        record_content:temperature_c::float           as temperature_c,
        record_content:precipitation_mm::float        as precipitation_mm,
        record_content:wind_speed_kmh::float          as wind_speed_kmh,
        record_content:cloud_cover_pct::int           as cloud_cover_pct,
        record_metadata:CreateTime::timestamp_ntz     as _kafka_timestamp
    from {{ source('raw', 'raw_weather_readings') }}

    {% if is_incremental() %}
    where record_metadata:CreateTime::timestamp_ntz > (
        select max(_kafka_timestamp) from {{ this }}
    )
    {% endif %}

),

deduped as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key(['region_id', 'reading_timestamp']) }}
            as weather_reading_id,
        row_number() over (
            partition by region_id, reading_timestamp
            order by _kafka_timestamp desc
        ) as _row_num
    from raw

)

select
    weather_reading_id,
    region_id,
    reading_timestamp,
    temperature_c,
    coalesce(precipitation_mm, 0) as precipitation_mm,
    coalesce(wind_speed_kmh, 0)   as wind_speed_kmh,
    coalesce(cloud_cover_pct, 0)  as cloud_cover_pct,
    _kafka_timestamp
from deduped
where _row_num = 1