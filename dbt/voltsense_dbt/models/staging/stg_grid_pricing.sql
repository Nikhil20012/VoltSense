{{
    config(
        materialized='incremental',
        unique_key='pricing_id',
        incremental_strategy='merge'
    )
}}

with raw as (

    select
        record_content:zone_id::string                as zone_id,
        record_content:timestamp::timestamp_ntz       as pricing_timestamp,
        record_content:price_per_kwh_usd::float       as price_per_kwh_usd,
        record_content:is_peak_hour::boolean          as is_peak_hour,
        record_metadata:CreateTime::timestamp_ntz     as _kafka_timestamp
    from {{ source('raw', 'raw_grid_pricing') }}

    {% if is_incremental() %}
    where record_metadata:CreateTime::timestamp_ntz > (
        select max(_kafka_timestamp) from {{ this }}
    )
    {% endif %}

),

deduped as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key(['zone_id', 'pricing_timestamp']) }}
            as pricing_id,
        row_number() over (
            partition by zone_id, pricing_timestamp
            order by _kafka_timestamp desc
        ) as _row_num
    from raw

)

select
    pricing_id,
    zone_id,
    pricing_timestamp,
    price_per_kwh_usd,
    coalesce(is_peak_hour, false) as is_peak_hour,
    _kafka_timestamp
from deduped
where _row_num = 1