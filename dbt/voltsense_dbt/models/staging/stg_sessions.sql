{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge'
    )
}}

with raw as (

    select
        record_content:event_type::string             as event_type,
        record_content:station_id::string             as station_id,
        record_content:charger_id::string             as charger_id,
        record_content:connector_type::string         as connector_type,
        record_content:timestamp::timestamp_ntz       as event_timestamp,
        record_content:session_id::string             as session_id,
        record_content:energy_kwh_cumulative::float   as energy_kwh_cumulative,
        record_content:power_kw::float                as power_kw,
        record_content:soc_pct::int                   as soc_pct,
        record_content:user_id_hash::string           as user_id_hash,
        record_metadata:CreateTime::timestamp_ntz     as _kafka_timestamp,
        record_metadata:offset::int                   as _kafka_offset,
        record_metadata:partition::int                as _kafka_partition
    from {{ source('raw', 'raw_charger_sessions') }}

    {% if is_incremental() %}
    where record_metadata:CreateTime::timestamp_ntz > (
        select max(_kafka_timestamp) from {{ this }}
    )
    {% endif %}

),

deduped as (

    select
        *,
        row_number() over (
            partition by station_id, session_id, event_timestamp, event_type
            order by _kafka_offset desc
        ) as _row_num,

        {{ dbt_utils.generate_surrogate_key([
            'station_id', 'session_id', 'event_timestamp', 'event_type'
        ]) }} as event_id

    from raw

)

select
    event_id,
    event_type,
    station_id,
    charger_id,
    connector_type,
    event_timestamp,
    session_id,
    energy_kwh_cumulative,
    power_kw,
    soc_pct,
    user_id_hash,
    _kafka_timestamp,
    _kafka_offset,
    _kafka_partition
from deduped
where _row_num = 1