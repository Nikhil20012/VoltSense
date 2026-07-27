{{
    config(
        materialized='incremental',
        unique_key=['station_id', 'timestamp_15min'],
        incremental_strategy='merge'
    )
}}

with sessions as (

    select * from {{ ref('stg_sessions') }}
    where event_type in ('SESSION_START', 'SESSION_END', 'HEARTBEAT')
    {% if is_incremental() %}
      and event_timestamp > dateadd(hour, -2, (select max(timestamp_15min) from {{ this }}))
    {% endif %}

),

bucketed as (

    select
        station_id,
        time_slice(event_timestamp, 15, 'MINUTE') as timestamp_15min,
        session_id,
        power_kw,
        event_type
    from sessions

),

station_meta as (

    select station_id, max_capacity_kw, total_connectors
    from {{ ref('station_metadata') }}

),

aggregated as (

    select
        b.station_id,
        b.timestamp_15min,
        count(distinct b.session_id)    as active_sessions,
        avg(b.power_kw)                 as avg_power_kw,
        max(b.power_kw)                 as peak_power_kw,
        sum(b.power_kw)                 as total_power_kw
    from bucketed b
    where b.event_type in ('HEARTBEAT', 'SESSION_START')
    group by b.station_id, b.timestamp_15min

)

select
    a.station_id,
    a.timestamp_15min,
    a.active_sessions,
    round(a.avg_power_kw, 2)     as avg_power_kw,
    round(a.peak_power_kw, 2)    as peak_power_kw,
    round(a.total_power_kw, 2)   as total_power_kw,
    greatest(sm.total_connectors - a.active_sessions, 0) as available_connectors,
    least(100, round(a.total_power_kw / nullif(sm.max_capacity_kw, 0) * 100, 1)) as utilization_pct,
    hour(a.timestamp_15min)           as hour_of_day,
    dayofweek(a.timestamp_15min)      as day_of_week,
    case
        when dayofweek(a.timestamp_15min) in (0, 6) then true
        else false
    end as is_weekend

from aggregated a
left join station_meta sm on a.station_id = sm.station_id