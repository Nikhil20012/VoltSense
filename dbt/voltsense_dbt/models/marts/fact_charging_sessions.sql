{{ config(materialized='incremental', unique_key='session_id', incremental_strategy='merge') }}

with starts as (

    select
        session_id,
        station_id,
        charger_id,
        connector_type,
        event_timestamp as session_start,
        power_kw as start_power_kw,
        soc_pct as start_soc_pct,
        user_id_hash
    from {{ ref('stg_sessions') }}
    where event_type = 'SESSION_START'
    {% if is_incremental() %}
      and event_timestamp > dateadd(day, -2, (select max(session_start) from {{ this }}))
    {% endif %}

),

ends as (

    select
        session_id,
        station_id,
        event_timestamp as session_end,
        energy_kwh_cumulative as final_energy_kwh,
        soc_pct as end_soc_pct
    from {{ ref('stg_sessions') }}
    where event_type = 'SESSION_END'

),

station as (

    select station_id, pricing_tier, station_type, grid_region, utility_zone
    from {{ ref('station_metadata') }}

),

weather as (
    select * from {{ ref('stg_weather') }}
),

pricing as (
    select * from {{ ref('stg_grid_pricing') }}
)

select
    s.session_id,
    s.station_id,
    s.charger_id,
    s.connector_type,
    s.session_start,
    e.session_end,
    s.user_id_hash,
    datediff(minute, s.session_start, e.session_end) as duration_minutes,
    coalesce(e.final_energy_kwh, 0) as energy_kwh,
    s.start_soc_pct,
    e.end_soc_pct,
    st.pricing_tier,
    st.station_type,
    w.temperature_c,
    w.precipitation_mm,
    p.price_per_kwh_usd,
    p.is_peak_hour,
    coalesce(e.final_energy_kwh, 0) * coalesce(p.price_per_kwh_usd, 0) as estimated_revenue

from starts s
inner join ends e on s.session_id = e.session_id and s.station_id = e.station_id
left join station st on s.station_id = st.station_id
left join weather w
    on w.region_id = st.grid_region
    and w.reading_timestamp = time_slice(s.session_start, 15, 'MINUTE')
left join pricing p
    on p.zone_id = st.utility_zone
    and p.pricing_timestamp = time_slice(s.session_start, 5, 'MINUTE')
where datediff(minute, s.session_start, e.session_end) > 0
  and datediff(minute, s.session_start, e.session_end) < 1440