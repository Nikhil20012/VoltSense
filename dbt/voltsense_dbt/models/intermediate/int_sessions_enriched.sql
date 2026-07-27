{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge'
    )
}}

with sessions as (

    select * from {{ ref('stg_sessions') }}
    {% if is_incremental() %}
    where event_timestamp > dateadd(hour, -2, (select max(event_timestamp) from {{ this }}))
    {% endif %}

),

station as (

    select
        station_id,
        grid_region,
        utility_zone,
        max_capacity_kw,
        total_connectors,
        station_type,
        pricing_tier
    from {{ ref('station_metadata') }}

),

weather as (
    select * from {{ ref('stg_weather') }}
),

pricing as (
    select * from {{ ref('stg_grid_pricing') }}
)

select
    s.event_id,
    s.event_type,
    s.station_id,
    s.charger_id,
    s.connector_type,
    s.event_timestamp,
    s.session_id,
    s.energy_kwh_cumulative,
    s.power_kw,
    s.soc_pct,
    s.user_id_hash,

    st.station_type,
    st.pricing_tier,
    st.max_capacity_kw,
    st.total_connectors,

    w.temperature_c,
    w.precipitation_mm,
    w.wind_speed_kmh,

    p.price_per_kwh_usd,
    p.is_peak_hour

from sessions s
left join station st on s.station_id = st.station_id
left join weather w
    on w.region_id = st.grid_region
    and w.reading_timestamp = time_slice(s.event_timestamp, 15, 'MINUTE')
left join pricing p
    on p.zone_id = st.utility_zone
    and p.pricing_timestamp = time_slice(s.event_timestamp, 5, 'MINUTE')