{{ config(materialized='incremental', unique_key=['station_id', 'timestamp_15min'], incremental_strategy='merge') }}

with utilization as (

    select * from {{ ref('int_station_utilization_15min') }}
    {% if is_incremental() %}
    where timestamp_15min > (select max(timestamp_15min) from {{ this }})
    {% endif %}

),

spatial as (
    select * from {{ ref('int_spatial_demand_features') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

pricing as (
    select * from {{ ref('stg_grid_pricing') }}
),

station as (
    select station_id, grid_region, utility_zone
    from {{ ref('station_metadata') }}
)

select
    u.station_id,
    u.timestamp_15min,
    u.active_sessions,
    u.avg_power_kw,
    u.peak_power_kw,
    u.total_power_kw,
    u.available_connectors,
    u.utilization_pct,
    u.hour_of_day,
    u.day_of_week,
    u.is_weekend,

    coalesce(sp.nearby_avg_utilization, 0)           as nearby_avg_utilization,
    coalesce(sp.nearby_weighted_avg_utilization, 0)  as nearby_weighted_avg_utilization,
    coalesce(sp.nearby_station_count, 0)             as nearby_station_count,
    coalesce(sp.nearby_available_capacity_kw, 0)     as nearby_available_capacity_kw,
    coalesce(sp.nearby_max_utilization, 0)           as nearby_max_utilization,
    coalesce(sp.cluster_saturation_pct, 0)           as cluster_saturation_pct,
    coalesce(sp.nearest_neighbor_km, 999.9)          as nearest_neighbor_km,
    coalesce(sp.isolation_score, 1.0)                as isolation_score,

    w.temperature_c,
    w.precipitation_mm,
    w.wind_speed_kmh,
    p.price_per_kwh_usd,
    p.is_peak_hour

from utilization u
left join spatial sp
    on u.station_id = sp.station_id and u.timestamp_15min = sp.timestamp_15min
left join station s on u.station_id = s.station_id
left join weather w
    on w.region_id = s.grid_region
    and w.reading_timestamp = u.timestamp_15min
left join pricing p
    on p.zone_id = s.utility_zone
    and p.pricing_timestamp = u.timestamp_15min