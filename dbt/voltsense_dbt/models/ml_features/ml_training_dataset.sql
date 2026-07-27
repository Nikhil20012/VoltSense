{{ config(materialized='table') }}

with util as (

    select * from {{ ref('fact_station_utilization_15min') }}

),

station as (

    select station_id, max_capacity_kw, total_connectors, station_type, pricing_tier
    from {{ ref('station_metadata') }}

),

with_lags as (

    select
        u.*,

        lag(u.utilization_pct, 1) over (
            partition by u.station_id order by u.timestamp_15min
        ) as util_lag_15min,

        lag(u.utilization_pct, 4) over (
            partition by u.station_id order by u.timestamp_15min
        ) as util_lag_1h,

        lag(u.utilization_pct, 96) over (
            partition by u.station_id order by u.timestamp_15min
        ) as util_lag_1d,

        lag(u.utilization_pct, 672) over (
            partition by u.station_id order by u.timestamp_15min
        ) as util_lag_1w,

        avg(u.utilization_pct) over (
            partition by u.station_id
            order by u.timestamp_15min
            rows between 96 preceding and 1 preceding
        ) as rolling_24h_avg_util,

        avg(u.utilization_pct) over (
            partition by u.station_id
            order by u.timestamp_15min
            rows between 672 preceding and 1 preceding
        ) as rolling_7d_avg_util,

        stddev(u.utilization_pct) over (
            partition by u.station_id
            order by u.timestamp_15min
            rows between 96 preceding and 1 preceding
        ) as rolling_24h_std_util

    from util u

)

select
    wl.station_id,
    wl.timestamp_15min,

    wl.utilization_pct as target_utilization_pct,

    wl.util_lag_15min,
    wl.util_lag_1h,
    wl.util_lag_1d,
    wl.util_lag_1w,
    wl.rolling_24h_avg_util,
    wl.rolling_7d_avg_util,
    wl.rolling_24h_std_util,

    wl.nearby_avg_utilization,
    wl.nearby_weighted_avg_utilization,
    wl.nearby_station_count,
    wl.nearby_available_capacity_kw,
    wl.nearby_max_utilization,
    wl.cluster_saturation_pct,
    wl.nearest_neighbor_km,
    wl.isolation_score,

    wl.hour_of_day,
    wl.day_of_week,
    wl.is_weekend,

    wl.temperature_c,
    wl.precipitation_mm,
    wl.wind_speed_kmh,

    s.max_capacity_kw,
    s.total_connectors,
    s.station_type,
    s.pricing_tier,

    wl.price_per_kwh_usd

from with_lags wl
left join station s on wl.station_id = s.station_id
where wl.util_lag_1h is not null