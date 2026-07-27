{{
    config(
        materialized='incremental',
        unique_key=['station_id', 'timestamp_15min'],
        incremental_strategy='merge'
    )
}}

with utilization as (

    select
        station_id,
        timestamp_15min,
        utilization_pct,
        total_power_kw,
        active_sessions,
        available_connectors
    from {{ ref('int_station_utilization_15min') }}
    {% if is_incremental() %}
    where timestamp_15min > (select max(timestamp_15min) from {{ this }})
    {% endif %}

),

pairs as (
    select * from {{ ref('int_station_pairs') }}
),

neighbor_demand as (

    select
        p.focal_station_id                   as station_id,
        u_focal.timestamp_15min,
        u_focal.utilization_pct              as own_utilization_pct,
        u_focal.total_power_kw               as own_power_kw,
        p.neighbor_station_id,
        p.distance_km,
        p.neighbor_capacity_kw,
        u_neighbor.utilization_pct           as neighbor_util_pct,
        u_neighbor.available_connectors      as neighbor_avail_connectors,
        1.0 / (p.distance_km + 0.1)         as inv_distance_weight

    from pairs p
    inner join utilization u_focal
        on u_focal.station_id = p.focal_station_id
    inner join utilization u_neighbor
        on u_neighbor.station_id = p.neighbor_station_id
        and u_neighbor.timestamp_15min = u_focal.timestamp_15min

),

spatial_agg as (

    select
        station_id,
        timestamp_15min,
        own_utilization_pct,
        own_power_kw,

        avg(neighbor_util_pct) as nearby_avg_utilization,

        sum(neighbor_util_pct * inv_distance_weight)
            / nullif(sum(inv_distance_weight), 0)
            as nearby_weighted_avg_utilization,

        count(distinct neighbor_station_id) as nearby_station_count,

        sum(neighbor_avail_connectors * neighbor_capacity_kw
            * (1.0 - neighbor_util_pct / 100.0))
            as nearby_available_capacity_kw,

        max(neighbor_util_pct) as nearby_max_utilization,

        sum(case when neighbor_util_pct >= 80 then 1 else 0 end)::float
            / nullif(count(distinct neighbor_station_id), 0)
            as cluster_saturation_pct,

        min(distance_km) as nearest_neighbor_km

    from neighbor_demand
    group by station_id, timestamp_15min, own_utilization_pct, own_power_kw

)

select
    s.station_id,
    s.timestamp_15min,
    s.own_utilization_pct,
    s.own_power_kw,
    coalesce(s.nearby_avg_utilization, 0)             as nearby_avg_utilization,
    coalesce(s.nearby_weighted_avg_utilization, 0)    as nearby_weighted_avg_utilization,
    coalesce(s.nearby_station_count, 0)               as nearby_station_count,
    coalesce(s.nearby_available_capacity_kw, 0)       as nearby_available_capacity_kw,
    coalesce(s.nearby_max_utilization, 0)             as nearby_max_utilization,
    coalesce(s.cluster_saturation_pct, 0)             as cluster_saturation_pct,
    coalesce(s.nearest_neighbor_km, 999.9)            as nearest_neighbor_km,
    case
        when s.nearby_station_count = 0 then 1.0
        else 1.0 / (1.0 + s.nearby_station_count
                         * (1.0 / nullif(s.nearest_neighbor_km, 0)))
    end as isolation_score

from spatial_agg s