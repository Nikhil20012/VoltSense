{{ config(materialized='incremental', unique_key='anomaly_id', incremental_strategy='merge') }}

with utilization as (

    select
        *,
        lag(utilization_pct, 1) over (
            partition by station_id order by timestamp_15min
        ) as prev_utilization_pct
    from {{ ref('fact_station_utilization_15min') }}
    {% if is_incremental() %}
    where timestamp_15min > (select max(detected_at) from {{ this }})
    {% endif %}

),

station as (
    select station_id, max_capacity_kw
    from {{ ref('station_metadata') }}
),

ghost_sessions as (
    select station_id, timestamp_15min as detected_at,
        'GHOST_SESSION' as anomaly_type, 'WARNING' as severity,
        'Active sessions but zero power draw' as description
    from utilization
    where active_sessions > 0 and total_power_kw = 0
),

capacity_breach as (
    select u.station_id, u.timestamp_15min as detected_at,
        'CAPACITY_BREACH' as anomaly_type, 'CRITICAL' as severity,
        'Power exceeded rated capacity' as description
    from utilization u
    join station s on u.station_id = s.station_id
    where u.total_power_kw > s.max_capacity_kw * 1.1
),

demand_spike as (
    select station_id, timestamp_15min as detected_at,
        'DEMAND_SPIKE' as anomaly_type, 'WARNING' as severity,
        'Utilization jumped 40+ points in one interval' as description
    from utilization
    where prev_utilization_pct is not null
      and (utilization_pct - prev_utilization_pct) > 40
),

cluster_overload as (
    select station_id, timestamp_15min as detected_at,
        'CLUSTER_OVERLOAD' as anomaly_type, 'CRITICAL' as severity,
        'Station and cluster both above 90%' as description
    from utilization
    where utilization_pct > 90
      and cluster_saturation_pct > 0.8
      and nearby_station_count > 0
),

all_anomalies as (
    select * from ghost_sessions
    union all select * from capacity_breach
    union all select * from demand_spike
    union all select * from cluster_overload
)

select
    {{ dbt_utils.generate_surrogate_key(['station_id', 'detected_at', 'anomaly_type']) }}
        as anomaly_id,
    station_id,
    detected_at,
    anomaly_type,
    severity,
    description,
    false as is_resolved
from all_anomalies