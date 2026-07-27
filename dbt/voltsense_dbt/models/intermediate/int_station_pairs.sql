{{ config(materialized='table') }}

-- Static table: only changes when station metadata changes.
-- Cross-joins all stations, computes Haversine distance, filters to 3 km.

{% set radius_km = 3.0 %}

with stations as (

    select
        station_id,
        station_name,
        latitude,
        longitude,
        max_capacity_kw,
        total_connectors,
        pricing_tier,
        station_type
    from {{ ref('station_metadata') }}

),

all_pairs as (

    select
        a.station_id        as focal_station_id,
        b.station_id        as neighbor_station_id,
        b.max_capacity_kw   as neighbor_capacity_kw,
        b.total_connectors  as neighbor_connectors,

        {{ haversine_km('a.latitude', 'a.longitude', 'b.latitude', 'b.longitude') }}
            as distance_km

    from stations a
    cross join stations b
    where a.station_id != b.station_id

)

select
    focal_station_id,
    neighbor_station_id,
    neighbor_capacity_kw,
    neighbor_connectors,
    round(distance_km, 3) as distance_km,
    row_number() over (
        partition by focal_station_id
        order by distance_km asc
    ) as neighbor_rank

from all_pairs
where distance_km <= {{ radius_km }}