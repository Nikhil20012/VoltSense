select
    a.focal_station_id,
    a.neighbor_station_id,
    a.distance_km as dist_a_to_b,
    b.distance_km as dist_b_to_a,
    abs(a.distance_km - b.distance_km) as difference_km
from {{ ref('int_station_pairs') }} a
inner join {{ ref('int_station_pairs') }} b
    on a.focal_station_id = b.neighbor_station_id
    and a.neighbor_station_id = b.focal_station_id
where abs(a.distance_km - b.distance_km) > 0.01