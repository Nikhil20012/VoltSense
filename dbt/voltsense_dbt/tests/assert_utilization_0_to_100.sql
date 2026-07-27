select station_id, timestamp_15min, utilization_pct
from {{ ref('fact_station_utilization_15min') }}
where utilization_pct < 0 or utilization_pct > 100