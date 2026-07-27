select session_id, station_id, duration_minutes
from {{ ref('fact_charging_sessions') }}
where duration_minutes > 1440 or duration_minutes <= 0