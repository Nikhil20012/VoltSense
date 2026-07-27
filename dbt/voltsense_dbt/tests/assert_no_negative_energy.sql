select session_id, station_id, energy_kwh
from {{ ref('fact_charging_sessions') }}
where energy_kwh < 0