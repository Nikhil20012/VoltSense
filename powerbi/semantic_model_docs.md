# Power BI Semantic Model

Connect via DirectQuery to Snowflake VOLTSENSE warehouse, DEV_MARTS schema.

## Tables

Dimensions: dim_station, dim_date_time, dim_geography
Facts: fact_charging_sessions, fact_station_utilization_15min, fact_anomaly_flags

## Relationships

dim_station.station_id --> fact_charging_sessions.station_id (1:M)
dim_station.station_id --> fact_station_utilization_15min.station_id (1:M)
dim_station.station_id --> fact_anomaly_flags.station_id (1:M)

## DAX Measures

Avg Utilization % = AVERAGE(fact_station_utilization_15min[utilization_pct])

Revenue Per Charger Day = DIVIDE(
    SUM(fact_charging_sessions[estimated_revenue]),
    DISTINCTCOUNT(fact_charging_sessions[charger_id])
        * DISTINCTCOUNT(dim_date_time[calendar_date])
)

Peak Hour Utilization % = CALCULATE(
    [Avg Utilization %],
    FILTER(dim_date_time, dim_date_time[hour_of_day] >= 17
                       && dim_date_time[hour_of_day] <= 21)
)

Active Anomalies = COUNTROWS(
    FILTER(fact_anomaly_flags, fact_anomaly_flags[is_resolved] = FALSE())
)

## Pages

1. Network Overview - KPIs, station map, utilization trend
2. Forecast vs Actual - model accuracy by station
3. Revenue and Pricing - revenue by station/tier, margin analysis
4. Station Scorecard (drillthrough) - hour x day heatmap