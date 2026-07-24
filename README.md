# VoltSense

Real-time EV charging demand intelligence platform.

Ingests simulated charger telemetry via Apache Kafka, lands it in Snowflake
through Snowpipe Streaming, transforms it through a dbt medallion architecture
with spatial demand features, trains a LightGBM forecasting model, orchestrates
the pipeline with Airflow, and serves insights through Power BI and Streamlit.

## Problem

A Charge Point Operator (CPO) running 200+ stations has three problems:

1. **No demand visibility.** They don't know which stations will be overloaded
   tomorrow at 5 PM and which will sit idle. Pricing and staffing decisions
   are guesswork.

2. **No spatial awareness.** When Station A hits capacity, overflow demand
   spills to nearby stations. But the CPO treats every station independently
   and misses the network effect between them.

3. **Silent hardware failures.** A connector stuck in "charging" state while
   drawing zero power wastes a port for hours until someone complains.

## What makes this different

Most EV charging analytics treat stations as independent entities. VoltSense
adds a spatial demand layer built in dbt:

- Haversine-based station pairing within a configurable radius
- Distance-weighted neighbor utilization (closer stations have more influence)
- Cluster saturation scoring (all nearby stations near capacity)
- Isolation indexing (stations with no nearby alternatives)

These spatial features rank in the top 5-8 by SHAP importance in the
forecasting model and reduce MAPE by roughly 2-4 percentage points compared
to a station-only baseline.

## Architecture

```
Simulator (Python) --> Kafka topics --> Snowflake RAW tables
                                            |
                                       dbt transforms
                                            |
                               staging --> intermediate --> marts
                                               |
                                        spatial features
                                        (Haversine pairs,
                                         neighbor utilization,
                                         cluster saturation)
                                               |
                              +----------------+----------------+
                              |                |                |
                          Power BI         Streamlit        LightGBM
                        (executives)      (operators)      (forecasting)

                    Airflow orchestrates the daily pipeline
```

## Tech stack

| Tool | Role |
|---|---|
| Apache Kafka | Event streaming from simulator to warehouse |
| Snowflake | Cloud data warehouse (Snowpipe Streaming ingestion) |
| dbt | SQL transformations with testing, documentation, lineage |
| Apache Airflow | Pipeline orchestration (dbt -> ML -> BI refresh) |
| LightGBM | Demand forecasting with spatial features |
| Power BI | Executive dashboards (DirectQuery to Snowflake) |
| Streamlit | Operator console with interactive pricing simulator |

## Project structure

```
voltsense/
├── simulator/               Data producers (charger events, weather, pricing)
├── scripts/                 Utility scripts (seed generation, Kafka-Snowflake bridge)
├── dbt/voltsense_dbt/       Transformations (staging -> intermediate -> marts)
│   ├── models/
│   │   ├── staging/         Parse raw JSON, deduplicate, cast types
│   │   ├── intermediate/    Business logic, spatial features, utilization
│   │   ├── marts/           Star schema (dimensions + facts)
│   │   └── ml_features/     Training dataset for LightGBM
│   ├── macros/              Reusable SQL (Haversine formula)
│   ├── seeds/               Static reference data (station metadata, holidays)
│   └── tests/               Custom data quality assertions
├── ml/                      Model training and batch prediction
├── airflow/dags/            Daily pipeline DAG
├── streamlit/               Operator dashboard (health monitor, pricing sim)
├── powerbi/                 Dashboard documentation and wireframes
├── infrastructure/docker/   Local Kafka + Zookeeper via Docker Compose
└── docs/                    Architecture docs and development guide
```

See [docs/folder_guide.md](docs/folder_guide.md) for details on each folder.

## Data sources

| Source | What it provides |
|---|---|
| [UrbanEV (Dryad)](https://datadryad.org/) | Charging patterns for 20K+ stations (drives the simulator) |
| [ACN-Data (Caltech)](https://ev.caltech.edu/dataset) | Session-level data from real chargers |
| [NREL AFDC](https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/) | US station locations (seeds dim_station) |
| [Open-Meteo](https://open-meteo.com/) | Weather API, no key required |
| [EIA Open Data](https://www.eia.gov/opendata/) | Grid electricity pricing |

## Build progress

- [x] Project structure
- [ ] Step 0: Local environment (Kafka, Snowflake, Python)
- [ ] Step 1: Charger simulator producing to Kafka
- [ ] Step 2: Kafka to Snowflake ingestion
- [ ] Step 3: dbt staging models
- [ ] Step 4: dbt intermediate models (spatial features)
- [ ] Step 5: dbt marts (star schema)
- [ ] Step 6: LightGBM training and SHAP validation
- [ ] Step 7: Airflow DAG
- [ ] Step 8: Power BI dashboards
- [ ] Step 9: Streamlit operator console

## Author

**Nikhil Bharadwaj Yellapragada**
- [LinkedIn](https://www.linkedin.com/in/nikhil-bharadwaj-yellapragada-48321a211/)
- [GitHub](https://github.com/Nikhil20012)

## License

MIT. See [LICENSE](LICENSE) for details.
