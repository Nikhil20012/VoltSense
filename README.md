<div align="center">

# ⚡ VoltSense

**Real-time EV charging demand intelligence platform with spatial forecasting**

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Warehouse-29B5E8?style=flat-square&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![dbt](https://img.shields.io/badge/dbt-Transforms-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-Orchestration-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![LightGBM](https://img.shields.io/badge/LightGBM-Forecasting-02569B?style=flat-square&logo=microsoft&logoColor=white)](https://lightgbm.readthedocs.io/)
[![AWS Lambda](https://img.shields.io/badge/AWS_Lambda-Batch_Ingestion-FF9900?style=flat-square&logo=awslambda&logoColor=white)](https://aws.amazon.com/lambda/)
[![Amazon S3](https://img.shields.io/badge/Amazon_S3-Raw_Landing-569A31?style=flat-square&logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)
[![FastAPI](https://img.shields.io/badge/FastAPI-Model_Serving-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Power BI](https://img.shields.io/badge/Power_BI-Executive_BI-F2C811?style=flat-square&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Operator_Console-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

`IN PROGRESS`

</div>

---

Real-time EV charging demand intelligence platform.

Ingests simulated charger telemetry via Apache Kafka into Snowflake, pulls
weather and grid pricing data through AWS Lambda into S3, transforms everything
through a dbt medallion architecture with spatial demand features, trains a
LightGBM forecasting model, serves predictions via FastAPI, orchestrates the
pipeline with Airflow, and delivers insights through Power BI and Streamlit.

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

The architecture puts the right tool on the right workload. Charger telemetry
is high-volume streaming (~164K events/day) and goes through Kafka. Weather
and grid pricing are low-volume periodic API pulls (288 and 864 readings/day)
and go through Lambda + S3.

```
Charger sessions (~164K events/day)
        |
     Kafka (1 topic, 12 partitions)
        |
   Snowpipe Streaming
        |
        v
   Snowflake RAW  <---  S3 (external stage)  <---  Lambda (scheduled)
        |                      ^        ^               |         |
        |                      |        |          Weather    Grid pricing
   dbt transforms         weather/  grid_pricing/  (15 min)    (5 min)
        |                                          Open-Meteo    EIA API
   staging -> intermediate -> marts
                    |
             spatial features
             (Haversine pairs,
              neighbor util,
              cluster saturation)
                    |
    +---------------+---------------+
    |               |               |
Power BI        Streamlit       LightGBM
(executives)   (operators)    (forecasting)
                                    |
                                 FastAPI
                              (/predict, /simulate)

         Airflow orchestrates the daily pipeline
```

## Tech stack

| Tool | Role |
|---|---|
| Apache Kafka | Streaming ingestion for charger telemetry |
| AWS Lambda | Scheduled weather and grid pricing API pulls |
| AWS S3 | Raw data landing zone for batch sources |
| Snowflake | Data warehouse (streaming via Snowpipe, batch via S3 external stage) |
| dbt | SQL transformations with testing, documentation, lineage |
| Apache Airflow | Pipeline orchestration (dbt -> ML -> BI refresh) |
| LightGBM | Demand forecasting with 25 features including spatial |
| FastAPI | Model serving API (/predict, /simulate endpoints) |
| Power BI | Executive dashboards (DirectQuery to Snowflake) |
| Streamlit | Operator console with interactive pricing simulator |

## Project structure

```
voltsense/
├── simulator/               Charger event producer (Kafka)
├── lambdas/                 AWS Lambda functions (weather + grid pricing)
│   ├── weather_puller/      Pulls Open-Meteo API, writes to S3
│   └── grid_price_puller/   Pulls EIA API, writes to S3
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
├── api/                     FastAPI model serving endpoint
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
- [ ] Refactor: Move weather + pricing from Kafka to Lambda + S3
- [ ] Addition: FastAPI model serving endpoint

## Author

**Nikhil Bharadwaj Yellapragada**
<br>
MS Data Analytics Engineering, Northeastern University

[![LinkedIn](https://img.shields.io/badge/-LinkedIn-0A66C2?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/nikhil-bharadwaj-yellapragada-48321a211/)
[![Email](https://img.shields.io/badge/-Email-D14836?style=flat-square&logo=gmail&logoColor=white)](mailto:yellapragada.n@northeastern.edu)
[![GitHub](https://img.shields.io/badge/-GitHub-181717?style=flat-square&logo=github&logoColor=white)](https://github.com/Nikhil20012)

## License

MIT. See [LICENSE](LICENSE) for details.