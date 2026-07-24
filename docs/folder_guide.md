# Folder Guide

What each folder does, why it exists, and when it gets built.

```
voltsense/
├── simulator/                 Step 1 - charger event producer (Kafka)
├── lambdas/                   Phase 2 - weather and grid pricing API pullers (AWS)
├── scripts/                   Steps 0-2 - helper scripts
├── dbt/voltsense_dbt/         Steps 3-5 - where data is transformed
├── ml/                        Step 6 - model training and prediction
├── api/                       Phase 3 - FastAPI model serving
├── airflow/                   Step 7 - pipeline automation
├── streamlit/                 Step 9 - operator-facing dashboard
├── powerbi/                   Step 8 - executive-facing dashboards
├── infrastructure/            Step 0 - local dev environment (Docker)
├── docs/                      Documentation
└── .github/                   CI/CD workflows
```

## simulator/

Generates realistic EV charger events and sends them to Kafka. This is the
high-volume streaming data source (~164K events/day). Events follow the OCPP
protocol format that real chargers use (session start, periodic heartbeats,
session end).

The simulator models station-specific demand patterns. Highway stations peak
at travel hours, workplace stations peak 9-to-5, and so on.

```
simulator/
├── charger_simulator.py      Generates charging events, produces to Kafka
├── station_profiles.py       Demand curves per station type
├── config.yml                Simulation parameters
└── requirements.txt
```

Weather and grid pricing are handled by Lambda functions, not the simulator.
Those are low-volume periodic API pulls that don't need Kafka.

## lambdas/

Two AWS Lambda functions that pull from external APIs on a schedule and
write JSON files to S3. CloudWatch Events triggers them.

```
lambdas/
├── weather_puller/
│   ├── handler.py            Pulls Open-Meteo API every 15 min, writes to S3
│   └── requirements.txt
└── grid_price_puller/
    ├── handler.py            Pulls EIA API every 5 min, writes to S3
    └── requirements.txt
```

Why Lambda instead of Kafka for these? Weather produces 288 readings/day.
Grid pricing produces 864 readings/day. Running those through a distributed
streaming platform is unnecessary overhead. Lambda on a CloudWatch schedule
is the right tool for periodic, low-volume API pulls.

S3 output structure:
```
s3://voltsense-raw/
├── weather/2026/07/22/00-00.json
├── weather/2026/07/22/00-15.json
└── grid_pricing/2026/07/22/00-00.json
```

Snowflake loads from S3 via an external stage.

## scripts/

Utility scripts that don't belong to a specific component.

```
scripts/
├── generate_station_seed.py  Creates 200 stations with Boston-area coordinates
├── kafka_to_snowflake.py     Dev bridge: reads Kafka, inserts into Snowflake
└── setup_snowflake.sql       Database, schema, stage, and table creation SQL
```

`kafka_to_snowflake.py` is a development shortcut. In production you would use
Snowpipe Streaming (managed, zero-code). For local dev, this Python consumer
does the same job and is simpler to set up.

## dbt/voltsense_dbt/

Takes raw data in Snowflake (from both Kafka and S3 sources) and transforms
it through three layers into clean, tested tables for Power BI and ML.

From staging onward, dbt doesn't know or care whether data arrived via
Kafka or S3. It's all in Snowflake RAW tables at that point.

```
dbt/voltsense_dbt/
├── dbt_project.yml           Project configuration
├── packages.yml              External packages (dbt_utils, dbt_expectations)
├── profiles.yml.example      Snowflake connection template (real one is gitignored)
├── seeds/                    Static reference data loaded into Snowflake
│   ├── station_metadata.csv  200 stations with lat/lon, type, capacity
│   ├── charger_specs.csv     Hardware specs per charger model
│   └── us_holidays_2026.csv  Federal holidays for the date dimension
├── macros/                   Reusable SQL functions
│   └── haversine_km.sql      Distance between two lat/lon pairs
├── models/
│   ├── staging/              Layer 1: parse raw JSON, deduplicate, cast types
│   ├── intermediate/         Layer 2: business logic, spatial features, utilization
│   ├── marts/                Layer 3: star schema for BI (dimensions + facts)
│   └── ml_features/          Training dataset for the forecasting model
└── tests/                    Custom data quality assertions
```

**Why three layers?**

Staging is a 1:1 mapping from raw tables. Each raw table gets one staging
model that parses the JSON, deduplicates, and casts types. No business logic.

Intermediate is where domain knowledge becomes SQL. This layer computes
station utilization at 15-minute intervals, builds Haversine-based station
pairs, and calculates spatial demand features (nearby utilization, cluster
saturation, isolation scores).

Marts organizes everything into a star schema. Dimensions describe things
(stations, dates, locations). Facts record measurements (utilization, sessions,
anomalies). Power BI connects directly to these tables.

## ml/

Reads the training dataset from Snowflake, trains a LightGBM model, and
writes predictions back.

```
ml/
├── train.py                  Reads features from Snowflake, trains LightGBM
├── predict.py                Batch predictions written to Snowflake
├── config.py                 Feature lists, hyperparameters, connection settings
└── requirements.txt
```

## api/

FastAPI application that serves LightGBM predictions via REST API.

```
api/
├── main.py                   Endpoints: /health, /predict, /simulate
├── requirements.txt
└── Dockerfile
```

The Streamlit pricing simulator does live model inference, but that's a UI
tool. The FastAPI endpoint is a proper API that any downstream system can
consume. It loads the trained model at startup and reads recent features
from Snowflake for the requested station.

## airflow/dags/

One DAG that runs the full pipeline daily: dbt transforms, data quality tests,
model training, batch prediction, Power BI refresh.

```
airflow/
└── dags/
    └── voltsense_daily.py    Linear DAG: dbt_run -> dbt_test -> train -> predict -> refresh
```

The pipeline stops if any dbt test fails, so the model never retrains on
bad data.

## streamlit/

Interactive dashboard for station operators. Different audience from Power BI:
operators need real-time health monitoring and the ability to run pricing
simulations, not quarterly revenue reports.

```
streamlit/
├── app.py                    Multi-page entry point
├── pages/
│   ├── 1_station_health.py   Station map with anomaly alerts
│   ├── 2_demand_forecast.py  Predicted vs actual utilization
│   └── 3_pricing_sim.py      What-if pricing simulator
├── utils/
│   ├── snowflake_client.py   Query helper with caching
│   └── map_utils.py          Map rendering
├── model/                    Trained model artifact (gitignored)
└── requirements.txt
```

## powerbi/

Documentation for the Power BI dashboards. The .pbix file itself is not
committed (it is binary and large).

```
powerbi/
├── semantic_model_docs.md    Table relationships and DAX measures
└── dashboard_wireframes.md   Page layouts
```

## infrastructure/docker/

Docker Compose setup for running Kafka and Zookeeper locally.

```
infrastructure/
└── docker/
    ├── docker-compose.yml    Kafka + Zookeeper containers
    └── .env.example          Environment variable template
```

## docs/

Project documentation.

```
docs/
├── folder_guide.md           This file
├── architecture.md           Detailed architecture and tech justifications
├── development_guide.md      Step-by-step build instructions
└── images/                   Diagrams for the README
```

## .github/workflows/

CI workflow that runs `dbt test` on pull requests.