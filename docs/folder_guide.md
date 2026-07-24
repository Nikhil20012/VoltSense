# Folder Guide

What each folder does, why it exists, and when it gets built.

```
voltsense/
├── simulator/                 Step 1 - where data is generated
├── scripts/                   Steps 0-2 - helper scripts
├── dbt/voltsense_dbt/         Steps 3-5 - where data is transformed
├── ml/                        Step 6 - model training and prediction
├── airflow/                   Step 7 - pipeline automation
├── streamlit/                 Step 9 - operator-facing dashboard
├── powerbi/                   Step 8 - executive-facing dashboards
├── infrastructure/            Step 0 - local dev environment (Docker)
├── docs/                      Documentation
└── .github/                   CI/CD workflows
```

## simulator/

Generates realistic EV charger events and sends them to Kafka. This is the
data source for the entire pipeline. Events follow the OCPP protocol format
that real chargers use (session start, periodic heartbeats, session end).

The simulator models station-specific demand patterns. Highway stations peak
at travel hours, workplace stations peak 9-to-5, and so on.

```
simulator/
├── charger_simulator.py      Generates charging events, produces to Kafka
├── weather_producer.py       Polls Open-Meteo API, produces to Kafka
├── grid_price_producer.py    Simulates electricity pricing, produces to Kafka
├── station_profiles.py       Demand curves per station type
├── config.yml                Simulation parameters
└── requirements.txt
```

## scripts/

Utility scripts that don't belong to a specific component.

```
scripts/
├── generate_station_seed.py  Creates 200 stations with Boston-area coordinates
├── kafka_to_snowflake.py     Dev bridge: reads Kafka, inserts into Snowflake
└── setup_snowflake.sql       Database and schema creation SQL
```

`kafka_to_snowflake.py` is a development shortcut. In production you would use
Snowpipe Streaming (managed, zero-code). For local dev, this Python consumer
does the same job and is simpler to set up.

## dbt/voltsense_dbt/

Takes raw JSON in Snowflake and transforms it through three layers into clean,
tested tables for Power BI and ML.

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

The pricing simulator loads the trained LightGBM model locally and runs
inference with modified price inputs. This is something Power BI cannot do.

```
streamlit/
├── app.py                    Multi-page entry point
├── pages/
│   ├── 1_station_health.py   Station map with anomaly alerts
│   ├── 2_demand_forecast.py  Predicted vs actual utilization
│   └── 3_pricing_sim.py      What-if pricing simulator (runs live ML inference)
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
