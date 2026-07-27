import os

SNOWFLAKE_CONFIG = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "database": "VOLTSENSE",
    "warehouse": "VOLTSENSE_WH",
}

TARGET = "TARGET_UTILIZATION_PCT"

FEATURES = [
    "UTIL_LAG_15MIN",
    "UTIL_LAG_1H",
    "UTIL_LAG_1D",
    "UTIL_LAG_1W",
    "ROLLING_24H_AVG_UTIL",
    "ROLLING_7D_AVG_UTIL",
    "ROLLING_24H_STD_UTIL",
    "NEARBY_AVG_UTILIZATION",
    "NEARBY_WEIGHTED_AVG_UTILIZATION",
    "NEARBY_STATION_COUNT",
    "NEARBY_AVAILABLE_CAPACITY_KW",
    "NEARBY_MAX_UTILIZATION",
    "CLUSTER_SATURATION_PCT",
    "NEAREST_NEIGHBOR_KM",
    "ISOLATION_SCORE",
    "HOUR_OF_DAY",
    "DAY_OF_WEEK",
    "IS_WEEKEND",
    "TEMPERATURE_C",
    "PRECIPITATION_MM",
    "WIND_SPEED_KMH",
    "MAX_CAPACITY_KW",
    "TOTAL_CONNECTORS",
    "STATION_TYPE",
    "PRICING_TIER",
    "PRICE_PER_KWH_USD",
]

CATEGORICAL_FEATURES = ["STATION_TYPE", "PRICING_TIER"]

LGBM_PARAMS = {
    "objective": "regression",
    "metric": "mae",
    "boosting_type": "gbdt",
    "num_leaves": 63,
    "learning_rate": 0.05,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "verbose": -1,
    "n_estimators": 300,
}