"""
Model serving API. Loads the trained LightGBM model and serves
predictions over HTTP.

Endpoints:
    GET  /health              - Service status and model info
    POST /predict             - Predict utilization for a station
    POST /simulate            - What-if pricing simulation

Run:
    uvicorn api.main:app --reload
"""

import os
from typing import Optional

import lightgbm as lgb
import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv()

app = FastAPI(title="VoltSense API", version="1.0.0")

MODEL_PATH = "ml/model/voltsense_lgbm.txt"

FEATURES = [
    "UTIL_LAG_15MIN", "UTIL_LAG_1H", "UTIL_LAG_1D", "UTIL_LAG_1W",
    "ROLLING_24H_AVG_UTIL", "ROLLING_7D_AVG_UTIL", "ROLLING_24H_STD_UTIL",
    "NEARBY_AVG_UTILIZATION", "NEARBY_WEIGHTED_AVG_UTILIZATION",
    "NEARBY_STATION_COUNT", "NEARBY_AVAILABLE_CAPACITY_KW",
    "NEARBY_MAX_UTILIZATION", "CLUSTER_SATURATION_PCT",
    "NEAREST_NEIGHBOR_KM", "ISOLATION_SCORE",
    "HOUR_OF_DAY", "DAY_OF_WEEK", "IS_WEEKEND",
    "TEMPERATURE_C", "PRECIPITATION_MM", "WIND_SPEED_KMH",
    "MAX_CAPACITY_KW", "TOTAL_CONNECTORS",
    "STATION_TYPE", "PRICING_TIER",
    "PRICE_PER_KWH_USD",
]

# Load model at startup
model = None
if os.path.exists(MODEL_PATH):
    model = lgb.Booster(model_file=MODEL_PATH)


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="VOLTSENSE",
        warehouse="VOLTSENSE_WH",
    )


def get_station_features(station_id: str) -> pd.DataFrame:
    conn = get_snowflake_conn()
    df = pd.read_sql(f"""
        SELECT * FROM VOLTSENSE.DEV_ML_FEATURES.ML_TRAINING_DATASET
        WHERE STATION_ID = '{station_id}'
        ORDER BY TIMESTAMP_15MIN DESC
        LIMIT 96
    """, conn)
    conn.close()

    if len(df) == 0:
        return df

    for col in FEATURES:
        if col not in ["STATION_TYPE", "PRICING_TIER"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["STATION_TYPE", "PRICING_TIER"]:
        df[col] = df[col].astype("category")

    return df


# Request/response schemas
class PredictRequest(BaseModel):
    station_id: str

class SimulateRequest(BaseModel):
    station_id: str
    proposed_price: float

class PredictionResult(BaseModel):
    station_id: str
    predictions: list
    avg_predicted_utilization: float

class SimulationResult(BaseModel):
    station_id: str
    current_avg_utilization: float
    proposed_avg_utilization: float
    change_pct_points: float
    proposed_price: float


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "model_path": MODEL_PATH,
    }


@app.post("/predict", response_model=PredictionResult)
def predict(req: PredictRequest):
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    df = get_station_features(req.station_id)
    if len(df) == 0:
        raise HTTPException(status_code=404, detail=f"No data for {req.station_id}")

    preds = model.predict(df[FEATURES])

    return PredictionResult(
        station_id=req.station_id,
        predictions=[round(float(p), 2) for p in preds],
        avg_predicted_utilization=round(float(np.mean(preds)), 2),
    )


@app.post("/simulate", response_model=SimulationResult)
def simulate(req: SimulateRequest):
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    df = get_station_features(req.station_id)
    if len(df) == 0:
        raise HTTPException(status_code=404, detail=f"No data for {req.station_id}")

    feature_df = df[FEATURES]

    # Baseline prediction
    baseline_preds = model.predict(feature_df)

    # What-if prediction with modified price
    whatif_df = feature_df.copy()
    whatif_df["PRICE_PER_KWH_USD"] = req.proposed_price
    whatif_preds = model.predict(whatif_df)

    baseline_avg = float(np.mean(baseline_preds))
    whatif_avg = float(np.mean(whatif_preds))

    return SimulationResult(
        station_id=req.station_id,
        current_avg_utilization=round(baseline_avg, 2),
        proposed_avg_utilization=round(whatif_avg, 2),
        change_pct_points=round(whatif_avg - baseline_avg, 2),
        proposed_price=req.proposed_price,
    )