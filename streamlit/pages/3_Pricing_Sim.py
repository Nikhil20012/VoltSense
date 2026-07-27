import streamlit as st
import plotly.graph_objects as go
import lightgbm as lgb
import numpy as np
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Pricing Simulator", layout="wide")
st.title("Pricing What-If Simulator")

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


@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="VOLTSENSE",
        warehouse="VOLTSENSE_WH",
    )


@st.cache_data(ttl=300)
def sf_query(sql):
    return pd.read_sql(sql, get_conn())


@st.cache_resource
def load_model():
    if not os.path.exists(MODEL_PATH):
        return None
    return lgb.Booster(model_file=MODEL_PATH)


model = load_model()
if model is None:
    st.error(f"Model not found at {MODEL_PATH}. Run ml/train.py first.")
    st.stop()

stations = sf_query("SELECT STATION_ID FROM VOLTSENSE.DEV_MARTS.DIM_STATION ORDER BY STATION_ID")
selected = st.selectbox("Station", stations["STATION_ID"].tolist())
proposed_price = st.slider("Proposed price ($/kWh)", 0.10, 0.60, 0.25, 0.01)

if selected and st.button("Run Simulation"):
    df = sf_query(f"""
        SELECT * FROM VOLTSENSE.DEV_ML_FEATURES.ML_TRAINING_DATASET
        WHERE STATION_ID = '{selected}'
        ORDER BY TIMESTAMP_15MIN
    """)

    if len(df) == 0:
        st.warning("No data for this station")
        st.stop()

    for col in FEATURES:
        if col not in ["STATION_TYPE", "PRICING_TIER"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["STATION_TYPE", "PRICING_TIER"]:
        df[col] = df[col].astype("category")

    feature_df = df[FEATURES]

    baseline_preds = model.predict(feature_df)

    whatif_df = feature_df.copy()
    whatif_df["PRICE_PER_KWH_USD"] = proposed_price
    whatif_preds = model.predict(whatif_df)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["TIMESTAMP_15MIN"], y=baseline_preds,
        name="Current Price", line=dict(color="#2196F3"),
    ))
    fig.add_trace(go.Scatter(
        x=df["TIMESTAMP_15MIN"], y=whatif_preds,
        name=f"At ${proposed_price:.2f}/kWh", line=dict(color="#FF9800"),
    ))
    fig.update_layout(
        title=f"Predicted Utilization - {selected}",
        xaxis_title="Time",
        yaxis_title="Predicted Utilization %",
        height=500,
    )
    st.plotly_chart(fig, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    avg_baseline = np.mean(baseline_preds)
    avg_whatif = np.mean(whatif_preds)
    col1.metric("Avg Util (Current)", f"{avg_baseline:.1f}%")
    col2.metric("Avg Util (Proposed)", f"{avg_whatif:.1f}%")
    col3.metric("Change", f"{avg_whatif - avg_baseline:+.1f} pct points")