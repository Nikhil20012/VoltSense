import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Demand Forecast", layout="wide")
st.title("Demand Forecast Explorer")


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


stations = sf_query("SELECT STATION_ID FROM VOLTSENSE.DEV_MARTS.DIM_STATION ORDER BY STATION_ID")
selected = st.selectbox("Station", stations["STATION_ID"].tolist())

if selected:
    df = sf_query(f"""
        SELECT TIMESTAMP_15MIN, UTILIZATION_PCT,
               NEARBY_WEIGHTED_AVG_UTILIZATION, CLUSTER_SATURATION_PCT
        FROM VOLTSENSE.DEV_MARTS.FACT_STATION_UTILIZATION_15MIN
        WHERE STATION_ID = '{selected}'
        ORDER BY TIMESTAMP_15MIN
    """)

    if len(df) > 0:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["TIMESTAMP_15MIN"], y=df["UTILIZATION_PCT"],
            name="Utilization %", line=dict(color="#2196F3"),
        ))
        fig.add_trace(go.Scatter(
            x=df["TIMESTAMP_15MIN"], y=df["NEARBY_WEIGHTED_AVG_UTILIZATION"],
            name="Nearby Weighted Avg", line=dict(color="#FF9800", dash="dash"),
        ))
        fig.update_layout(
            title=f"Utilization - {selected}",
            xaxis_title="Time",
            yaxis_title="Utilization %",
            height=500,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Cluster Saturation Over Time")
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=df["TIMESTAMP_15MIN"], y=df["CLUSTER_SATURATION_PCT"],
            name="Cluster Saturation", fill="tozeroy",
            line=dict(color="#F44336"),
        ))
        fig2.update_layout(height=300, yaxis_title="Saturation %")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No data for this station")