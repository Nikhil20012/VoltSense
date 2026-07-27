import streamlit as st
import pydeck as pdk
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Station Health", layout="wide")
st.title("Station Health Monitor")


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


util_df = sf_query("""
    SELECT u.STATION_ID, u.TIMESTAMP_15MIN, u.UTILIZATION_PCT,
           u.ACTIVE_SESSIONS, u.TOTAL_POWER_KW,
           u.NEARBY_AVG_UTILIZATION, u.CLUSTER_SATURATION_PCT,
           u.ISOLATION_SCORE, s.LATITUDE, s.LONGITUDE,
           s.STATION_NAME, s.STATION_TYPE
    FROM VOLTSENSE.DEV_MARTS.FACT_STATION_UTILIZATION_15MIN u
    JOIN VOLTSENSE.DEV_MARTS.DIM_STATION s ON u.STATION_ID = s.STATION_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY u.STATION_ID ORDER BY u.TIMESTAMP_15MIN DESC) = 1
""")

anomalies_df = sf_query("""
    SELECT STATION_ID, DETECTED_AT, ANOMALY_TYPE, SEVERITY, DESCRIPTION
    FROM VOLTSENSE.DEV_MARTS.FACT_ANOMALY_FLAGS
    WHERE IS_RESOLVED = FALSE ORDER BY DETECTED_AT DESC
""")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Stations", len(util_df))
col2.metric("Avg Utilization", f"{util_df['UTILIZATION_PCT'].mean():.1f}%")
col3.metric("Active Anomalies", len(anomalies_df))
col4.metric("Stations > 80%", len(util_df[util_df["UTILIZATION_PCT"] > 80]))


def get_color(util_pct):
    if util_pct > 80:
        return [220, 50, 50, 180]
    elif util_pct > 60:
        return [230, 180, 30, 180]
    return [50, 180, 50, 180]


util_df["color"] = util_df["UTILIZATION_PCT"].apply(get_color)

st.subheader("Station Map")
st.caption("Green: <60%  |  Yellow: 60-80%  |  Red: >80%")

layer = pdk.Layer(
    "ScatterplotLayer",
    data=util_df,
    get_position=["LONGITUDE", "LATITUDE"],
    get_color="color",
    get_radius=200,
    pickable=True,
)

view = pdk.ViewState(
    latitude=util_df["LATITUDE"].mean(),
    longitude=util_df["LONGITUDE"].mean(),
    zoom=11,
    pitch=0,
)

st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view))

st.subheader("Active Anomalies")
if len(anomalies_df) > 0:
    st.dataframe(anomalies_df, use_container_width=True)
else:
    st.info("No active anomalies")

st.subheader("Station Detail")
selected = st.selectbox("Select station", util_df["STATION_ID"].tolist())
if selected:
    row = util_df[util_df["STATION_ID"] == selected].iloc[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("Utilization", f"{row['UTILIZATION_PCT']:.1f}%")
    c2.metric("Active Sessions", int(row["ACTIVE_SESSIONS"]))
    c3.metric("Nearby Avg Util", f"{row['NEARBY_AVG_UTILIZATION']:.1f}%")

    c4, c5, c6 = st.columns(3)
    c4.metric("Cluster Saturation", f"{row['CLUSTER_SATURATION_PCT']:.1%}")
    c5.metric("Isolation Score", f"{row['ISOLATION_SCORE']:.2f}")
    c6.metric("Type", row["STATION_TYPE"])