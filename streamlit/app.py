import streamlit as st

st.set_page_config(
    page_title="VoltSense",
    page_icon="⚡",
    layout="wide",
)

st.title("VoltSense")
st.subheader("EV Charging Intelligence - Operator Console")

st.markdown("""
Select a page from the sidebar:

- **Station Health** - Monitor station status and active anomalies
- **Demand Forecast** - Explore predicted vs actual demand
- **Pricing Simulator** - Test pricing scenarios with live ML inference
""")