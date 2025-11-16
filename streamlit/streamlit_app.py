import streamlit as st
import redis
import psycopg2
import pandas as pd
import json
import time
from datetime import datetime

# --- Page Configuration ---
st.set_page_config(
    page_title="IoT Anomaly Dashboard",
    page_icon="🚨",
    layout="wide"
)

# --- Database Connections ---

# Connect to Redis
@st.cache_resource
def get_redis_conn():
    return redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# Connect to PostgreSQL
@st.cache_resource
def get_postgres_conn():
    return psycopg2.connect(
        host="postgres",
        database="iot_db",
        user="user",
        password="password"
    )

r = get_redis_conn()
conn = get_postgres_conn()

# --- Helper Function ---
def get_historical_anomalies(selected_date):
    """Query Postgres for anomalies on a specific date."""
    query = """
    SELECT device_id, timestamp_recorded, temperature, reason 
    FROM anomalies 
    WHERE DATE(timestamp_recorded) = %s
    ORDER BY timestamp_recorded DESC
    """
    try:
        df = pd.read_sql_query(query, conn, params=(selected_date,))
        return df
    except Exception as e:
        st.error(f"Error querying PostgreSQL: {e}")
        return pd.DataFrame()

# --- Main Dashboard ---
st.title("🚨 Real-Time IoT Anomaly Detector")
st.markdown("---")

# --- 1. Live Data Section ---
st.header("🔴 Live Sensor Status")
st.markdown("Data updates automatically every 2 seconds from **Redis**.")

# Create placeholders for live data
cols = st.columns(3)
with cols[0]:
    st.subheader("Sensor A")
    sensor_a_placeholder = st.empty()
with cols[1]:
    st.subheader("Sensor B")
    sensor_b_placeholder = st.empty()
with cols[2]:
    st.subheader("Sensor C")
    sensor_c_placeholder = st.empty()

st.subheader("Recent Anomalies")
anomalies_placeholder = st.empty()

st.markdown("---")

# --- 2. Historical Data Section ---
st.header("📜 Historical Anomaly Log")
st.markdown("Query historical anomalies from **PostgreSQL**.")

# Date picker
selected_date = st.date_input("Select a date", datetime.today())

# Query and display data
history_df = get_historical_anomalies(selected_date)
if history_df.empty:
    st.info("No anomalies found for the selected date.")
else:
    st.dataframe(history_df, use_container_width=True)

# --- Auto-refreshing Loop ---
while True:
    # Update Live Sensor Data
    for i, sensor_id in enumerate(['sensor-A', 'sensor-B', 'sensor-C']):
        data = r.get(f"device:{sensor_id}:latest")
        if data:
            data_dict = json.loads(data)
            temp = data_dict['temperature']
            
            # Update the correct placeholder
            if i == 0:
                placeholder = sensor_a_placeholder
            elif i == 1:
                placeholder = sensor_b_placeholder
            else:
                placeholder = sensor_c_placeholder
                
            # Use appropriate color
            if temp > 100:
                placeholder.error(f"**{temp}°C** (Anomaly)")
            else:
                placeholder.success(f"**{temp}°C** (Normal)")

    # Update Recent Anomalies List
    anomalies_list = r.lrange("recent_anomalies", 0, 9)
    with anomalies_placeholder.container():
        st.markdown("**Last 10 detected anomalies:**")
        if not anomalies_list:
            st.text("No recent anomalies.")
        for anomaly in anomalies_list:
            anomaly_dict = json.loads(anomaly)
            dt = datetime.fromtimestamp(anomaly_dict['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            st.warning(f"**{anomaly_dict['device_id']}** reported **{anomaly_dict['temperature']}°C** at {dt}")
    
    # Wait 2 seconds before refreshing
    time.sleep(2)