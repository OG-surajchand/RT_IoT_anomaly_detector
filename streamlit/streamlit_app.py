import streamlit as st
import redis
import pandas as pd
from datetime import datetime

# --- 1. Configuration ---
REDIS_HOST = "redis" # Must be resolvable from the Streamlit container
REDIS_PORT = 6379
KEY_SCAN_PATTERN = "latest_iot_reading:*"

# --- 2. Data Retrieval Function with Real-Time Refresh ---
@st.cache_data(ttl=5) # 👈 KEY: Automatically re-run this function every 5 seconds (5s TTL)
def fetch_latest_data():
    """Connects to Redis, fetches data, and returns a Pandas DataFrame."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping()
    except Exception as e:
        st.error(f"⚠️ Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}. Dashboard will not update. Error: {e}")
        return pd.DataFrame()

    keys = r.keys(KEY_SCAN_PATTERN)
    data_list = []
    
    for key in keys:
        record = r.hgetall(key)
        record['redis_key'] = key
        data_list.append(record)

    df = pd.DataFrame(data_list)
    
    if not df.empty:
        # Convert types for correct display and sorting
        numeric_cols = ['temperature']
        for col_name in numeric_cols:
            if col_name in df.columns:
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
        
        if 'timestamp_recorded' in df.columns:
            df['timestamp_recorded'] = pd.to_datetime(df['timestamp_recorded'], errors='coerce')
            df = df.sort_values('timestamp_recorded', ascending=False)
            
    return df

# --- 3. Streamlit UI Layout ---
st.set_page_config(layout="wide")

st.caption(f"Last updated: **{datetime.now().strftime('%H:%M:%S')}** (Data refreshes every 5 seconds)")


# Fetch and display data
data_df = fetch_latest_data()

if not data_df.empty:
    
    # -------------------------------------------------------------------------
    # NEW: Aggregation for Real-Time Bar Chart
    # -------------------------------------------------------------------------
    
    # 1. Filter for only records marked as 'anomaly'
    df_anomalies = data_df[data_df['reason'].str.contains('anomaly', case=False, na=False)]
    
    # 2. Group by device_id and count the number of anomalies (within the latest 50 records)
    anomaly_counts = df_anomalies.groupby('device_id').size().reset_index(name='Anomaly Count')
    
    # 3. Ensure all known devices are in the chart, even if their count is 0 in the current batch
    all_devices = data_df['device_id'].unique()
    df_all_devices = pd.DataFrame({'device_id': all_devices})
    df_final_bar = pd.merge(df_all_devices, anomaly_counts, on='device_id', how='left').fillna(0)
    df_final_bar['Anomaly Count'] = df_final_bar['Anomaly Count'].astype(int)

    # --- Visualization Section ---
    st.header("Anomalies by Device (Last 50 Records)")
    
    # Display the dynamic bar chart
    st.bar_chart(
        df_final_bar.set_index('device_id'), 
        y='Anomaly Count',
        height=300
    )
    
    # -------------------------------------------------------------------------
    # End of NEW Aggregation
    # -------------------------------------------------------------------------

    st.header("Temperature Trend (Detailed View)")
    st.line_chart(
        data_df, 
        x='timestamp_recorded', 
        y='temperature', 
        color='device_id'
    )
    
    # --- Anomaly Statistics ---
    anomaly_total = df_anomalies.shape[0]
    st.metric(label="Total Recent Anomalies Detected (in batch)", value=anomaly_total)
    
    # --- Data Table ---
    st.header("Latest Readings Table")
    st.dataframe(data_df.head(50), use_container_width=True) 

else:
    st.warning("No data found in Redis or failed to connect.")