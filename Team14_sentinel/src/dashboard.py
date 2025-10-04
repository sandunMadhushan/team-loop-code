import streamlit as st
import pandas as pd
import json
import os

# Construct a robust path to the events file.
# The dashboard script is in `src/`, and the events file is in `evidence/output/test/`.
script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
EVENTS_FILE_PATH = os.path.join(project_root, 'evidence/output/test/events.jsonl')

def load_events(filepath):
    """Loads events from a JSONL file."""
    events = []
    if not os.path.exists(filepath):
        st.error(f"Events file not found at: {os.path.abspath(filepath)}")
        st.info("Please run the data processing pipeline first to generate the events.jsonl file.")
        return pd.DataFrame()

    with open(filepath, 'r') as f:
        for line in f:
            events.append(json.loads(line))

    if not events:
        st.warning("No events found in the output file.")
        return pd.DataFrame()

    # Normalize the nested JSON data into a flat DataFrame
    df = pd.json_normalize(events)
    # Rename columns for clarity
    df.rename(columns={
        'event_data.event_name': 'Event Name',
        'event_data.station_id': 'Station ID',
        'timestamp': 'Timestamp'
    }, inplace=True)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    return df

def run_dashboard():
    """
    Main function to create and run the Streamlit dashboard.
    """
    st.set_page_config(page_title="Project Sentinel Dashboard", layout="wide")
    st.title("🚨 Project Sentinel: Anomaly Detection Dashboard")
    st.markdown("This dashboard visualizes events detected by the Sentinel monitoring system.")

    events_df = load_events(EVENTS_FILE_PATH)

    if not events_df.empty:
        # --- Key Metrics ---
        st.header("Live Event Summary")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Events Detected", len(events_df))
        col2.metric("Most Frequent Event", events_df['Event Name'].mode()[0])
        col3.metric("Stations Affected", events_df['Station ID'].nunique())

        # --- Event Visualization ---
        st.header("Event Analysis")

        # Bar chart of event counts
        event_counts = events_df['Event Name'].value_counts()
        st.bar_chart(event_counts)

        # Detailed event log (filterable)
        st.header("Detailed Event Log")
        st.dataframe(events_df)

    else:
        st.info("Dashboard is waiting for event data.")


if __name__ == "__main__":
    run_dashboard()