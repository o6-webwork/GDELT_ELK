# frontend/analysis.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests # Import requests library
import pymannkendall as mk
from datetime import date, timedelta, datetime
import logging
import os

# --- Configuration ---
# Use environment variables for backend URL or default
# Assumes the backend service is named 'backend-api' in docker-compose
BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://backend-api:8000")

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Helper Functions ---

# MODIFIED: Fetch data from FastAPI backend
@st.cache_data(ttl=3600) # Cache data for 1 hour
def fetch_data_from_api(start_date_str: str, end_date_str: str, max_entities: int = 500):
    """Fetches aggregated entity trends from the FastAPI backend."""
    api_endpoint = f"{BACKEND_API_URL}/trends"
    params = {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "max_entities": max_entities,
        # Add other parameters like index, entity_field, date_field if needed
        # and if the backend endpoint supports them.
        # "index": "gkg*",
        # "entity_field": "V21AllNames.Name.keyword",
        # "date_field": "V2ExtrasXML.PubTimestamp",
    }
    logger.info(f"Fetching data from API: {api_endpoint} with params: {params}")
    try:
        response = requests.get(api_endpoint, params=params, timeout=120) # Increased timeout
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()
        logger.info(f"Successfully fetched {len(data.get('buckets', []))} buckets from API.")
        # The backend already returns the buckets directly under the "buckets" key
        return data.get('buckets', [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")
        st.error(f"Error fetching data from backend API: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during API data fetch: {e}", exc_info=True)
        st.error(f"An unexpected error occurred: {e}")
        return None

# UNCHANGED: Process data function (assuming backend returns the same 'buckets' structure)
def process_data(buckets):
    """Processes the raw bucket data from Elasticsearch/API into a usable format."""
    if not buckets:
        return pd.DataFrame()

    processed_data = []
    for day_bucket in buckets:
        day_str = day_bucket['key_as_string'][:10] # Extract YYYY-MM-DD
        # Check if top_entities and its buckets exist
        top_entities_agg = day_bucket.get('top_entities', {})
        entity_buckets = top_entities_agg.get('buckets', [])

        if entity_buckets: # Only process if there are entities for the day
            for entity_bucket in entity_buckets:
                entity_name = entity_bucket['key']
                doc_count = entity_bucket['doc_count']
                processed_data.append({
                    'Date': pd.to_datetime(day_str),
                    'Entity': entity_name,
                    'Count': doc_count
                })
        # Optional: Include days with 0 documents if the API provides them
        # elif day_bucket.get('doc_count', 0) == 0:
        #     processed_data.append({'Date': pd.to_datetime(day_str), 'Entity': None, 'Count': 0})

    if not processed_data:
        return pd.DataFrame()

    df = pd.DataFrame(processed_data)

    # Pivot table to get entities as columns and dates as index
    try:
        pivot_df = df.pivot(index='Date', columns='Entity', values='Count').fillna(0)
        return pivot_df
    except Exception as e:
        logger.error(f"Error pivoting data: {e}")
        st.error(f"Error processing data for analysis: {e}")
        return pd.DataFrame()


# UNCHANGED: Mann-Kendall analysis function
def run_mk_analysis(series, alpha=0.05):
    """Runs Mann-Kendall test on a pandas Series."""
    try:
        # Ensure series has numeric type and enough data points
        series = pd.to_numeric(series, errors='coerce').dropna()
        if len(series) < 3: # MK test needs at least 3 data points
            return {"trend": "no trend", "p_value": None, "slope": None}

        result = mk.original_test(series, alpha=alpha)
        return {"trend": result.trend, "p_value": result.p, "slope": result.slope}
    except Exception as e:
        # Log the specific series name if available, or just the error
        series_name = series.name if hasattr(series, 'name') else 'Unknown Series'
        logger.warning(f"Mann-Kendall test failed for series '{series_name}': {e}")
        return {"trend": "error", "p_value": None, "slope": None}

# UNCHANGED: Plotting function
def plot_trends(df_pivot, mk_results, title="Entity Trends Over Time"):
    """Plots entity trends using Plotly."""
    fig = go.Figure()

    for entity in df_pivot.columns:
        mk_res = mk_results.get(entity, {})
        trend = mk_res.get('trend', 'no trend')
        p_value = mk_res.get('p_value')
        slope = mk_res.get('slope')

        legend_label = f"{entity}"
        line_style = 'solid' # Default
        color = None # Default plotly color cycle

        if trend == "increasing":
            legend_label += " (↗)"
            # color = 'green' # Optional: specific color
        elif trend == "decreasing":
            legend_label += " (↘)"
            # color = 'red' # Optional: specific color
        elif trend == "no trend":
             line_style = 'dash' # Optional: different style for no trend
        # Add handling for 'error' if needed

        fig.add_trace(go.Scatter(
            x=df_pivot.index,
            y=df_pivot[entity],
            mode='lines+markers',
            name=legend_label,
            line=dict(dash=line_style),
            marker=dict(size=4),
            # hovertemplate=f"<b>{entity}</b><br>Date: %{{x|%Y-%m-%d}}<br>Count: %{{y}}<extra></extra>" # Basic hover
            customdata=[[entity, trend, p_value, slope]] * len(df_pivot.index), # Store MK results for hover
             hovertemplate=(
                f"<b>%{{customdata[0]}}</b><br>"
                f"Date: %{{x|%Y-%m-%d}}<br>"
                f"Count: %{{y}}<br>"
                f"Trend: %{{customdata[1]}}<br>"
                f"p-value: %{{customdata[2]:.3f}}<br>"
                f"Slope: %{{customdata[3]:.2f}}<extra></extra>"
             )
        ))

    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Document Count",
        legend_title="Entities (Trend)",
        hovermode="x unified"
    )
    return fig


# --- Streamlit App Layout ---
st.set_page_config(layout="wide")
st.title("GDELT Entity Trend Analysis")

# --- Date Range Selection ---
col1, col2 = st.columns(2)
with col1:
    # Default end date is yesterday
    default_end_date = date.today() - timedelta(days=1)
    end_date = st.date_input("End Date", value=default_end_date, max_value=default_end_date)
with col2:
    # Default start date is 7 days before the default end date
    default_start_date = default_end_date - timedelta(days=6)
    start_date = st.date_input("Start Date", value=default_start_date, max_value=end_date)

# Validate date range
if start_date > end_date:
    st.error("Error: Start date must be before or the same as end date.")
else:
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # --- Fetch and Process Data ---
    st.subheader(f"Analyzing trends from {start_date_str} to {end_date_str}")

    # Fetch data using the modified function
    raw_buckets = fetch_data_from_api(start_date_str, end_date_str)

    if raw_buckets is not None: # Proceed only if API call was successful
        if not raw_buckets:
            st.warning("No data returned from the backend for the selected date range.")
        else:
            with st.spinner("Processing data..."):
                 df_pivot = process_data(raw_buckets)

            if df_pivot.empty:
                 st.warning("Could not process data or no documents found for the selected entities and date range.")
            else:
                st.write("Data processing complete. Running trend analysis...")

                # --- Trend Analysis ---
                with st.spinner("Running Mann-Kendall trend analysis..."):
                    mk_results = {}
                    significant_trends = {}
                    for entity in df_pivot.columns:
                        mk_results[entity] = run_mk_analysis(df_pivot[entity])
                        if mk_results[entity]["trend"] in ["increasing", "decreasing"]:
                            significant_trends[entity] = mk_results[entity]

                # --- Display Results ---
                st.subheader("Significant Trends Found:")
                if significant_trends:
                    # Sort by trend type then slope magnitude (descending for increase, ascending for decrease)
                    sorted_trends = sorted(
                        significant_trends.items(),
                        key=lambda item: (item[1]['trend'] == 'decreasing', abs(item[1]['slope'] or 0)),
                        reverse=True
                    )
                    trend_data = []
                    for entity, res in sorted_trends:
                        trend_data.append({
                             "Entity": entity,
                             "Trend": res['trend'].capitalize(),
                             "P-value": f"{res['p_value']:.3f}" if res['p_value'] is not None else "N/A",
                             "Slope": f"{res['slope']:.2f}" if res['slope'] is not None else "N/A"
                        })
                    st.dataframe(pd.DataFrame(trend_data), use_container_width=True)
                else:
                    st.info("No statistically significant increasing or decreasing trends found.")

                st.subheader("Trend Visualization")
                with st.spinner("Generating plot..."):
                    fig = plot_trends(df_pivot, mk_results, title=f"Entity Counts ({start_date_str} to {end_date_str})")
                    st.plotly_chart(fig, use_container_width=True)

                # Optional: Display Raw Pivot Data
                with st.expander("Show Raw Daily Counts"):
                    st.dataframe(df_pivot)

    else:
         # Error message already shown by fetch_data_from_api
         st.info("Could not retrieve data from the backend API.")

# --- Monitoring Section (Example) ---
st.sidebar.title("Pipeline Monitoring")

# Fetch status from backend /status endpoint
status_endpoint = f"{BACKEND_API_URL}/status"
try:
    status_resp = requests.get(status_endpoint, timeout=10)
    status_resp.raise_for_status()
    status_data = status_resp.json()
    st.sidebar.metric("API Status", status_data.get("api_status", "Unknown").capitalize())
    st.sidebar.metric("Elasticsearch Status", status_data.get("elasticsearch_status", "Unknown").capitalize())
except requests.exceptions.RequestException as e:
    st.sidebar.error(f"API Status Error: {e}")
except Exception as e:
    st.sidebar.error(f"Error parsing status: {e}")


# Fetch logs from backend /logs/gdelt_etl endpoint
logs_endpoint = f"{BACKEND_API_URL}/logs/gdelt_etl" # Example log name
try:
    logs_resp = requests.get(logs_endpoint, params={"lines": 50}, timeout=15) # Get last 50 lines
    logs_resp.raise_for_status()
    logs_data = logs_resp.json()
    log_lines = logs_data.get("lines", ["Failed to load logs."])
    with st.sidebar.expander("Show ETL Logs (Last 50 lines)", expanded=False):
         st.code("\n".join(log_lines), language="log")
except requests.exceptions.RequestException as e:
    st.sidebar.error(f"ETL Logs Error: {e}")
except Exception as e:
    st.sidebar.error(f"Error parsing logs: {e}")

# Add placeholders for other monitoring info if needed
# st.sidebar.metric("ETL Status", "N/A")
# st.sidebar.metric("Load Status", "N/A")