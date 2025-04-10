# frontend/pages/2_Trend_Analysis.py
# Updated to REMOVE Hamed-Rao test as requested

import streamlit as st
import pandas as pd
import pymannkendall as mk
from datetime import datetime, timedelta
import os
import traceback
import plotly.express as px
import plotly.graph_objects as go
import pytz
import requests
import logging

# --- Configuration ---
BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://backend-api:8000")
ENTITY_FIELD = "V21AllNames.Name.keyword"
DATE_FIELD = "V2ExtrasXML.PubTimestamp"
ES_INDEX = "gkg*"

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- API Data Fetching (Unchanged) ---
@st.cache_data(ttl=600)
def fetch_data_from_api(start_date_str: str, end_date_str: str, index: str, max_entities: int):
    """Fetches aggregated entity trends from the FastAPI backend."""
    api_endpoint = f"{BACKEND_API_URL}/trends"
    params = {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "max_entities": max_entities,
        "index": index,
        "entity_field": ENTITY_FIELD,
        "date_field": DATE_FIELD,
    }
    logger.info(f"Fetching data from API: {api_endpoint} with params: {params}")
    st.write(f"Fetching data for {start_date_str} to {end_date_str} (max {max_entities}/day)...")
    try:
        response = requests.get(api_endpoint, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        buckets = data.get('buckets', [])
        logger.info(f"Successfully fetched {len(buckets)} buckets from API.")
        st.write("Data fetched successfully!")
        return buckets
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")
        st.error(f"Error fetching data from backend API: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during API data fetch: {e}", exc_info=True)
        st.error(f"An unexpected error occurred: {e}")
        return None

# --- Data Processing (Unchanged) ---
def process_data(buckets):
    """Processes API aggregation results into a pivoted DataFrame."""
    st.write("Processing data...")
    if not buckets:
        st.warning("No data returned from API for the selected period.")
        return None
    processed_data = []
    try:
        for day_bucket in buckets:
            date_str = day_bucket.get('key_as_string')
            if not date_str: continue
            date = pd.to_datetime(date_str).normalize()
            entities_in_bucket = day_bucket.get('top_entities', {}).get('buckets', [])
            for entity_bucket in entities_in_bucket:
                processed_data.append({
                    'Date': date,
                    'Entity': entity_bucket['key'],
                    'Count': entity_bucket['doc_count']
                })
    except Exception as e:
         st.error(f"Error during initial data extraction from buckets: {e}")
         return None
    if not processed_data:
        st.warning("No entity data found within the buckets for the selected period.")
        return None
    df = pd.DataFrame(processed_data)
    try:
        pivot_df = df.pivot_table(index='Date', columns='Entity', values='Count', aggfunc='sum', fill_value=0)
    except Exception as e:
        st.error(f"Failed to pivot data. Error: {e}")
        st.dataframe(df.head())
        return None
    pivot_df = pivot_df.sort_index()
    st.write("Data processed successfully!")
    return pivot_df

# --- MK Analysis (Hamed-Rao Removed) ---
def run_mk_analysis(df, top_n_inc=10, top_n_dec=10, include_decreasing=False):
    """Runs Original Mann-Kendall test, filtering for trends."""
    st.write("Running Mann-Kendall analysis...")
    if df is None or df.empty:
        st.warning("Cannot run analysis on empty data.")
        return [], [] # Return TWO empty lists now

    results_orig_inc, results_orig_dec = {}, {}
    # REMOVED: results_hamed_inc, results_hamed_dec = {}, {}
    # REMOVED: debug_hamed_results = {}

    num_entities = len(df.columns)
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    status_text.text(f"Analyzing 0/{num_entities} entities...")

    for i, entity in enumerate(df.columns):
        series = df[entity]
        update_text = f"Analyzing entity {i+1}/{num_entities}: {entity}"
        series_length = len(series)

        if series.count() < 3 or series.nunique() < 2:
             status_text.text(f"{update_text} (skipped - insufficient data)")
             progress_bar.progress((i + 1) / num_entities)
             continue

        status_text.text(update_text)

        try:
            # --- Original Test ONLY ---
            result_orig = mk.original_test(series)
            if result_orig.h and result_orig.trend == 'increasing' and result_orig.slope > 0:
                results_orig_inc[entity] = result_orig
            elif include_decreasing and result_orig.h and result_orig.trend == 'decreasing' and result_orig.slope < 0:
                results_orig_dec[entity] = result_orig

            # --- REMOVED Hamed-Rao Test Section ---

        except Exception as e:
            logger.warning(f"MK analysis failed for entity '{entity}': {e}", exc_info=True)
            st.warning(f"Could not analyze entity '{entity}'. Error: {e}")

        progress_bar.progress((i + 1) / num_entities)

    progress_bar.empty()
    status_text.text("Analysis complete!")

    # --- REMOVED Hamed-Rao Debug Section ---

    # Sort results
    sorted_results_orig_inc = sorted(results_orig_inc.items(), key=lambda item: item[1].slope, reverse=True)
    sorted_results_orig_dec = sorted(results_orig_dec.items(), key=lambda item: item[1].slope, reverse=False)
    # REMOVED: sorted_results_hamed_inc / dec

    # Return only Original MK results
    return sorted_results_orig_inc[:top_n_inc], sorted_results_orig_dec[:top_n_dec]
    # REMOVED: Hamed results from return

# --- Formatting Results (Unchanged) ---
def format_results_df(results_list, trend_type):
    """Formats the MK results list into a DataFrame for display."""
    if not results_list:
        return pd.DataFrame()
    return pd.DataFrame([{
        'Rank': i + 1, 'Entity': entity, 'Trend Type': trend_type,
        'MK Slope': f"{result.slope:.4f}", 'Trend': result.trend,
        'Significant (h)': result.h, 'P-value': f"{result.p:.3g}"
    } for i, (entity, result) in enumerate(results_list)])

# --- Plotting (Unchanged, still takes inc/dec lists) ---
def plot_trends(df_pivot, entities_inc, entities_dec, title, max_lines_inc=5, max_lines_dec=5):
    """Plots the time series for top increasing and decreasing entities."""
    st.subheader(f"Time Series Plot ({title})")

    entities_to_plot_inc = [entity for entity, _ in entities_inc[:max_lines_inc]]
    entities_to_plot_dec = [entity for entity, _ in entities_dec[:max_lines_dec]]
    entities_to_plot = entities_to_plot_inc + entities_to_plot_dec

    if not entities_to_plot:
        st.write("No significant trends selected to plot.")
        return

    valid_entities_to_plot = [col for col in entities_to_plot if col in df_pivot.columns]
    if not valid_entities_to_plot:
        st.write("Selected entities for plotting not found in processed data.")
        return
    df_plot_wide = df_pivot[valid_entities_to_plot]

    df_plot_long = df_plot_wide.reset_index().melt(id_vars='Date', var_name='Entity', value_name='Daily Count')

    inc_count = len(entities_to_plot_inc)
    dec_count = len(entities_to_plot_dec)
    plot_title = f"Entity Mentions Over Time ({title})"
    if inc_count > 0 and dec_count > 0:
        plot_title = f"Top {inc_count} Inc / {dec_count} Dec Mentions ({title})"
    elif inc_count > 0:
        plot_title = f"Top {inc_count} Increasing Mentions ({title})"
    elif dec_count > 0:
        plot_title = f"Top {dec_count} Decreasing Mentions ({title})"

    fig = px.line(df_plot_long, x='Date', y='Daily Count', color='Entity',
                  title=plot_title,
                  labels={'Daily Count': 'Mentions per Day'},
                  markers=True)

    fig.update_layout(legend_title_text='Entity')
    st.plotly_chart(fig, use_container_width=True)


# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="GDELT Trend Analysis")
st.title("GDELT Entity Trend Analysis")

# --- Sidebar Inputs (Unchanged) ---
st.sidebar.header("Analysis Parameters")
try:
    singapore_tz = pytz.timezone('Asia/Singapore')
    today = datetime.now(singapore_tz).date()
except Exception as e:
    logger.warning(f"Could not use Singapore timezone ({e}). Using UTC for date defaults.")
    today = datetime.utcnow().date()

default_start = today - timedelta(days=30)
start_date = st.sidebar.date_input("Start date", default_start, key="trend_start_date_orig")
end_date = st.sidebar.date_input("End date", today, key="trend_end_date_orig")

include_decreasing = st.sidebar.toggle("Include Decreasing Trends", value=False,
                                       help="Show entities with significant decreasing trends in addition to increasing ones.")

top_n_inc = st.sidebar.number_input(f"Top N **Increasing** entities:", min_value=1, max_value=50, value=10)
max_lines_inc = st.sidebar.number_input("Max **Increasing** entities to plot:", min_value=1, max_value=20, value=10)

top_n_dec = 0
max_lines_dec = 0
if include_decreasing:
    st.sidebar.markdown("---")
    top_n_dec = st.sidebar.number_input("Top N **Decreasing** entities:", min_value=1, max_value=50, value=10)
    max_lines_dec = st.sidebar.number_input("Max **Decreasing** entities to plot:", min_value=1, max_value=20, value=10)
    st.sidebar.markdown("---")

max_entities_per_day = st.sidebar.number_input("Max unique entities to fetch per day:", min_value=10, max_value=20000, value=10000,
                                               help="Higher values capture more entities but increase backend query load.")

# --- Main App Logic ---
if start_date > end_date:
    st.sidebar.error("Error: Start date must be before end date.")
else:
    if st.sidebar.button("Run Analysis"):
        st.markdown("---")

        # 1. Fetch Data via API
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        with st.spinner(f"Fetching data for {start_date_str} to {end_date_str}..."):
            buckets = fetch_data_from_api(start_date_str, end_date_str, ES_INDEX, max_entities_per_day)

        if buckets is not None:
            # 2. Process Data
            with st.spinner("Processing data into time series..."):
                pivot_df = process_data(buckets)

            if pivot_df is not None and not pivot_df.empty:
                st.success(f"Data processed into {pivot_df.shape[0]} days and {pivot_df.shape[1]} unique entities.")

                # 3. Run Analysis (Now only returns Original MK results)
                with st.spinner("Performing Mann-Kendall trend analysis..."):
                    # --- MODIFIED: Unpack only two results ---
                    top_orig_inc, top_orig_dec = run_mk_analysis(
                        pivot_df, top_n_inc, top_n_dec, include_decreasing
                    )
                    # --- END MODIFICATION ---

                st.markdown("---")
                st.header("Analysis Results")

                # --- Display Results (Only Original MK) ---
                # Remove column layout if only showing one set of results
                st.subheader(f"Original MK Test Results")
                st.caption("(Assumes no autocorrelation)")

                # Increasing
                st.markdown(f"**Top {len(top_orig_inc)} Increasing Trends**")
                df_orig_inc = format_results_df(top_orig_inc, "Increasing")
                if not df_orig_inc.empty: st.dataframe(df_orig_inc, use_container_width=True, hide_index=True)
                else: st.write("No significant increasing trends found.")

                # Decreasing (Conditional)
                if include_decreasing:
                    st.markdown(f"**Top {len(top_orig_dec)} Decreasing Trends**")
                    df_orig_dec = format_results_df(top_orig_dec, "Decreasing")
                    if not df_orig_dec.empty: st.dataframe(df_orig_dec, use_container_width=True, hide_index=True)
                    else: st.write("No significant decreasing trends found.")

                # Plot Original MK Results
                plot_trends(pivot_df, top_orig_inc, top_orig_dec if include_decreasing else [],
                            "Original MK Test", max_lines_inc, max_lines_dec)

                # --- REMOVED Hamed-Rao Display Section ---

            elif pivot_df is not None and pivot_df.empty:
                st.warning("Processing resulted in an empty dataset. Cannot run analysis.")
            else:
                st.error("Data processing failed. Cannot run analysis.")

        else:
            st.error("Data fetching failed. Check logs and backend API status.")

    else:
        st.info("Select parameters and click 'Run Analysis' in the sidebar.")

# --- Instructions (Unchanged) ---
st.sidebar.markdown("---")
st.sidebar.markdown(
    """
    **Instructions:**
    1. Ensure backend API and Elasticsearch are running.
    2. Select the desired **date range**.
    3. Optionally, toggle **Include Decreasing Trends**.
    4. Adjust **Top N**, **Max entities to plot**, and **Max entities per day** if needed.
    5. Click **Run Analysis**.
    """
)