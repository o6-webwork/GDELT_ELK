# frontend/pages/2_Trend_Analysis.py
# Updated to replicate original analysis.py functionality while using backend API

import streamlit as st
import pandas as pd
import pymannkendall as mk
# import numpy as np # Not used in original after modifications
# from elasticsearch import Elasticsearch, exceptions # Not needed, using API
from datetime import datetime, timedelta
import os
import traceback # Keep for potential error logging
import plotly.express as px
import plotly.graph_objects as go # Import go for plot_trends if needed
import pytz # For timezone handling
import requests # For API calls
import logging # Use logging instead of print for container logs

# --- Configuration ---
BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://backend-api:8000")
# These might be configurable via UI later if needed
ENTITY_FIELD = "V21AllNames.Name.keyword" # Default entity field
DATE_FIELD = "V2ExtrasXML.PubTimestamp" # Default date field
ES_INDEX = "gkg*" # Default index pattern for API call

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- API Data Fetching ---
@st.cache_data(ttl=600) # Cache data for 10 minutes
def fetch_data_from_api(start_date_str: str, end_date_str: str, index: str, max_entities: int):
    """Fetches aggregated entity trends from the FastAPI backend."""
    api_endpoint = f"{BACKEND_API_URL}/trends"
    params = {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "max_entities": max_entities,
        "index": index,
        "entity_field": ENTITY_FIELD, # Use configured entity field
        "date_field": DATE_FIELD,     # Use configured date field
    }
    logger.info(f"Fetching data from API: {api_endpoint} with params: {params}")
    st.write(f"Fetching data for {start_date_str} to {end_date_str} (max {max_entities}/day)...") # User feedback
    try:
        response = requests.get(api_endpoint, params=params, timeout=120) # Increased timeout
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()
        buckets = data.get('buckets', [])
        logger.info(f"Successfully fetched {len(buckets)} buckets from API.")
        st.write("Data fetched successfully!") # User feedback
        return buckets
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")
        st.error(f"Error fetching data from backend API: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during API data fetch: {e}", exc_info=True)
        st.error(f"An unexpected error occurred: {e}")
        return None

# --- Data Processing (closer to original) ---
def process_data(buckets):
    """Processes API aggregation results into a pivoted DataFrame."""
    st.write("Processing data...") # User feedback
    if not buckets:
        st.warning("No data returned from API for the selected period.")
        return None # Return None like original
    processed_data = []
    try:
        for day_bucket in buckets:
            # Use key_as_string for date, normalize to remove time part
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
        return None # Return None like original

    df = pd.DataFrame(processed_data)
    try:
        # Use pivot_table like original, handle potential duplicate entries if any
        pivot_df = df.pivot_table(index='Date', columns='Entity', values='Count', aggfunc='sum', fill_value=0)
    except Exception as e:
        st.error(f"Failed to pivot data. Error: {e}")
        st.dataframe(df.head()) # Show head of intermediate data on error
        return None # Return None like original

    pivot_df = pivot_df.sort_index() # Ensure sorted by date
    st.write("Data processed successfully!") # User feedback
    return pivot_df

# --- MK Analysis (closer to original + DEBUGGING) ---
def run_mk_analysis(df, top_n_inc=10, top_n_dec=10, include_decreasing=False):
    """Runs Mann-Kendall tests, filtering for increasing and optionally decreasing trends."""
    st.write("Running Mann-Kendall analysis...") # User feedback
    if df is None or df.empty:
        st.warning("Cannot run analysis on empty data.")
        return [], [], [], [] # Return four empty lists like original

    results_orig_inc, results_orig_dec = {}, {}
    results_hamed_inc, results_hamed_dec = {}, {}
    debug_hamed_results = {} # Store raw Hamed results for debugging

    num_entities = len(df.columns)
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    status_text.text(f"Analyzing 0/{num_entities} entities...")

    for i, entity in enumerate(df.columns):
        series = df[entity]
        update_text = f"Analyzing entity {i+1}/{num_entities}: {entity}"
        series_length = len(series) # Get length before potential modification by MK tests

        # --- DEBUG: Print Series Length ---
        # Use logger instead of st.write inside loop for less clutter
        logger.info(f"Analyzing '{entity}': Series Length = {series_length}")
        # --- END DEBUG ---

        # Skip analysis if not enough data points like original
        if series.count() < 3 or series.nunique() < 2:
             status_text.text(f"{update_text} (skipped - insufficient data)")
             progress_bar.progress((i + 1) / num_entities)
             continue

        status_text.text(update_text)

        try:
            # --- Original Test ---
            result_orig = mk.original_test(series)
            if result_orig.h and result_orig.trend == 'increasing' and result_orig.slope > 0:
                results_orig_inc[entity] = result_orig
            elif include_decreasing and result_orig.h and result_orig.trend == 'decreasing' and result_orig.slope < 0:
                results_orig_dec[entity] = result_orig

            # --- Hamed-Rao Test ---
            # Check length requirement
            if series_length >= 10: # Check original length
                result_hamed = mk.hamed_rao_modification_test(series)
                # --- DEBUG: Store Raw Hamed Result ---
                debug_hamed_results[entity] = {
                    'trend': result_hamed.trend, 'h': result_hamed.h,
                    'p': result_hamed.p, 'slope': result_hamed.slope
                }
                # --- END DEBUG ---

                # Filter based on significance and slope
                if result_hamed.h and result_hamed.trend == 'increasing' and result_hamed.slope > 0:
                    results_hamed_inc[entity] = result_hamed
                elif include_decreasing and result_hamed.h and result_hamed.trend == 'decreasing' and result_hamed.slope < 0:
                    results_hamed_dec[entity] = result_hamed
            else:
                 # --- DEBUG: Log why Hamed-Rao was skipped ---
                 logger.info(f"Skipping Hamed-Rao for '{entity}': Length ({series_length}) < 10")
                 debug_hamed_results[entity] = {'status': 'skipped (length < 10)'}
                 # --- END DEBUG ---

        except Exception as e:
            logger.warning(f"MK analysis failed for entity '{entity}': {e}", exc_info=True)
            st.warning(f"Could not analyze entity '{entity}'. Error: {e}")

        progress_bar.progress((i + 1) / num_entities)

    progress_bar.empty()
    status_text.text("Analysis complete!")

    # --- DEBUG: Display Raw Hamed-Rao results that didn't make the cut ---
    st.write("--- DEBUG: Raw Hamed-Rao Results (Showing All Attempted) ---")
    skipped_count = sum(1 for r in debug_hamed_results.values() if r.get('status') == 'skipped (length < 10)')
    st.write(f"Entities skipped for Hamed-Rao due to length < 10: {skipped_count}")
    # Display results for entities where test was run but might not have been significant
    hamed_run_results = {k: v for k, v in debug_hamed_results.items() if 'status' not in v}
    if hamed_run_results:
         df_debug_hamed = pd.DataFrame.from_dict(hamed_run_results, orient='index')
         st.dataframe(df_debug_hamed)
    else:
         st.write("No Hamed-Rao tests were executed (likely all series < 10 points).")
    st.write("--- END DEBUG ---")
    # --- END DEBUG ---

    # Sort significant results like original
    sorted_results_orig_inc = sorted(results_orig_inc.items(), key=lambda item: item[1].slope, reverse=True)
    sorted_results_orig_dec = sorted(results_orig_dec.items(), key=lambda item: item[1].slope, reverse=False)
    sorted_results_hamed_inc = sorted(results_hamed_inc.items(), key=lambda item: item[1].slope, reverse=True)
    sorted_results_hamed_dec = sorted(results_hamed_dec.items(), key=lambda item: item[1].slope, reverse=False)

    # Apply separate top_n limits like original
    return sorted_results_orig_inc[:top_n_inc], sorted_results_orig_dec[:top_n_dec], \
           sorted_results_hamed_inc[:top_n_inc], sorted_results_hamed_dec[:top_n_dec]

# --- Formatting Results (from original) ---
def format_results_df(results_list, trend_type):
    """Formats the MK results list into a DataFrame for display."""
    if not results_list:
        return pd.DataFrame() # Return empty DataFrame if no results
    return pd.DataFrame([{
        'Rank': i + 1, 'Entity': entity, 'Trend Type': trend_type,
        'MK Slope': f"{result.slope:.4f}", 'Trend': result.trend,
        'Significant (h)': result.h, 'P-value': f"{result.p:.3g}"
    } for i, (entity, result) in enumerate(results_list)])

# --- Plotting (closer to original) ---
def plot_trends(df_pivot, entities_inc, entities_dec, title, max_lines_inc=5, max_lines_dec=5):
    """Plots the time series for top increasing and decreasing entities."""
    # This function was named plot_trends in the original, let's keep it
    st.subheader(f"Time Series Plot ({title})") # Match original subheader format

    # Select entities based on separate limits like original
    entities_to_plot_inc = [entity for entity, _ in entities_inc[:max_lines_inc]]
    entities_to_plot_dec = [entity for entity, _ in entities_dec[:max_lines_dec]]
    entities_to_plot = entities_to_plot_inc + entities_to_plot_dec

    if not entities_to_plot:
        st.write("No significant trends selected to plot.") # Match original message
        return

    # Filter pivot table for plotting like original
    # Ensure columns exist before trying to select them
    valid_entities_to_plot = [col for col in entities_to_plot if col in df_pivot.columns]
    if not valid_entities_to_plot:
        st.write("Selected entities for plotting not found in processed data.")
        return
    df_plot_wide = df_pivot[valid_entities_to_plot]

    # Original code used plotly express after melting
    df_plot_long = df_plot_wide.reset_index().melt(id_vars='Date', var_name='Entity', value_name='Daily Count')

    # Generate plot title dynamically like original
    inc_count = len(entities_to_plot_inc)
    dec_count = len(entities_to_plot_dec)
    plot_title = f"Entity Mentions Over Time ({title})" # Base title
    if inc_count > 0 and dec_count > 0:
        plot_title = f"Top {inc_count} Inc / {dec_count} Dec Mentions ({title})"
    elif inc_count > 0:
        plot_title = f"Top {inc_count} Increasing Mentions ({title})"
    elif dec_count > 0:
        plot_title = f"Top {dec_count} Decreasing Mentions ({title})"

    # Use Plotly Express like original
    fig = px.line(df_plot_long, x='Date', y='Daily Count', color='Entity',
                  title=plot_title,
                  labels={'Daily Count': 'Mentions per Day'},
                  markers=True)

    fig.update_layout(legend_title_text='Entity') # Match original layout update
    st.plotly_chart(fig, use_container_width=True)


# --- Streamlit UI (closer to original) ---
st.set_page_config(layout="wide", page_title="GDELT Trend Analysis")
st.title("GDELT Entity Trend Analysis (via Backend API)") # Modified title
# st.write(f"Analyzing field `{ENTITY_FIELD}` from index `{ES_INDEX}`.") # Removed ES specific write

# --- Sidebar Inputs (closer to original) ---
st.sidebar.header("Analysis Parameters")

# Date Range Input (using UTC fallback like original if pytz fails)
try:
    singapore_tz = pytz.timezone('Asia/Singapore')
    today = datetime.now(singapore_tz).date()
except Exception as e:
    logger.warning(f"Could not use Singapore timezone ({e}). Using UTC for date defaults.")
    today = datetime.utcnow().date()

default_start = today - timedelta(days=30)
start_date = st.sidebar.date_input("Start date", default_start, key="trend_start_date_orig") # Use different keys
end_date = st.sidebar.date_input("End date", today, key="trend_end_date_orig")

# Toggle for decreasing trends (like original)
include_decreasing = st.sidebar.toggle("Include Decreasing Trends", value=False,
                                       help="Show entities with significant decreasing trends in addition to increasing ones.")

# Conditional Inputs (like original)
top_n_inc = st.sidebar.number_input(f"Top N **Increasing** entities:", min_value=1, max_value=50, value=10)
max_lines_inc = st.sidebar.number_input("Max **Increasing** entities to plot:", min_value=1, max_value=20, value=10)

top_n_dec = 0
max_lines_dec = 0
if include_decreasing:
    st.sidebar.markdown("---") # Separator
    top_n_dec = st.sidebar.number_input("Top N **Decreasing** entities:", min_value=1, max_value=50, value=10)
    max_lines_dec = st.sidebar.number_input("Max **Decreasing** entities to plot:", min_value=1, max_value=20, value=10)
    st.sidebar.markdown("---") # Separator

# Max entities per day input (like original)
max_entities_per_day = st.sidebar.number_input("Max unique entities to fetch per day:", min_value=10, max_value=20000, value=10000,
                                               help="Higher values capture more entities but increase backend query load.")

# --- Main App Logic (Triggered by Button like original) ---
if start_date > end_date:
    st.sidebar.error("Error: Start date must be before end date.")
else:
    # Use button to trigger analysis like original
    if st.sidebar.button("Run Analysis"):
        st.markdown("---") # Separator

        # 1. Fetch Data via API
        # Use strftime explicitly for API compatibility
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        with st.spinner(f"Fetching data for {start_date_str} to {end_date_str}..."):
            # Pass index pattern and max entities per day to API
            buckets = fetch_data_from_api(start_date_str, end_date_str, ES_INDEX, max_entities_per_day)

        if buckets is not None: # Check if fetch succeeded
            # 2. Process Data
            with st.spinner("Processing data into time series..."):
                pivot_df = process_data(buckets)

            if pivot_df is not None and not pivot_df.empty: # Check if process succeeded
                st.success(f"Data processed into {pivot_df.shape[0]} days and {pivot_df.shape[1]} unique entities.")

                # 3. Run Analysis - Pass separate N values like original
                with st.spinner("Performing Mann-Kendall trend analysis (this may take a while)..."):
                    top_orig_inc, top_orig_dec, top_hamed_inc, top_hamed_dec = run_mk_analysis(
                        pivot_df, top_n_inc, top_n_dec, include_decreasing # Pass separate N values
                    )

                st.markdown("---")
                st.header("Analysis Results")

                # --- Display Results (like original) ---
                col1, col2 = st.columns(2)

                # == Original MK Test Results ==
                with col1:
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

                # == Hamed-Rao Test Results ==
                with col2:
                    st.subheader(f"Hamed-Rao Test Results")
                    st.caption("(Accounts for autocorrelation)")
                    # Increasing
                    st.markdown(f"**Top {len(top_hamed_inc)} Increasing Trends**")
                    df_hamed_inc = format_results_df(top_hamed_inc, "Increasing")
                    if not df_hamed_inc.empty: st.dataframe(df_hamed_inc, use_container_width=True, hide_index=True)
                    else: st.write("No significant increasing trends found.")
                    # Decreasing (Conditional)
                    if include_decreasing:
                        st.markdown(f"**Top {len(top_hamed_dec)} Decreasing Trends**")
                        df_hamed_dec = format_results_df(top_hamed_dec, "Decreasing")
                        if not df_hamed_dec.empty: st.dataframe(df_hamed_dec, use_container_width=True, hide_index=True)
                        else: st.write("No significant decreasing trends found.")
                    # Plot Hamed-Rao Results
                    plot_trends(pivot_df, top_hamed_inc, top_hamed_dec if include_decreasing else [],
                                "Hamed-Rao Test", max_lines_inc, max_lines_dec)

            elif pivot_df is not None and pivot_df.empty: # Case where processing returned empty df
                st.warning("Processing resulted in an empty dataset. Cannot run analysis.")
            else: # Case where processing failed (returned None)
                st.error("Data processing failed. Cannot run analysis.") # Error msg already shown by process_data

        else: # Case where fetching failed (returned None)
            st.error("Data fetching failed. Check logs and backend API status.") # Error msg already shown by fetch_data_from_api

    else:
        # Initial state before button press
        st.info("Select parameters and click 'Run Analysis' in the sidebar.")

# --- Instructions (like original) ---
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