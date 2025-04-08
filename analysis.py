import streamlit as st
import pandas as pd
import pymannkendall as mk
import numpy as np
from elasticsearch import Elasticsearch, exceptions
from datetime import datetime, timedelta
import os
import traceback
import plotly.express as px
import pytz # For timezone handling

# --- CONFIG & ES Connection ---
# Get connection details from environment variables or use defaults
ES_HOST = os.environ.get("ES_HOST", "https://localhost:9200") # Still HTTPS usually
ES_USERNAME = os.environ.get("ES_USERNAME", "elastic")
ES_PASSWORD = os.environ.get("ES_PASSWORD", "changeme")
ES_INDEX = os.environ.get("ES_INDEX", "gkg")

# Field containing the entity names
ENTITY_FIELD = "V21AllNames.Name.keyword"
# Field containing the timestamp for filtering and aggregation
DATE_FIELD = "V2ExtrasXML.PubTimestamp" # Adjust if your date field is different

@st.cache_resource # Cache the ES client for efficiency
def get_es_client():
    """Creates and returns the Elasticsearch client instance."""
    st.write(f"Connecting to Elasticsearch at {ES_HOST}...")
    try:
        client = Elasticsearch(
            ES_HOST,
            basic_auth=(ES_USERNAME, ES_PASSWORD),
            verify_certs=False, # Disables certificate verification entirely
        )
        if not client.ping():
            st.error("Connection to Elasticsearch failed. Please check connection details.")
            st.stop()
        st.success("Connected to Elasticsearch successfully!")
        return client
    except exceptions.AuthenticationException:
        st.error("Authentication failed. Please check ES_USERNAME and ES_PASSWORD.")
        st.stop()
    except exceptions.ConnectionError as e:
        st.error(f"Connection error: {e}. Is Elasticsearch running and accessible at {ES_HOST}?")
        st.stop()
    except Exception as e:
        st.error(f"An unexpected error occurred during Elasticsearch connection: {e}")
        traceback.print_exc() # Print detailed error for debugging
        st.stop()

@st.cache_data(ttl=600) # Cache data for 10 minutes
def fetch_data(_es_client, start_date, end_date, entity_field, date_field, index, max_entities=500):
    """Fetches and aggregates data from Elasticsearch."""
    st.write(f"Fetching data from index '{index}' between {start_date} and {end_date}...")
    end_date_es = end_date + timedelta(days=1)
    query = {
        "size": 0,
        "query": {"range": {date_field: {"gte": start_date.strftime("%Y-%m-%d"), "lt": end_date_es.strftime("%Y-%m-%d")}}},
        "aggs": {
            "entities_over_time": {
                "date_histogram": {"field": date_field, "calendar_interval": "1d", "min_doc_count": 0},
                "aggs": {"top_entities": {"terms": {"field": entity_field, "size": max_entities}}}
            }
        }
    }
    try:
        response = _es_client.search(index=index, body=query, request_timeout=120)
        st.write("Data fetched successfully!")
        return response['aggregations']['entities_over_time']['buckets']
    except exceptions.RequestError as e:
         st.error(f"Elasticsearch query failed: {e.info['error']['root_cause'][0]['reason']}")
         return None
    except Exception as e:
        st.error(f"An error occurred during data fetching: {e}")
        traceback.print_exc()
        return None

def process_data(buckets):
    """Processes Elasticsearch aggregation results into a pivoted DataFrame."""
    st.write("Processing data...")
    if not buckets:
        st.warning("No data returned from Elasticsearch for the selected period.")
        return None
    processed_data = []
    for day_bucket in buckets:
        date = pd.to_datetime(day_bucket['key_as_string']).normalize()
        entities_in_bucket = day_bucket.get('top_entities', {}).get('buckets', [])
        for entity_bucket in entities_in_bucket:
            processed_data.append({'Date': date, 'Entity': entity_bucket['key'], 'Count': entity_bucket['doc_count']})
    if not processed_data:
        st.warning("No entity data found within the buckets for the selected period.")
        return None
    df = pd.DataFrame(processed_data)
    try:
        pivot_df = df.pivot_table(index='Date', columns='Entity', values='Count', fill_value=0)
    except Exception as e:
        st.error(f"Failed to pivot data. Error: {e}")
        st.dataframe(df.head())
        return None
    pivot_df = pivot_df.sort_index()
    st.write("Data processed successfully!")
    return pivot_df

# Modified run_mk_analysis to accept separate top_n values
def run_mk_analysis(df, top_n_inc=10, top_n_dec=10, include_decreasing=False):
    """Runs Mann-Kendall tests, filtering for increasing and optionally decreasing trends."""
    st.write("Running Mann-Kendall analysis...")
    if df is None or df.empty:
        st.warning("Cannot run analysis on empty data.")
        return [], [], [], [] # Return four empty lists

    results_orig_inc, results_orig_dec = {}, {}
    results_hamed_inc, results_hamed_dec = {}, {}

    num_entities = len(df.columns)
    progress_bar = st.progress(0)
    status_text = st.empty()

    for i, entity in enumerate(df.columns):
        series = df[entity]
        if series.count() < 3 or series.nunique() < 2:
            progress_bar.progress((i + 1) / num_entities)
            status_text.text(f"Analyzing entity {i+1}/{num_entities}: {entity} (skipped - insufficient data)")
            continue

        status_text.text(f"Analyzing entity {i+1}/{num_entities}: {entity}")

        try:
            # --- Original Test ---
            result_orig = mk.original_test(series)
            if result_orig.h and result_orig.trend == 'increasing' and result_orig.slope > 0:
                results_orig_inc[entity] = result_orig
            elif include_decreasing and result_orig.h and result_orig.trend == 'decreasing' and result_orig.slope < 0:
                 results_orig_dec[entity] = result_orig

            # --- Hamed-Rao Test ---
            if len(series) >= 10:
                result_hamed = mk.hamed_rao_modification_test(series)
                if result_hamed.h and result_hamed.trend == 'increasing' and result_hamed.slope > 0:
                    results_hamed_inc[entity] = result_hamed
                elif include_decreasing and result_hamed.h and result_hamed.trend == 'decreasing' and result_hamed.slope < 0:
                    results_hamed_dec[entity] = result_hamed
            else: pass
        except Exception as e:
            st.warning(f"Could not analyze entity '{entity}'. Error: {e}")

        progress_bar.progress((i + 1) / num_entities)

    progress_bar.empty()
    status_text.text("Analysis complete!")

    # Sort results: increasing by slope descending, decreasing by slope ascending
    sorted_results_orig_inc = sorted(results_orig_inc.items(), key=lambda item: item[1].slope, reverse=True)
    sorted_results_orig_dec = sorted(results_orig_dec.items(), key=lambda item: item[1].slope, reverse=False)
    sorted_results_hamed_inc = sorted(results_hamed_inc.items(), key=lambda item: item[1].slope, reverse=True)
    sorted_results_hamed_dec = sorted(results_hamed_dec.items(), key=lambda item: item[1].slope, reverse=False)

    # Apply separate top_n limits
    return sorted_results_orig_inc[:top_n_inc], sorted_results_orig_dec[:top_n_dec], \
           sorted_results_hamed_inc[:top_n_inc], sorted_results_hamed_dec[:top_n_dec]

def format_results_df(results_list, trend_type):
    """Formats the MK results list into a DataFrame for display."""
    if not results_list:
        return pd.DataFrame()
    return pd.DataFrame([{
        'Rank': i + 1, 'Entity': entity, 'Trend Type': trend_type,
        'MK Slope': f"{result.slope:.4f}", 'Trend': result.trend,
        'Significant (h)': result.h, 'P-value': f"{result.p:.3g}"
    } for i, (entity, result) in enumerate(results_list)])

# Modified plot_trends to accept separate max_lines values
def plot_trends(df_pivot, entities_inc, entities_dec, title, max_lines_inc=5, max_lines_dec=5):
    """Plots the time series for top increasing and decreasing entities."""
    st.subheader(f"Time Series Plot ({title})")

    # Select entities based on separate limits
    entities_to_plot_inc = [entity for entity, _ in entities_inc[:max_lines_inc]]
    entities_to_plot_dec = [entity for entity, _ in entities_dec[:max_lines_dec]]
    entities_to_plot = entities_to_plot_inc + entities_to_plot_dec

    if not entities_to_plot:
        st.write("No significant trends found to plot.")
        return

    # Filter pivot table AFTER identifying entities to plot
    df_plot_wide = df_pivot[[col for col in entities_to_plot if col in df_pivot.columns]] # Ensure columns exist

    if df_plot_wide.empty:
        st.write("Selected entities for plotting not found in processed data.")
        return

    df_plot_long = df_plot_wide.reset_index().melt(id_vars='Date', var_name='Entity', value_name='Daily Count')

    trend_map = {**{entity: 'Increasing' for entity in entities_to_plot_inc},
                 **{entity: 'Decreasing' for entity in entities_to_plot_dec}}
    df_plot_long['Trend Type'] = df_plot_long['Entity'].map(trend_map)

    # Generate plot title dynamically
    plot_title = f"Top Mentions Over Time ({title})"
    if entities_to_plot_inc and entities_to_plot_dec:
        plot_title = f"Top {len(entities_to_plot_inc)} Inc / {len(entities_to_plot_dec)} Dec Mentions ({title})"
    elif entities_to_plot_inc:
         plot_title = f"Top {len(entities_to_plot_inc)} Increasing Mentions ({title})"
    elif entities_to_plot_dec:
         plot_title = f"Top {len(entities_to_plot_dec)} Decreasing Mentions ({title})"


    fig = px.line(df_plot_long, x='Date', y='Daily Count', color='Entity',
                  title=plot_title,
                  labels={'Daily Count': 'Mentions per Day'},
                  markers=True)

    fig.update_layout(legend_title_text='Entity')
    st.plotly_chart(fig, use_container_width=True)


# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title("Elasticsearch GKG Entity Trend Analysis")
st.write(f"Analyzing field `{ENTITY_FIELD}` from index `{ES_INDEX}`.")

# Get ES Client
es_client = get_es_client()

# --- Sidebar Inputs ---
st.sidebar.header("Analysis Parameters")

# Date Range Input
try:
    # Use Asia/Singapore timezone
    singapore_tz = pytz.timezone('Asia/Singapore')
    today = datetime.now(singapore_tz).date()
except ImportError:
    st.sidebar.warning("`pytz` not installed. Using UTC for date defaults.")
    today = datetime.utcnow().date() # Fallback to UTC if pytz not installed
except Exception as e:
     st.sidebar.warning(f"Could not determine Singapore timezone ({e}). Using UTC for date defaults.")
     today = datetime.utcnow().date()

default_start = today - timedelta(days=30)
start_date = st.sidebar.date_input("Start date", default_start)
end_date = st.sidebar.date_input("End date", today)

# Toggle for decreasing trends
include_decreasing = st.sidebar.toggle("Include Decreasing Trends", value=False,
                                      help="Show entities with significant decreasing trends in addition to increasing ones.")

# --- Conditional Inputs ---
top_n_inc = st.sidebar.number_input(f"Top N **Increasing** entities:", min_value=1, max_value=50, value=10)
max_lines_inc = st.sidebar.number_input("Max **Increasing** entities to plot:", min_value=1, max_value=20, value=10)

top_n_dec = 0
max_lines_dec = 0
if include_decreasing:
    st.sidebar.markdown("---") # Separator for decreasing controls
    top_n_dec = st.sidebar.number_input("Top N **Decreasing** entities:", min_value=1, max_value=50, value=10)
    max_lines_dec = st.sidebar.number_input("Max **Decreasing** entities to plot:", min_value=1, max_value=20, value=10)
    st.sidebar.markdown("---") # Separator

# Max entities per day input (always visible)
max_entities_per_day = st.sidebar.number_input("Max unique entities to fetch per day (ES Terms Agg Size):", min_value=10, max_value=20000, value=10000,
                                               help="Higher values capture more entities but increase ES query load. May miss trends if set too low.")


# --- Main App Logic ---
if start_date > end_date:
    st.sidebar.error("Error: Start date must be before end date.")
else:
    if st.sidebar.button("Run Analysis"):
        st.markdown("---") # Separator

        # 1. Fetch Data
        with st.spinner(f"Fetching data for {start_date} to {end_date}..."):
            buckets = fetch_data(es_client, start_date, end_date, ENTITY_FIELD, DATE_FIELD, ES_INDEX, max_entities_per_day)

        if buckets is not None:
            # 2. Process Data
            with st.spinner("Processing data into time series..."):
                pivot_df = process_data(buckets)

            if pivot_df is not None and not pivot_df.empty:
                st.success(f"Data processed into {pivot_df.shape[0]} days and {pivot_df.shape[1]} unique entities.")

                # 3. Run Analysis - Pass separate N values
                with st.spinner("Performing Mann-Kendall trend analysis (this may take a while)..."):
                    top_orig_inc, top_orig_dec, top_hamed_inc, top_hamed_dec = run_mk_analysis(
                        pivot_df, top_n_inc, top_n_dec, include_decreasing # Pass separate N values
                    )

                st.markdown("---")
                st.header("Analysis Results")

                # --- Display Results ---
                col1, col2 = st.columns(2)

                # == Original MK Test Results ==
                with col1:
                    st.subheader(f"Original MK Test Results")
                    st.caption("(Assumes no autocorrelation)")

                    # Increasing
                    st.markdown(f"**Top {len(top_orig_inc)} Increasing Trends**") # Show actual count returned
                    df_orig_inc = format_results_df(top_orig_inc, "Increasing")
                    if not df_orig_inc.empty:
                        st.dataframe(df_orig_inc, use_container_width=True, hide_index=True)
                    else:
                        st.write("No significant increasing trends found.")

                    # Decreasing (Conditional)
                    if include_decreasing:
                        st.markdown(f"**Top {len(top_orig_dec)} Decreasing Trends**") # Show actual count returned
                        df_orig_dec = format_results_df(top_orig_dec, "Decreasing")
                        if not df_orig_dec.empty:
                            st.dataframe(df_orig_dec, use_container_width=True, hide_index=True)
                        else:
                            st.write("No significant decreasing trends found.")

                    # Plot Original MK Results - Pass separate max_lines values
                    plot_trends(pivot_df, top_orig_inc, top_orig_dec if include_decreasing else [],
                                "Original MK Test", max_lines_inc, max_lines_dec)


                # == Hamed-Rao Test Results ==
                with col2:
                    st.subheader(f"Hamed-Rao Test Results")
                    st.caption("(Accounts for autocorrelation)")

                     # Increasing
                    st.markdown(f"**Top {len(top_hamed_inc)} Increasing Trends**") # Show actual count returned
                    df_hamed_inc = format_results_df(top_hamed_inc, "Increasing")
                    if not df_hamed_inc.empty:
                        st.dataframe(df_hamed_inc, use_container_width=True, hide_index=True)
                    else:
                        st.write("No significant increasing trends found.")

                    # Decreasing (Conditional)
                    if include_decreasing:
                        st.markdown(f"**Top {len(top_hamed_dec)} Decreasing Trends**") # Show actual count returned
                        df_hamed_dec = format_results_df(top_hamed_dec, "Decreasing")
                        if not df_hamed_dec.empty:
                            st.dataframe(df_hamed_dec, use_container_width=True, hide_index=True)
                        else:
                            st.write("No significant decreasing trends found.")

                    # Plot Hamed-Rao Results - Pass separate max_lines values
                    plot_trends(pivot_df, top_hamed_inc, top_hamed_dec if include_decreasing else [],
                                "Hamed-Rao Test", max_lines_inc, max_lines_dec)

            elif pivot_df is not None and pivot_df.empty:
                st.warning("Processing resulted in an empty dataset. Cannot run analysis.")
            else:
                st.error("Data processing failed. Cannot run analysis.") # Error message already shown

        else:
            st.error("Data fetching failed. Check logs and Elasticsearch connection/query.")

    else:
        st.info("Select parameters and click 'Run Analysis' in the sidebar.")


# --- Updated Instructions ---
st.sidebar.markdown("---")
st.sidebar.markdown(
    """
    **Instructions:**
    1. Ensure Elasticsearch is running and accessible.
    2. Verify connection details (Host, User, Pass) are correct (or set as environment variables).
    3. Select the desired **date range**.
    4. Optionally, toggle **Include Decreasing Trends**.
    5. Adjust **Top N** (for increasing/decreasing), **Max entities to plot** (for increasing/decreasing), and **Max entities per day** if needed.
    6. Click **Run Analysis**.

    **Note:** Certificate verification is currently disabled (`verify_certs=False`). This is insecure.
    """
)