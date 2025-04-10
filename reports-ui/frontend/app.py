import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="📈 Entity Trends Dashboard", layout="wide", initial_sidebar_state="expanded")
BACKEND_URL = "http://localhost:8000" 
st.markdown("### 📊 Entity Mentions Trend and Time Series Dashboard")

col = st.columns((2, 4.5, 4.5), gap="small")

# Column 1 – Metrics and Filters
with col[0]:
    st.markdown("#### 🚦 Top Trend Metrics")
    st.markdown("##### 🔍 Filters")

    start_date = st.date_input("Start Date", pd.to_datetime("2025-03-25"))
    end_date = st.date_input("End Date", pd.to_datetime("2025-04-09"))
    
    fields_response = requests.get(f"{BACKEND_URL}/fields")
    
    if fields_response.status_code == 200:
        fields = fields_response.json()
        entity_field = st.selectbox("Select Entity Field", fields)
    else:
        st.error("❌ Failed to fetch fields from Elasticsearch.")
        entity_field = None  
    
    trend_type = st.selectbox("Trend Type", ["increasing", "decreasing"])
    top_n = st.slider("Top N Trends", min_value=1, max_value=50, value=10)
    show_significant_only = st.checkbox("Show only statistically significant trends (p < 0.05)", value=True)

    fetch_data = st.button("🚀 Fetch Trends and Time Series")

# Column 2 – Trends Visualization
with col[1]:
    if fetch_data and entity_field:
        trends_response = requests.get(
            f"{BACKEND_URL}/trends",
            params={
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "trend_type": trend_type,
                "top_n": top_n,
                "entity_field": entity_field 
            }
        )

        if trends_response.status_code == 200:
            trend_data = trends_response.json()
            
            # Ensure the data is in list of dictionaries format
            if isinstance(trend_data, dict):  # in case we got a single dictionary
                trend_data = [trend_data]
                
            trend_df = pd.DataFrame(trend_data)

            st.markdown(f"#### 📈 Top {top_n} {trend_type.capitalize()} Trends")

            if not trend_df.empty:
                # Filter by significance if checkbox is on
                if show_significant_only:
                    trend_df = trend_df[trend_df["p"] < 0.05]

                if trend_df.empty:
                    st.warning("⚠️ No statistically significant trends found.")
                else:
                    st.dataframe(trend_df, use_container_width=True)

                    with st.container():
                        fig, ax = plt.subplots(figsize=(12, 6))
                        ax.bar(trend_df['entity'], trend_df['trend strength'],
                               color='green' if trend_type == "increasing" else 'red')
                        ax.set_xlabel("Entities")
                        ax.set_ylabel("Mann-Kendall Tau")
                        ax.set_title("Top Entity Trends")
                        
                        # Set ticks and labels explicitly to avoid warnings
                        ax.set_xticks(range(len(trend_df['entity'])))
                        ax.set_xticklabels(trend_df['entity'], rotation=45, ha='right')
                        
                        st.pyplot(fig, use_container_width=True)
            else:
                st.warning("⚠️ No trends data available for the selected filters.")
        else:
            st.error("❌ Failed to fetch trends data.")

# Column 3 – Time Series
with col[2]:
    if fetch_data and entity_field:
        time_series_response = requests.get(
            f"{BACKEND_URL}/timeseries",
            params={
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "entities": "",  
                "top_n": top_n,
                "entity_field": entity_field  
            }
        )

        if time_series_response.status_code == 200:
            timeseries_data = time_series_response.json()

            # Ensure timeseries data is in list of dictionaries format
            if isinstance(timeseries_data, dict):  # in case we got a single dictionary
                timeseries_data = [timeseries_data]
            
            timeseries_df = pd.DataFrame(timeseries_data)
            st.markdown("#### ⏱️ Time Series")

            if not timeseries_df.empty:
                st.dataframe(timeseries_df, use_container_width=True)

                with st.container():
                    fig, ax = plt.subplots(figsize=(12, 6))
                    for entity in timeseries_df.columns[1:]:
                        ax.plot(timeseries_df['date'], timeseries_df[entity], label=entity)

                    ax.set_title("Mentions Over Time")
                    ax.set_xlabel("Date")
                    ax.set_ylabel("Mentions")
                    ax.legend()
                    plt.xticks(rotation=90)
                    st.pyplot(fig, use_container_width=True)
        else:
            st.error("❌ Failed to fetch time series data.")
