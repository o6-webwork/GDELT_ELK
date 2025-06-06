import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import datetime
from streamlit_autorefresh import st_autorefresh
import os
import base64 

BACKEND_URL = "http://reportsui_backend:8000" # backend URL

@st.cache_data # Cache this so it's fetched once
def fetch_param_sets_from_backend():
    try:
        response = requests.get(f"{BACKEND_URL}/api/v1/config/alert_param_sets")
        response.raise_for_status() # Raise an exception for HTTP errors
        return response.json()
    except Exception as e:
        st.error(f"Failed to fetch PARAM_SETS from backend: {e}")
        return None

def acknowledge_alert_in_api(alert_id: int):
    """Acknowledges an alert via the backend API."""
    if not alert_id:
        st.warning("Cannot acknowledge an alert without an ID.")
        return None
    try:
        response = requests.post(f"{BACKEND_URL}/monitoring/alerts/{alert_id}/acknowledge")
        if response.status_code == 200:
            st.success(f"Alert ID {alert_id} acknowledged.")
            return response.json()
        else:
            error_detail = "Unknown error"
            try: error_detail = response.json().get('detail', response.text)
            except: error_detail = response.text
            st.error(f"Failed to acknowledge alert {alert_id} (Status {response.status_code}): {error_detail}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error acknowledging alert {alert_id}: {e}")
        return None

# Function to generate HTML to play sound
def play_alert_sound(sound_file_name="alert.mp3", audio_format="mpeg"): # Path relative to static folder
    """
    Plays an alert sound using an invisible HTML5 audio tag with autoplay,
    embedding the audio as a Base64 data URI.
    """
    try:
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, "static", sound_file_name)

        if not os.path.exists(abs_file_path):
            st.warning(f"Alert sound file not found at: {abs_file_path}")
            return

        with open(abs_file_path, "rb") as f:
            audio_bytes = f.read()
        
        audio_base64 = base64.b64encode(audio_bytes).decode()
        
        # Determine the correct MIME type
        if audio_format == "mpeg" or sound_file_name.lower().endswith(".mp3"):
            mime_type = "audio/mpeg"
        elif audio_format == "wav" or sound_file_name.lower().endswith(".wav"):
            mime_type = "audio/wav"
        elif audio_format == "ogg" or sound_file_name.lower().endswith(".ogg"):
            mime_type = "audio/ogg"
        else:
            st.warning(f"Unsupported audio format for sound: {sound_file_name}. Using 'audio/mpeg' as default.")
            mime_type = "audio/mpeg"

        st.html(
            f"""
                    <audio autoplay="true">
                        <source src="data:{mime_type};base64,{audio_base64}" type="{mime_type}">
                        Your browser does not support the audio element for data URIs.
                    </audio>
            """
        )

    except FileNotFoundError:
        st.warning(f"Alert sound file '{sound_file_name}' not found in static folder when trying to encode.")
    except Exception as e:
        st.warning(f"Could not play alert sound: {e}")
        import traceback
        print(traceback.format_exc())

# --- Helper functions to interact with the backend for monitored tasks ---

def fetch_monitored_tasks_from_api():
    """Fetches the list of active monitored tasks from the backend."""
    try:
        response = requests.get(f"{BACKEND_URL}/monitoring/tasks?active_only=true&limit=100")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching monitored tasks: {e}")
        return []
    except Exception as e:
        st.error(f"Error processing response for monitored tasks: {e}")
        return []

def add_monitored_task_to_api(payload:dict):
    """Adds a new task to be monitored via the backend API."""
    try:
        response = requests.post(f"{BACKEND_URL}/monitoring/tasks", json=payload)
        if response.status_code == 201:  # Created
            st.success(f"Request to monitor query sent: \"{payload.get('query_string')}\"")
            return response.json()
        else:
            error_detail = response.json().get('detail', response.text)
            st.error(f"Failed to add monitoring task: {error_detail}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error adding task: {e}")
        return None

def stop_monitoring_task_in_api(task_id: int):
    """Stops monitoring a task (marks as inactive) via the backend API."""
    try:
        response = requests.delete(f"{BACKEND_URL}/monitoring/tasks/{task_id}")
        if response.status_code == 200:
            st.success(f"Request to stop monitoring task ID: {task_id} sent.")
            return response.json()
        else:
            error_detail = response.json().get('detail', response.text)
            st.error(f"Failed to stop monitoring task: {error_detail}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error stopping task: {e}")
        return None

def perform_one_off_alert_check(user_query_dict, 
                                date_mode_for_one_off, 
                                end_date_str_for_one_off, 
                                interval_str_for_one_off, 
                                specific_alert_timestamp_str=None, 
                                p_task_id=None,
                                p_custom_baseline_window_pd_str=None,
                                p_custom_min_periods_baseline=None,
                                p_custom_min_count_for_alert=None,
                                p_custom_spike_threshold=None,
                                p_custom_build_window_periods_count=None,
                                p_custom_build_threshold=None):
    
    """Performs the one-off alert check and updates session state for its display."""
    payload = {
        "user_query": user_query_dict,
        "date_mode": date_mode_for_one_off, # "Custom Date" or "Live Monitoring"
        "end_date": end_date_str_for_one_off, # Date string or "now"
        "interval": interval_str_for_one_off # "1d" or "15m"
    }
    if specific_alert_timestamp_str: # Add specific timestamp to payload if provided
        payload["specific_alert_timestamp"] = specific_alert_timestamp_str
    if p_custom_baseline_window_pd_str is not None:
        payload["custom_baseline_window_pd_str"] = p_custom_baseline_window_pd_str
    if p_custom_min_periods_baseline is not None:
        payload["custom_min_periods_baseline"] = p_custom_min_periods_baseline
    if p_custom_min_count_for_alert is not None:
        payload["custom_min_count_for_alert"] = p_custom_min_count_for_alert
    if p_custom_spike_threshold is not None:
        payload["custom_spike_threshold"] = p_custom_spike_threshold
    if p_custom_build_window_periods_count is not None:
        payload["custom_build_window_periods_count"] = p_custom_build_window_periods_count
    if p_custom_build_threshold is not None:
        payload["custom_build_threshold"] = p_custom_build_threshold

    if p_task_id is not None:
        payload["task_id"] = p_task_id


    # Store query string to display graph title 
    try:
        # Extract the actual query string
        # The user_query_dict structure is {"query": {"query_string": {"query": "ACTUAL QUERY", "fields": ["*"]}}}
        actual_query_string = user_query_dict.get("query", {}).get("query_string", {}).get("query", "Query not available")
        st.session_state.current_graph_query_string = actual_query_string
    except Exception as e:
        print(f"Error extracting query string: {e}")
        st.session_state.current_graph_query_string = "Query extraction error"

    try:
        with st.spinner("Checking for alerts ..."): # Spinner message more generic
            alerts_response = requests.post(f"{BACKEND_URL}/check_gdelt_alerts", json=payload)
        if alerts_response.status_code == 200:
            api_alert_data = alerts_response.json()
            st.session_state.alert_timeseries_data = api_alert_data.get('timeseries_data', [])
            st.session_state.alert_details_for_rerun = api_alert_data
            st.session_state.current_graph_historical_alerts = api_alert_data.get('historical_alerts_data', []) # Get historical alerts

        else:
            st.session_state.alert_details_for_rerun = {"error": f"❌ Error from backend: {alerts_response.status_code} - {alerts_response.text}"}
            st.session_state.alert_timeseries_data = []
            st.session_state.current_graph_historical_alerts = []
            st.session_state.current_graph_query_string = None
    except requests.exceptions.RequestException as e:
        st.session_state.alert_details_for_rerun = {"error": f"❌ Error connecting to backend: {e}"}
        st.session_state.alert_timeseries_data = []
        st.session_state.current_graph_historical_alerts = []
        st.session_state.current_graph_query_string = None


def show_alert_page():
    st.title("🚨 GDELT Alerting")

    # session state for "More Detail" button 
    if 'show_alert_graph' not in st.session_state:
        st.session_state.show_alert_graph = False
    if 'alert_timeseries_data' not in st.session_state:
        st.session_state.alert_timeseries_data = None
    if 'alert_details_for_rerun' not in st.session_state: # To store alert_data across reruns
        st.session_state.alert_details_for_rerun = None
    if 'monitored_tasks_list' not in st.session_state:
        st.session_state.monitored_tasks_list = fetch_monitored_tasks_from_api()

    # session state variable for sound control
    if 'sound_played_for_alert_instance_id' not in st.session_state:
        st.session_state.sound_played_for_alert_instance_id = None
    if 'sounded_persistent_alert_ids' not in st.session_state: #
        st.session_state.sounded_persistent_alert_ids = set()

    # For storing custom params temporarily before submitting the form
    if 'current_custom_params' not in st.session_state:
        st.session_state.current_custom_params = {}

    # Tracks which mode (Live Monitoring / Custom Date) created the graph
    if 'graph_data_origin_mode' not in st.session_state:
        st.session_state.graph_data_origin_mode = None # Will be "Custom Date" or "Live Monitoring"

    # For storing query string for graph display
    if 'current_graph_query_string' not in st.session_state:
        st.session_state.current_graph_query_string = None
    
    # For storing historical alert for graph display
    if 'current_graph_historical_alerts' not in st.session_state:
        st.session_state.current_graph_historical_alerts = []

    # Callbacks to reset graph visibility
    def reset_graph():
        st.session_state.show_alert_graph = False
        st.session_state.alert_details_for_rerun = None # Also clear previous alert details
        st.session_state.sound_played_for_alert_instance_id = None # Reset sound flag
        st.session_state.sounded_persistent_alert_ids.clear() # Clear the set for persistent alerts
        st.session_state.graph_data_origin_mode = None # Reset graph origin
        st.session_state.current_graph_query_string = None # 
        st.session_state.current_graph_historical_alerts = []

    # --- Date Selection Mode ---
    operation_mode = st.radio(
        "Select Date Mode:",
        ('Live Monitoring', 'Custom Date'),
        horizontal=True,
        key="operation_mode_radio",
        on_change=reset_graph
    )

    current_operation_mode = st.session_state.get("operation_mode_radio", "Custom Date") # Get current mode

    # --- Clear graph data if mode is inconsistent with graph origin ---
    if st.session_state.get('show_alert_graph', False): # If a graph *thinks* it should be shown
        if st.session_state.graph_data_origin_mode and \
           st.session_state.graph_data_origin_mode != current_operation_mode:
            print(f"ALERT_PAGE: Stale graph detected. Origin: {st.session_state.graph_data_origin_mode}, Current Mode: {current_operation_mode}. Resetting graph.")
            reset_graph() # Call reset_graph to clear everything including show_alert_graph


    if operation_mode == 'Custom Date': # Label reflects its new fixed nature
        st.markdown("#### Perform a check for a Specific Date (Daily Interval)")

        st.session_state.new_alert_processed_by_autorefresh = False 

        # UI for the ad-hoc check is now simpler
        col_date_input, col_query_input, col_button = st.columns([1,2,1]) # Adjusted column ratios
        
        with col_date_input:
            selected_end_date = st.date_input(
                "Select End Date for Check", datetime.date.today(),
                key="one_off_custom_end_date", on_change=reset_graph,
                label_visibility="collapsed"
            )
        
        end_date_str_one_off = selected_end_date.strftime("%Y-%m-%d")

        # For this mode, date_mode is always "Custom Date" and interval is "1d"
        date_mode_for_payload = "Custom Date" 
        interval_one_off = "1d"

        with col_query_input:
            query_string_one_off = st.text_input(
                "Enter Query String for Ad-hoc Check:",
                placeholder="Enter Query: e.g. Trump AND China ",
                key="one_off_query_string", 
                on_change=reset_graph,
                label_visibility="collapsed"
            )
        
        with col_button:
            adhoc_check_button_pressed = st.button("Search", key="execute_one_off_check_button", use_container_width=True, type="primary")
        
        one_off_alert_placeholder = st.empty() 

        if adhoc_check_button_pressed: 
            reset_graph() # Use updated callback
            if not query_string_one_off: 
                one_off_alert_placeholder.warning("Please enter a query string") #
            else:
                user_query = {"query": {"query_string": {"query": query_string_one_off, "fields": ["*"]}}} #
                perform_one_off_alert_check(user_query, 
                                            date_mode_for_payload, 
                                            end_date_str_one_off, 
                                            interval_one_off) 
                # If graph data is generated by perform_one_off_alert_check and intended to be shown:
                if st.session_state.alert_timeseries_data: # Check if data was populated
                    st.session_state.graph_data_origin_mode = "Custom Date" # Set origin

        # Display results for One-Off Alert Check
        if st.session_state.alert_details_for_rerun:
            current_alert_data = st.session_state.alert_details_for_rerun
            with one_off_alert_placeholder.container():
                if current_alert_data.get("error"):
                    st.error(current_alert_data.get("error"))
                elif current_alert_data.get('alert_triggered') is True:
                    st.error("🔥 ALERT TRIGGERED!")
                    st.write(f"**Timestamp:** {current_alert_data.get('timestamp', 'N/A')}")
                    st.write(f"**Type:** {current_alert_data.get('alert_type', 'Unknown')}")
                    st.write(f"**Reason:** {current_alert_data.get('reason', 'No specific reason provided.')}")

                    # Play sound for alert 
                    # Use a combination of current inputs to create unique ID for this ad-hoc alert event
                    adhoc_alert_instance_id = f"adhoc_{date_mode_for_payload}_{end_date_str_one_off}_{interval_one_off}_{query_string_one_off}_{current_alert_data.get('timestamp')}"
                    if st.session_state.sound_played_for_alert_instance_id != adhoc_alert_instance_id:
                        play_alert_sound("alert.mp3") # Path to your sound file in static folder
                        st.session_state.sound_played_for_alert_instance_id = adhoc_alert_instance_id

                    if st.button("More Detail", key="one_off_alert_more_detail"):
                        st.session_state.show_alert_graph = not st.session_state.show_alert_graph
                elif current_alert_data: 
                    st.info(f"ℹ️ Status: {current_alert_data.get('message', 'No alerts triggered.')}")
                    metrics_to_show = {k: v for k, v in current_alert_data.items() if k in ['count', 'baseline_mean', 'baseline_std', 'z_score', 'build_ratio', 'extended_build_ratio'] and pd.notna(v)}
                    if metrics_to_show:
                        st.write("Calculated Metrics (No Threshold Breach):")
                        st.json({k: (f"{v:.2f}" if isinstance(v, float) else v) for k,v in metrics_to_show.items()})


    elif operation_mode == 'Live Monitoring': # Label reflects its interval
        st.markdown("#### :red[Live Monitoring] 🔴")

        default_param = fetch_param_sets_from_backend() 

        # --- AUTO-REFRESH FOR THIS SECTION ---
        # Refresh every 60 seconds (for example) to get latest statuses from backend poller
        # The backend polling service itself runs every 15 mins per task.
        # This frontend refresh is just to update the display with what the backend has found.
        refresh_interval_ms = 60 * 1000 # 60 seconds
        st_autorefresh(interval=refresh_interval_ms, key="persistent_monitoring_refresher")
        # When st_autorefresh triggers a rerun, it will call fetch_monitored_tasks_from_api() below.

        # Form for adding a new monitored query
        with st.form(key="add_new_monitoring_task_form"):
            st.subheader("Add New Query")
            
            col_query_input_live, col_submit_button_live = st.columns([4, 1])

            with col_query_input_live:
                new_query_to_monitor = st.text_input( #
                    "Query String to Monitor: e.g. Singapore AND Trump", 
                    key="new_query_input", 
                    placeholder="Query String to Monitor: e.g. Singapore AND Trump",
                    label_visibility='collapsed'
                )

            # Popover for Custom Parameters
            with st.popover("🔧 Customize Alert Parameters (Optional)"):
                st.markdown("**Override default parameters for this query:**")
                # Store popover inputs temporarily in session state before form submission
                
                # These assignments directly update session state if the widgets change
                st.session_state.current_custom_params['interval_minutes'] = st.number_input(
                    "Monitoring Interval (minutes):", 
                    min_value=1, value=st.session_state.current_custom_params.get('interval_minutes', 15), step=1,
                    help="How often the backend should check this query. Default: 15 minutes.",
                    key="popover_interval_minutes" # Add unique keys to popover widgets
                )
                st.session_state.current_custom_params['custom_baseline_window_pd_str'] = st.text_input(
                    "Baseline Window (e.g., 1d, 7d):", 
                    value=st.session_state.current_custom_params.get('custom_baseline_window_pd_str', '1d'),
                    placeholder="Default: 1d",
                    key="popover_baseline_window"
                )
                st.session_state.current_custom_params['custom_min_periods_baseline'] = st.number_input(
                    "Min Periods for Baseline:", min_value=1, value=st.session_state.current_custom_params.get('custom_min_periods_baseline'), step=1, placeholder="Default: 12",
                    key="popover_min_periods"
                )
                st.session_state.current_custom_params['custom_min_count_for_alert'] = st.number_input(
                    "Min Count for Alert:", min_value=0, value=st.session_state.current_custom_params.get('custom_min_count_for_alert'), step=1, placeholder="Default: 1",
                    key="popover_min_count"
                )
                st.markdown("###### Spike Detection (Z-Score):")
                st.session_state.current_custom_params['custom_spike_threshold'] = st.number_input(
                    "Spike Threshold (Z-score):", min_value=0.0, value=st.session_state.current_custom_params.get('custom_spike_threshold'), step=0.1, format="%.1f", placeholder="Default: 2.5",
                    key="popover_spike_thresh"
                )
                st.markdown("###### Build Detection:")
                st.session_state.current_custom_params['custom_build_window_periods_count'] = st.number_input(
                    "Build Window (No. of Intervals):", min_value=1, value=st.session_state.current_custom_params.get('custom_build_window_periods_count'), step=1, placeholder="Default: 8",
                    key="popover_build_window"
                )
                st.session_state.current_custom_params['custom_build_threshold'] = st.number_input(
                    "Build Threshold (Ratio):", min_value=0.0, value=st.session_state.current_custom_params.get('custom_build_threshold'), step=0.1, format="%.1f", placeholder="Default: 2.2",
                    key="popover_build_thresh"
                )

            # Form submit button
            with col_submit_button_live:
                submitted_new_task_form = st.form_submit_button("Start Monitoring", type="primary")

        if submitted_new_task_form:
            if new_query_to_monitor:
                payload = {
                    "query_string": new_query_to_monitor,
                    # Always send interval_minutes, defaulting to 15 if not customized
                    "interval_minutes": st.session_state.current_custom_params.get('interval_minutes', 15) 
                }
                # Add other custom parameters to payload if they have values
                if st.session_state.current_custom_params.get('custom_baseline_window_pd_str',''): # Check for non-empty string
                    payload["custom_baseline_window_pd_str"] = st.session_state.current_custom_params['custom_baseline_window_pd_str']
                for p_key in ['custom_min_periods_baseline', 'custom_min_count_for_alert', 
                              'custom_spike_threshold', 'custom_build_window_periods_count', 
                              'custom_build_threshold']:
                    if st.session_state.current_custom_params.get(p_key) is not None:
                        payload[p_key] = st.session_state.current_custom_params[p_key]

                st.write(payload)
                
                result = add_monitored_task_to_api(payload) # Pass the full payload
                if result:
                    st.session_state.monitored_tasks_list = fetch_monitored_tasks_from_api()
                    st.session_state.sound_played_for_alert_instance_id = None 
                    st.session_state.current_custom_params = {} # Reset custom params after submission
                    st.rerun() 
            else:
                st.warning("Please enter a query string to monitor.")

        st.markdown("---")
        if st.button("Refresh Monitored Tasks List & Statuses", key="refresh_monitored_list_button"):
            st.session_state.monitored_tasks_list = fetch_monitored_tasks_from_api()
            st.session_state.sound_played_for_alert_instance_id = None # Reset sound flag as we are getting fresh statuses
            st.session_state.sounded_persistent_alert_ids.clear() # Clear the set
            st.session_state.show_alert_graph = False #
            st.rerun()

        # --- Fetch tasks on each rerun when in this mode (due to autorefresh) ---
        # This ensures the list is up-to-date when st_autorefresh causes a rerun.
        # We only fetch if the list is not already populated by a button click in this same run.
        # A more sophisticated approach might check timestamps if performance becomes an issue.
        st.session_state.monitored_tasks_list = fetch_monitored_tasks_from_api()

        if not st.session_state.monitored_tasks_list:
            st.caption("No queries are currently being persistently monitored.")
        else:
            st.markdown(f"**Currently Monitoring {len(st.session_state.monitored_tasks_list)} Queries:**")
            
            for i, task in enumerate(st.session_state.monitored_tasks_list):
                task_id = task['id']
                last_checked_display = "Never"
                if task.get('last_checked_at'):
                    try:
                        utc_dt = pd.to_datetime(task['last_checked_at'])
                        sgt_dt = utc_dt.tz_convert('Asia/Singapore')
                        last_checked_display = sgt_dt.strftime('%Y-%m-%d %H:%M:%S SGT')
                    except Exception:
                        last_checked_display = task['last_checked_at'] 

                latest_alert = task.get('latest_alert') # This field should now come from your backend API
                status_color = "green"
                status_text = "OK                                                                                                              "
                alert_details_line = "" # For storing specific alert details for the title
                alert_timestamp_display = "" 
                is_alert_acknowledged = False
                           

                if latest_alert and latest_alert.get('alert_type'):
                    status_color = "red"
                    status_text = f"ALERT: {latest_alert.get('alert_type', 'Unknown')}"
                    alert_ts_display = "N/A"
                    if latest_alert.get('alert_timestamp'):
                        try:
                            alert_dt = pd.to_datetime(latest_alert.get('alert_timestamp')).tz_convert('Asia/Singapore')
                            alert_ts_display = alert_dt.strftime('%Y-%m-%d %H:%M SGT')
                        except:
                            alert_ts_display = latest_alert.get('alert_timestamp')
                    alert_details_line = f"— {latest_alert.get('reason', 'N/A')} at {alert_ts_display}"


                    is_alert_acknowledged = latest_alert.get('is_acknowledged', False) # Get acknowledgment status
                    if is_alert_acknowledged:
                        status_color = "orange" # Or some other color for acknowledged alerts
                        status_text += " (Acknowledged)"
                    else:
                        persistent_alert_instance_id = f"persistent_{task_id}_{latest_alert.get('alert_timestamp')}"
                        if persistent_alert_instance_id not in st.session_state.sounded_persistent_alert_ids:
                            play_alert_sound("alert.mp3")
                            st.toast(f"***Alert for query '{task['query_string'][:30]}'***", icon="🔔")
                            st.session_state.sounded_persistent_alert_ids.add(persistent_alert_instance_id)
                    
                elif task.get('is_active') is False:
                    status_text = "Inactive"
                    status_color = "grey"


                expander_title = (
                    f"Query: `{task['query_string']}` | Status: :{status_color}[{status_text}]"
                    f"{alert_details_line} | Last Checked: {last_checked_display})"
                )  

                with st.expander(expander_title, expanded=False):
                    st.write(f"Task ID: {task_id}")
                    st.write(f"Monitored since: {pd.to_datetime(task['created_at']).strftime('%Y-%m-%d %H:%M:%S %Z')}")

                    var_list = [['custom_baseline_window_pd_str', 'BASELINE_WINDOW'], 
                                ['custom_build_window_periods_count', 'BUILD_WINDOW_PERIODS_COUNT']]
                    
                    # populate dictionary with either custom or default (if custom is None) parameters 
                    expander_detail = {}
                    for custom, default in var_list:
                        if task.get(custom) is None:
                            expander_detail[default] = default_param.get('15m').get('BASELINE_WINDOW')
                        else:
                            expander_detail[default] = task.get(custom)
                    st.write(f"  - **Interval:** {task.get('interval_minutes')} minutes") #
                    st.write(f"  - **Baseline Window:** {expander_detail.get('BASELINE_WINDOW')}") #
                    st.write(f"  - **Build Period:** {expander_detail.get('BUILD_WINDOW_PERIODS_COUNT')}") #


                    if latest_alert and latest_alert.get('alert_type'): #
                        #st.markdown("**Latest Alert Details (from backend polling):**") #
                        #st.write(f"  - **Alert ID:** {latest_alert.get('id', 'N/A')}") # Display alert_id
                        # ... (display other latest_alert details as before) ...
                        count_val = latest_alert.get('count') #
                        if count_val is not None: st.write(f"  - **Count:** {count_val}") #
                        # ...

                        # Buttons for Alert: Acknowledge and View Graph
                        col_ack, col_graph, col_stop_mon, col_spacer = st.columns([1, 1, 1, 4])
                        with col_ack:
                            if not is_alert_acknowledged: # Show Acknowledge button only if not yet acknowledged
                                if st.button("Acknowledge Alert", key=f"ack_alert_{latest_alert.get('id')}_{task_id}", type="primary", use_container_width=True):
                                    ack_result = acknowledge_alert_in_api(latest_alert.get('id'))
                                    if ack_result:
                                        # Refresh list to show updated acknowledgment status
                                        st.session_state.monitored_tasks_list = fetch_monitored_tasks_from_api()
                                        st.session_state.sound_played_for_alert_instance_id = None # Allow sound for next new alert
                                        st.rerun()
                            else:
                                st.success("Alert Acknowledged ✓")       

                        with col_graph:
                            if st.button(f"📊 View Graph", key=f"more_detail_monitored_{task_id}", type="secondary", use_container_width=True): #
                                user_query_for_alert_graph = {"query": {"query_string": {"query": task['query_string'], "fields": ["*"]}}} #

                                # Get the original alert timestamp string (should be ISO format or similar from backend)
                                alert_specific_ts_str = latest_alert.get('alert_timestamp')
                                st.write(f"alert timestamp: {alert_specific_ts_str}")

                                alert_end_time_obj = pd.to_datetime(latest_alert.get('alert_timestamp')).tz_convert('Asia/Singapore')
                                alert_end_date_str = alert_end_time_obj.strftime("%Y-%m-%d") 
                                perform_one_off_alert_check(user_query_for_alert_graph, 
                                                            "Custom Date", 
                                                            alert_end_date_str, 
                                                            f"{task['interval_minutes']}m", 
                                                            specific_alert_timestamp_str=alert_specific_ts_str,
                                                            p_task_id = task_id,
                                                            p_custom_baseline_window_pd_str=task.get("custom_baseline_window_pd_str"),
                                                            p_custom_min_periods_baseline=task.get("custom_min_periods_baseline"),
                                                            p_custom_min_count_for_alert=task.get("custom_min_count_for_alert"),
                                                            p_custom_spike_threshold=task.get("custom_spike_threshold"),
                                                            p_custom_build_window_periods_count=task.get("custom_build_window_periods_count"),
                                                            p_custom_build_threshold=task.get("custom_build_threshold")) 
                                if st.session_state.alert_details_for_rerun and st.session_state.alert_timeseries_data: #
                                    st.session_state.show_alert_graph = True #
                                    st.session_state.graph_data_origin_mode = "Live Monitoring" # Set origin
                                st.rerun() # 

                        with col_stop_mon:
                             if st.button("🚫 Stop Monitoring", key=f"stop_monitoring_{task_id}", type="secondary", use_container_width=True): #
                                stop_monitoring_task_in_api(task_id) #
                                st.session_state.monitored_tasks_list = fetch_monitored_tasks_from_api() #
                                st.rerun() 

                    else: # Case where latest_alert or alert_type is None (no active alert to acknowledge/graph)
                        st.caption(":green-background[No alert generated from the latest check.]") #
                        # Still show stop monitoring if no alert
                        if st.button("Stop Monitoring", key=f"stop_monitoring_no_alert_{task_id}", type="secondary"): # Renamed key for uniqueness
                            stop_monitoring_task_in_api(task_id) #
                            st.session_state.monitored_tasks_list = fetch_monitored_tasks_from_api() #
                            st.rerun() #

    # For graph display
    if st.session_state.show_alert_graph:
        if st.session_state.alert_timeseries_data:
            st.markdown("---")
            query_for_display = st.session_state.get('current_graph_query_string', "Selected Query")
            st.subheader(f"Counts Over Time for: `{query_for_display}`")
            try:
                graph_df = pd.DataFrame(st.session_state.alert_timeseries_data)
                if not graph_df.empty and 'timestamp' in graph_df.columns and 'count' in graph_df.columns:
                    graph_df['timestamp'] = pd.to_datetime(graph_df['timestamp'])
                    graph_df = graph_df.sort_values(by='timestamp')
                    fig = px.line(graph_df, x='timestamp', y='count', title='Event Counts', markers=True)
                    fig.update_layout(xaxis_title="Timestamp", yaxis_title="Count")

                    # plot latest alert 
                    primary_alert_details = st.session_state.get("alert_details_for_rerun")
                    primary_alert_ts_to_skip_in_historical = None
                    if primary_alert_details and primary_alert_details.get("alert_triggered") is True:
                        pa_timestamp_str = primary_alert_details.get("timestamp")
                        pa_metrics = primary_alert_details.get("metrics", {})
                        pa_count = pa_metrics.get("count")
                        pa_type = primary_alert_details.get("alert_type", "Alert")
                        if pa_timestamp_str and pa_count is not None:
                            pa_dt = pd.to_datetime(pa_timestamp_str)
                            primary_alert_ts_to_skip_in_historical = pa_dt
                            fig.add_trace(go.Scatter(
                                x=[pa_dt], y=[pa_count], mode='markers',
                                marker=dict(color='red', size=15, symbol='circle', line=dict(width=1,color='black')),
                                name=f'Latest Event: {pa_type}', hoverinfo='text',
                                text=f"<b>{pa_type}</b><br>Time: {pa_dt.strftime('%Y-%m-%d %H:%M:%S')}<br>Count: {pa_count}<br>Reason: {primary_alert_details.get('reason', '')}"
                            ))

                    # plot historical alerts
                    historical_alerts = st.session_state.get("current_graph_historical_alerts", [])
                    if historical_alerts:

                        # Group historical alerts by type and collect their data points
                        historical_points_by_type = {} 
                        # Format: {"Spike": {"x": [dt1, dt2], "y": [c1, c2], "text": [t1, t2]}, ...}

                    
                        for hist_alert in historical_alerts:
                            hist_ts_str = hist_alert.get("alert_timestamp") # Check key from your historical alert objects
                            hist_type = hist_alert.get("alert_type", "Past Event")
                            # You might need to get count from hist_alert.get('count') or hist_alert.get('metrics', {}).get('count')
                            # For this example, I'll assume hist_alert has 'count' directly or you look it up in graph_df
                            hist_count_val = hist_alert.get('count') # Assuming historical alerts from DB contain the count
                            hist_dt = pd.to_datetime(hist_ts_str).tz_convert('Asia/Singapore')
                            if hist_dt == primary_alert_ts_to_skip_in_historical: # Avoid re-plotting primary alert
                                continue

                            if hist_dt and hist_count_val is not None:
                                try: 
                                    if hist_type not in historical_points_by_type:
                                        historical_points_by_type[hist_type] = {"x": [], "y": [], "text": []}
                                    
                                    historical_points_by_type[hist_type]["x"].append(hist_dt)
                                    historical_points_by_type[hist_type]["y"].append(hist_count_val)
                                    historical_points_by_type[hist_type]["text"].append(
                                        f"<b>{hist_type} (Past)</b><br>Time: {hist_dt.strftime('%Y-%m-%d %H:%M:%S SGT')}<br>Count: {hist_count_val}"
                                    )
                                except Exception as e:
                                    st.warning(f"Could not process/plot historical alert at {hist_ts_str}: {e}")

                        # different colours for different alert type. for legend.
                        marker_colors = ['orange', 'green', 'purple']

                        for i, (alert_type, data_points) in enumerate(historical_points_by_type.items()):
                            if data_points["x"]: 
                                fig.add_trace(go.Scatter(
                                    x=data_points["x"],
                                    y=data_points["y"],
                                    mode='markers',
                                    marker=dict(
                                        color=marker_colors[i % len(marker_colors)], # Cycle through colors
                                        size=10, 
                                        symbol='x-thin-open', # Cycle through symbols
                                        line=dict(width=2)
                                    ),
                                    name=f'{alert_type}', # This creates grouped legend items
                                    text=data_points["text"],
                                    hoverinfo='text'
                                ))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Time series data for graph is empty or not in the expected format.")
            except Exception as e:
                st.error(f"Error generating graph: {e}")
        elif st.session_state.show_alert_graph:
             st.warning("No time series data available for graph.")

if __name__ == "__main__":
    show_alert_page()