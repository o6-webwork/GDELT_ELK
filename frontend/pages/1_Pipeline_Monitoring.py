# frontend/pages/1_Pipeline_Monitoring.py

import streamlit as st
import requests
import time
import os
from datetime import date, timedelta

# --- Configuration ---
BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://backend-api:8000")
DEFAULT_LOOKBACK_DAYS = 3
POLL_INTERVAL_SECONDS = 3  # How often to check task status

# Initialize session state variables if they don't exist
if 'task_id' not in st.session_state:
    st.session_state['task_id'] = None
if 'task_status_data' not in st.session_state:
    st.session_state['task_status_data'] = None
if 'polling_active' not in st.session_state:
    st.session_state['polling_active'] = False

# --- API Call Functions ---

def start_task(endpoint: str, payload: dict) -> dict | None:
    """Calls the backend API to start a task."""
    url = f"{BACKEND_API_URL}{endpoint}"
    try:
        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error starting task: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None

def get_task_status(task_id: str) -> dict | None:
    """Calls the backend API to get task status."""
    if not task_id:
        return None
    url = f"{BACKEND_API_URL}/api/v1/tasks/status/{task_id}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 404:
            st.warning(f"Task {task_id} not found (may have expired or failed).")
            return None
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching task status: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None

def cancel_task(task_id: str) -> dict | None:
    """Calls the backend API to cancel a task."""
    if not task_id:
        return None
    url = f"{BACKEND_API_URL}/api/v1/tasks/cancel/{task_id}"
    try:
        response = requests.post(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error cancelling task: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None

def fetch_backend_status():
    """Fetches API and Elasticsearch status from backend."""
    url = f"{BACKEND_API_URL}/status"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException:
        return {"api_status": "error", "elasticsearch_status": "error"}
    except Exception:
         return {"api_status": "error", "elasticsearch_status": "error"}

def fetch_logs(log_name="gdelt_etl", lines=50):
    """Fetches logs from backend."""
    url = f"{BACKEND_API_URL}/logs/{log_name}"
    try:
        response = requests.get(url, params={"lines": lines}, timeout=15)
        response.raise_for_status()
        return response.json().get("lines", [])
    except requests.exceptions.RequestException:
        return [f"Error fetching {log_name} logs."]
    except Exception:
        return [f"Error parsing {log_name} logs."]

# --- Streamlit UI ---

st.set_page_config(layout="wide", page_title="Pipeline Monitoring")
st.title("📊 Pipeline Monitoring & Control")

# --- General Status Sidebar ---
st.sidebar.title("System Status")
status_data = fetch_backend_status()
api_status = status_data.get("api_status", "unknown").capitalize()
es_status = status_data.get("elasticsearch_status", "unknown").capitalize()

# Use appropriate color based on status
api_color = "green" if api_status == "Ok" else ("orange" if api_status == "Unknown" else "red")
es_color = "green" if es_status == "Ok" else ("orange" if es_status == "Unknown" else "red")

st.sidebar.markdown(f"**Backend API:** <span style='color:{api_color};'>{api_status}</span>", unsafe_allow_html=True)
st.sidebar.markdown(f"**Elasticsearch:** <span style='color:{es_color};'>{es_status}</span>", unsafe_allow_html=True)

with st.sidebar.expander("ETL Logs (Last 50 lines)"):
    etl_logs = fetch_logs("gdelt_etl", 50)
    st.code("\n".join(etl_logs), language="log")

with st.sidebar.expander("Logstash Logs (Last 50 lines)"):
    ls_logs = fetch_logs("logstash-plain", 50)  # Assuming log file name is 'logstash-plain'
    st.code("\n".join(ls_logs), language="log")

# --- Task Control Area ---
st.subheader("🔧 Manual Tasks")

col1, col2 = st.columns(2)

# Patching Task
with col1:
    st.markdown("**Patch Recent Data**")
    lookback_days = st.number_input("Days to look back:", min_value=1, value=DEFAULT_LOOKBACK_DAYS, step=1, key="patch_days")
    if st.button("Start Patching Task", key="start_patch", disabled=st.session_state.get('polling_active', False)):
        st.session_state['task_id'] = None  # Reset previous task
        st.session_state['task_status_data'] = None
        payload = {"look_back_days": lookback_days}
        result = start_task("/api/v1/tasks/patch", payload)
        if result and result.get("task_id"):
            st.session_state['task_id'] = result["task_id"]
            st.session_state['polling_active'] = True
            st.success(f"Patching task started successfully! Task ID: {st.session_state['task_id']}")
            st.rerun()  # Rerun to start polling display
        else:
            st.error("Failed to start patching task.")

# Archive Task
with col2:
    st.markdown("**Download Archive Data**")
    default_end = date.today() - timedelta(days=1)
    default_start = default_end - timedelta(days=6)
    archive_start_date = st.date_input("Start Date", value=default_start, max_value=default_end, key="archive_start")
    archive_end_date = st.date_input("End Date", value=default_end, min_value=archive_start_date, max_value=default_end, key="archive_end")
    if st.button("Start Archive Download Task", key="start_archive", disabled=st.session_state.get('polling_active', False)):
        if archive_start_date > archive_end_date:
            st.warning("End date must be on or after start date.")
        else:
            st.session_state['task_id'] = None  # Reset previous task
            st.session_state['task_status_data'] = None
            payload = {
                "start_date": archive_start_date.strftime("%Y-%m-%d"),
                "end_date": archive_end_date.strftime("%Y-%m-%d")
            }
            result = start_task("/api/v1/tasks/archive", payload)
            if result and result.get("task_id"):
                st.session_state['task_id'] = result["task_id"]
                st.session_state['polling_active'] = True
                st.success(f"Archive download task started! Task ID: {st.session_state['task_id']}")
                st.rerun()  # Rerun to start polling display
            else:
                st.error("Failed to start archive download task.")

# --- Task Status Display and Polling ---
st.subheader("⏳ Task Status")

active_task_id = st.session_state.get('task_id')
if active_task_id and st.session_state.get('polling_active'):
    task_data = get_task_status(active_task_id)
    if task_data:
        st.session_state['task_status_data'] = task_data
        status = task_data.get("status", "unknown")
        message = task_data.get("message", "...")
        progress = task_data.get("progress", 0)
        success_count = task_data.get("success", 0)
        error_count = task_data.get("errors", 0)
        skipped_count = task_data.get("skipped", 0)

        # Display task details
        st.info(f"**Task ID:** {active_task_id}")
        st.write(f"**Status:** {status.capitalize()}")
        st.write(f"**Details:** {message}")
        st.write(f"**Files Downloaded:** {success_count}")
        st.write(f"**Errors:** {error_count}")
        st.write(f"**Skipped:** {skipped_count}")
        st.progress(progress / 100.0)

        # Display cancel button if the task is active
        if status in ["running", "starting", "aborting"]:
            if st.button("Cancel Task", key=f"cancel_{active_task_id}"):
                cancel_result = cancel_task(active_task_id)
                if cancel_result:
                    st.warning(f"Cancellation requested for task {active_task_id}. Status will update shortly.")
                else:
                    st.error("Failed to send cancellation request.")

        # If the task is still active, wait a moment and rerun to update status
        if status not in ["completed", "aborted", "error"]:
            time.sleep(POLL_INTERVAL_SECONDS)
            st.rerun()
        else:
            # Task finished; stop polling.
            st.session_state['polling_active'] = False
    else:
        st.session_state['polling_active'] = False
        st.error(f"Could not retrieve status for task {active_task_id}. Stopping status updates.")
        st.rerun()

elif st.session_state.get('task_status_data'):
    # Display the last known status if polling is no longer active
    last_data = st.session_state['task_status_data']
    status = last_data.get("status", "unknown")
    message = last_data.get("message", "...")
    success_count = last_data.get("success", 0)
    error_count = last_data.get("errors", 0)
    skipped_count = last_data.get("skipped", 0)
    task_id_display = last_data.get("task_id", "N/A")  # Get task_id from data if possible

    st.info(f"**Last Task ID:** {task_id_display}")
    st.write(f"**Final Status:** {status.capitalize()}")
    st.write(f"**Final Message:** {message}")
    st.write(f"**Files Downloaded:** {success_count}")
    st.write(f"**Errors:** {error_count}")
    st.write(f"**Skipped:** {skipped_count}")
else:
    st.info("No active task running. Start a patching or archive task above.")
