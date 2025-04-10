# frontend/pages/1_Pipeline_Monitoring.py
# Replicates original dashboard layout and functionality more closely

import streamlit as st
import requests
import time
import os
from datetime import date, timedelta, datetime
import logging

# --- Configuration ---
BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://backend-api:8000")
DEFAULT_LOOKBACK_DAYS = 3
POLL_INTERVAL_SECONDS = 3 # How often to check task status

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize session state variables if they don't exist
# Use separate keys for patch and archive tasks
if 'patch_task_id' not in st.session_state:
    st.session_state['patch_task_id'] = None
if 'patch_task_status_data' not in st.session_state:
    st.session_state['patch_task_status_data'] = None
if 'patch_polling_active' not in st.session_state:
    st.session_state['patch_polling_active'] = False

if 'archive_task_id' not in st.session_state:
    st.session_state['archive_task_id'] = None
if 'archive_task_status_data' not in st.session_state:
    st.session_state['archive_task_status_data'] = None
if 'archive_polling_active' not in st.session_state:
    st.session_state['archive_polling_active'] = False

# --- API Call Functions (Identical to previous version) ---

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

@st.cache_data(ttl=5) # Cache status for 5 seconds to reduce rapid calls
def get_task_status(task_id: str) -> dict | None:
    """Calls the backend API to get task status."""
    if not task_id: return None
    url = f"{BACKEND_API_URL}/api/v1/tasks/status/{task_id}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 404:
            logger.warning(f"Task {task_id} not found (404).")
            # Return a specific structure to indicate not found
            return {"status": "not_found", "message": f"Task {task_id} not found."}
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # Don't show error directly in UI here, return None and let caller handle
        logger.error(f"Error fetching task status for {task_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching status for {task_id}: {e}")
        return None

def cancel_task(task_id: str) -> dict | None:
    """Calls the backend API to cancel a task."""
    if not task_id: return None
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

@st.cache_data(ttl=10) # Cache backend status for 10 seconds
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

@st.cache_data(ttl=10) # Cache logs for 10 seconds
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


# --- Helper to display status card ---
def display_status_card(title, status_text, status_color, icon="⚙️", subtext=None):
    """Displays a status card similar to the original dashboard."""
    st.markdown(f"""
    <div style="background-color: white; padding: 1rem; border-radius: 0.5rem; box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);">
        <h2 style="font-size: 1.125rem; font-weight: 600; margin-bottom: 0.5rem; color: #4B5563;">
           {icon} {title}
        </h2>
        <div style="display: flex; align-items: center;">
            <span style="text-transform: capitalize; color: #374151; margin-right: 8px;">{status_text}</span>
            <span style="width: 12px; height: 12px; border-radius: 50%; background-color: {status_color}; display: inline-block;"></span>
        </div>
        {f'<p style="font-size: 0.75rem; color: #6B7280; margin-top: 0.25rem;">{subtext}</p>' if subtext else ''}
    </div>
    """, unsafe_allow_html=True)


# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="Pipeline Monitoring")
st.title("📊 GDELT Pipeline Dashboard")

# --- Top Status Row ---
st.subheader("Pipeline Status")
top_cols = st.columns(3)
status_data = fetch_backend_status()

# API Status Card
api_status = status_data.get("api_status", "unknown").capitalize()
api_color = "limegreen" if api_status == "Ok" else ("orange" if api_status == "Unknown" else "tomato")
with top_cols[0]:
    display_status_card("Backend API", api_status, api_color, "☁️")

# Elasticsearch Status Card
es_status = status_data.get("elasticsearch_status", "unknown").capitalize()
es_color = "limegreen" if es_status == "Ok" else ("orange" if es_status == "Unknown" else "tomato")
with top_cols[1]:
    display_status_card("Elasticsearch", es_status, es_color, "🔍")

# Placeholder/Info for ETL/Load Status (since backend doesn't provide it directly)
with top_cols[2]:
     display_status_card("ETL/Load", "Info", "dodgerblue", "ℹ️", "Check Logs Below")

st.markdown("---")


# --- Task Control Area ---
st.subheader("🔧 Manual Tasks")
task_cols = st.columns(2)

# --- Patching Task Column ---
with task_cols[0]:
    st.markdown("**Patch Recent Data**")
    lookback_days = st.number_input("Days to look back:", min_value=1, value=DEFAULT_LOOKBACK_DAYS, step=1, key="patch_days",
                                    disabled=st.session_state.get('patch_polling_active', False) or st.session_state.get('archive_polling_active', False)) # Disable if any task active

    if st.button("Start Patching Task", key="start_patch",
                  disabled=st.session_state.get('patch_polling_active', False) or st.session_state.get('archive_polling_active', False)): # Disable if any task active
        st.session_state['patch_task_id'] = None # Reset previous task id
        st.session_state['patch_task_status_data'] = None # Reset previous task data
        payload = {"look_back_days": lookback_days}
        result = start_task("/api/v1/tasks/patch", payload)
        if result and result.get("task_id"):
            st.session_state['patch_task_id'] = result["task_id"]
            st.session_state['patch_polling_active'] = True
            st.success(f"Patching task started! Task ID: {st.session_state['patch_task_id']}")
            time.sleep(0.5) # Short delay before rerun
            st.rerun()
        else:
            st.error("Failed to start patching task.")

    # --- Patching Task Status Display ---
    st.markdown("**Patching Task Status**")
    patch_status_container = st.container(border=True) # Add border for visual grouping
    with patch_status_container:
        active_patch_task_id = st.session_state.get('patch_task_id')
        patch_polling_active = st.session_state.get('patch_polling_active', False)
        patch_task_data = None

        if active_patch_task_id and patch_polling_active:
            patch_task_data = get_task_status(active_patch_task_id)
            if patch_task_data and patch_task_data.get("status") != "not_found":
                st.session_state['patch_task_status_data'] = patch_task_data
            else:
                # Task not found or error fetching, stop polling for this task
                st.session_state['patch_polling_active'] = False
                patch_polling_active = False
                if patch_task_data and patch_task_data.get("status") == "not_found":
                    st.warning(patch_task_data.get("message", f"Task {active_patch_task_id} not found."))
                else:
                    st.error(f"Could not retrieve status for task {active_patch_task_id}.")
                # Clear the task ID only if status fetch failed after starting
                # st.session_state['patch_task_id'] = None

        # Display based on latest data in session state for this task type
        current_patch_status_data = st.session_state.get('patch_task_status_data')

        if current_patch_status_data and current_patch_status_data.get("task_id") == active_patch_task_id:
            status = current_patch_status_data.get("status", "unknown")
            message = current_patch_status_data.get("message", "...")
            progress = current_patch_status_data.get("progress", 0)
            success_count = current_patch_status_data.get("success", "-")
            error_count = current_patch_status_data.get("errors", "-")
            skipped_count = current_patch_status_data.get("skipped", "-")

            st.info(f"**Task ID:** `{active_patch_task_id}`")
            st.write(f"**Status:** {status.capitalize()}")
            st.write(f"**Details:** {message}")
            st.progress(progress / 100.0)
            st.caption(f"Success: {success_count} | Errors: {error_count} | Skipped: {skipped_count}")

            if status in ["running", "starting", "aborting"]:
                 if st.button("Cancel Patch Task", key=f"cancel_patch_{active_patch_task_id}"):
                     with st.spinner("Requesting cancellation..."):
                        cancel_result = cancel_task(active_patch_task_id)
                        if cancel_result: st.warning(f"Cancellation requested. Status will update.")
                        else: st.error("Failed to send cancellation request.")
                        time.sleep(1) # Give backend time to maybe update status
                        st.rerun()

            # Check if task is finished to potentially stop polling
            if status in ["completed", "aborted", "error", "not_found"]:
                 if st.session_state.get('patch_polling_active'):
                     logger.info(f"Patch task {active_patch_task_id} finished with status {status}, stopping polling.")
                     st.session_state['patch_polling_active'] = False
                     # Optionally clear task ID once finished if desired
                     # st.session_state['patch_task_id'] = None
                     # st.session_state['patch_task_status_data'] = None
                     st.rerun() # Rerun to remove cancel button etc.

            # If still active, schedule a rerun
            elif patch_polling_active:
                try:
                    st.experimental_rerun(ttl=POLL_INTERVAL_SECONDS)
                except Exception as e:
                    logger.warning(f"Could not schedule auto-rerun for patch task: {e}")
                    st.toast(f"Status polling inactive. Manual refresh needed.", icon="⚠️")
                    st.session_state['patch_polling_active'] = False # Stop trying if rerun fails

        elif active_patch_task_id and patch_polling_active:
             # Case where polling is active but status fetch failed
             st.warning(f"Waiting for status update for task {active_patch_task_id}...")
             try:
                 st.experimental_rerun(ttl=POLL_INTERVAL_SECONDS)
             except Exception as e:
                  logger.warning(f"Could not schedule auto-rerun for patch task: {e}")
                  st.toast(f"Status polling inactive. Manual refresh needed.", icon="⚠️")
                  st.session_state['patch_polling_active'] = False
        else:
             st.caption("No active patching task.")


# --- Archive Task Column ---
with task_cols[1]:
    st.markdown("**Download Archive Data**")
    default_end = date.today() - timedelta(days=1)
    default_start = default_end - timedelta(days=6)
    archive_start_date = st.date_input("Start Date", value=default_start, max_value=default_end, key="archive_start",
                                       disabled=st.session_state.get('patch_polling_active', False) or st.session_state.get('archive_polling_active', False))
    archive_end_date = st.date_input("End Date", value=default_end, min_value=archive_start_date, max_value=default_end, key="archive_end",
                                     disabled=st.session_state.get('patch_polling_active', False) or st.session_state.get('archive_polling_active', False))

    if st.button("Start Archive Download Task", key="start_archive",
                  disabled=st.session_state.get('patch_polling_active', False) or st.session_state.get('archive_polling_active', False)):
        if archive_start_date > archive_end_date:
             st.warning("End date must be on or after start date.")
        else:
            st.session_state['archive_task_id'] = None # Reset previous task
            st.session_state['archive_task_status_data'] = None
            payload = {
                "start_date": archive_start_date.strftime("%Y-%m-%d"),
                "end_date": archive_end_date.strftime("%Y-%m-%d")
            }
            result = start_task("/api/v1/tasks/archive", payload)
            if result and result.get("task_id"):
                st.session_state['archive_task_id'] = result["task_id"]
                st.session_state['archive_polling_active'] = True
                st.success(f"Archive download task started! Task ID: {st.session_state['archive_task_id']}")
                time.sleep(0.5) # Short delay before rerun
                st.rerun()
            else:
                st.error("Failed to start archive download task.")

    # --- Archive Task Status Display ---
    st.markdown("**Archive Task Status**")
    archive_status_container = st.container(border=True)
    with archive_status_container:
        active_archive_task_id = st.session_state.get('archive_task_id')
        archive_polling_active = st.session_state.get('archive_polling_active', False)
        archive_task_data = None

        if active_archive_task_id and archive_polling_active:
            archive_task_data = get_task_status(active_archive_task_id)
            if archive_task_data and archive_task_data.get("status") != "not_found":
                st.session_state['archive_task_status_data'] = archive_task_data
            else:
                st.session_state['archive_polling_active'] = False
                archive_polling_active = False
                if archive_task_data and archive_task_data.get("status") == "not_found":
                     st.warning(archive_task_data.get("message", f"Task {active_archive_task_id} not found."))
                else:
                     st.error(f"Could not retrieve status for task {active_archive_task_id}.")
                # st.session_state['archive_task_id'] = None

        current_archive_status_data = st.session_state.get('archive_task_status_data')

        if current_archive_status_data and current_archive_status_data.get("task_id") == active_archive_task_id:
            status = current_archive_status_data.get("status", "unknown")
            message = current_archive_status_data.get("message", "...")
            progress = current_archive_status_data.get("progress", 0)
            success_count = current_archive_status_data.get("success", "-")
            error_count = current_archive_status_data.get("errors", "-")
            skipped_count = current_archive_status_data.get("skipped", "-")

            st.info(f"**Task ID:** `{active_archive_task_id}`")
            st.write(f"**Status:** {status.capitalize()}")
            st.write(f"**Details:** {message}")
            st.progress(progress / 100.0)
            st.caption(f"Success: {success_count} | Errors: {error_count} | Skipped: {skipped_count}")

            if status in ["running", "starting", "aborting"]:
                 if st.button("Cancel Archive Task", key=f"cancel_archive_{active_archive_task_id}"):
                     with st.spinner("Requesting cancellation..."):
                         cancel_result = cancel_task(active_archive_task_id)
                         if cancel_result: st.warning(f"Cancellation requested. Status will update.")
                         else: st.error("Failed to send cancellation request.")
                         time.sleep(1)
                         st.rerun()

            if status in ["completed", "aborted", "error", "not_found"]:
                if st.session_state.get('archive_polling_active'):
                    logger.info(f"Archive task {active_archive_task_id} finished with status {status}, stopping polling.")
                    st.session_state['archive_polling_active'] = False
                    # st.session_state['archive_task_id'] = None
                    # st.session_state['archive_task_status_data'] = None
                    st.rerun()

            elif archive_polling_active:
                try:
                    st.experimental_rerun(ttl=POLL_INTERVAL_SECONDS)
                except Exception as e:
                    logger.warning(f"Could not schedule auto-rerun for archive task: {e}")
                    st.toast(f"Status polling inactive. Manual refresh needed.", icon="⚠️")
                    st.session_state['archive_polling_active'] = False

        elif active_archive_task_id and archive_polling_active:
            st.warning(f"Waiting for status update for task {active_archive_task_id}...")
            try:
                st.experimental_rerun(ttl=POLL_INTERVAL_SECONDS)
            except Exception as e:
                logger.warning(f"Could not schedule auto-rerun for archive task: {e}")
                st.toast(f"Status polling inactive. Manual refresh needed.", icon="⚠️")
                st.session_state['archive_polling_active'] = False
        else:
            st.caption("No active archive task.")


st.markdown("---")

# --- Log Display ---
st.subheader("📄 Logs")
log_cols = st.columns(2)
with log_cols[0]:
    with st.expander("ETL Logs (Last 50 lines)", expanded=False):
        etl_logs = fetch_logs("gdelt_etl", 50)
        st.code("\n".join(etl_logs), language="log")

with log_cols[1]:
    with st.expander("Logstash Logs (Last 50 lines)", expanded=False):
        # Ensure backend API uses "logstash-plain" as key if that's the filename
        ls_logs = fetch_logs("logstash-plain", 50)
        st.code("\n".join(ls_logs), language="log")