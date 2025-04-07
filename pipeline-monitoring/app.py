# <app.py>
import os
import datetime
import requests
import zipfile
import shutil # Needed for run_patching_task file moves
from io import BytesIO
from flask import Flask, render_template, jsonify, request, current_app
import threading
import pytz
import logging
import time
from collections import deque
from elasticsearch import Elasticsearch # Added import
from elasticsearch.exceptions import ConnectionError, AuthenticationException # Added specific exceptions

# --- Configuration ---
LOG_FOLDER = os.environ.get("PIPELINE_LOG_FOLDER", "/app/logs")
DOWNLOAD_FOLDER = os.environ.get("PIPELINE_DOWNLOAD_FOLDER", "/app/csv")
GDELT_BASE_URL = os.environ.get("GDELT_BASE_URL", "http://data.gdeltproject.org/gdeltv2/")
DEFAULT_PATCH_LOOKBACK_DAYS = 3
MAX_LOG_LINES_DISPLAY = 500
TAIL_LOG_LINES = 30
# Elasticsearch Config
ES_HOST = os.environ.get("ES_HOST_FOR_APP", "es01")
ES_PORT = os.environ.get("ES_PORT", "9200")
ES_USER = os.environ.get("ELASTIC_USER", "elastic")
ES_PASSWORD = os.environ.get("ELASTIC_PASSWORD", "changeme")
ES_SCHEME = os.environ.get("ES_SCHEME", "https")
ES_INDEX_PATTERN = os.environ.get("ES_INDEX_PATTERN", "gkg*")
JSON_INGEST_FOLDER = os.environ.get("GDELT_JSON_INGEST_FOLDER", "/app/logstash_ingest_data/json")

# --- Flask App Setup ---
app = Flask(__name__)
app.config['LOG_FOLDER'] = LOG_FOLDER
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER
app.config['GDELT_BASE_URL'] = GDELT_BASE_URL
app.config['TIMESTAMP_LOG_FILE'] = os.path.join(LOG_FOLDER, "timestamp.log")
app.config['MAIN_ETL_LOG_FILE'] = os.path.join(LOG_FOLDER, "gdelt_etl.log")
app.config['LOGSTASH_LOG_FILE'] = os.path.join(LOG_FOLDER, "logstash-plain.log")
app.config['DOWNLOAD_INTERVAL_SECONDS'] = 15 * 60
app.config['JSON_INGEST_FOLDER'] = JSON_INGEST_FOLDER

# --- Logging Setup for Flask App ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = logging.FileHandler(os.path.join(LOG_FOLDER, 'pipeline_monitor.log'))
log_handler.setFormatter(log_formatter)
if not app.logger.handlers:
    app.logger.addHandler(log_handler)
    app.logger.setLevel(logging.INFO)

# --- Elasticsearch Client Setup ---
es_client = None
try:
    if not ES_PASSWORD or ES_PASSWORD == "changeme":
         app.logger.warning("ELASTIC_PASSWORD environment variable not set or is default 'changeme'. ES checks will likely fail.")
    es_client = Elasticsearch(
        f"{ES_SCHEME}://{ES_HOST}:{ES_PORT}",
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False, # Set to True if using trusted certs
        request_timeout=30
    )
    if not es_client.ping():
        raise ConnectionError("Elasticsearch ping failed.")
    app.logger.info(f"Successfully connected to Elasticsearch at {ES_HOST}:{ES_PORT}")
except (ConnectionError, AuthenticationException) as e:
    app.logger.critical(f"Failed to connect to Elasticsearch: {e}")
    es_client = None
except Exception as e:
    app.logger.critical(f"An unexpected error occurred during Elasticsearch client setup: {e}")
    es_client = None

# --- Background Task Management ---
background_tasks = {}
tasks_lock = threading.Lock()

def check_if_data_exists(timestamp_str):
    """Queries Elasticsearch to check if data for the given timestamp exists."""
    if not es_client:
        app.logger.error("Elasticsearch client not available, cannot check for existing data.")
        return False # Assume data doesn't exist if ES is down

    try:
        # Query using the string timestamp, assuming ES mapping is keyword/text
        query_body = {
            "query": {
                "term": {
                    # Adjust field name based on final ES mapping (dot notation for nested)
                    "GkgRecordId.Date": timestamp_str
                }
            }
        }
        # Add timeout to ES request
        resp = es_client.count(index=ES_INDEX_PATTERN, body=query_body, request_timeout=10)
        count = resp.get('count', 0)
        app.logger.debug(f"Elasticsearch check for timestamp {timestamp_str} (string): Found {count} documents.")
        return count > 0
    except ConnectionError:
         app.logger.error("Elasticsearch connection error during data check.")
         return False # Assume data doesn't exist if ES is down
    except Exception as e:
        app.logger.error(f"Error checking Elasticsearch for timestamp {timestamp_str}: {e}", exc_info=True)
        return False

# --- MODIFIED: Added local CSV file check ---
def run_patching_task(task_id, stop_event, look_back_days=None, start_date_str=None, end_date_str=None):
    """
    Worker function for patching tasks. Includes ES check, local CSV check, and cancellation.
    """
    global background_tasks
    app.logger.info(f"Starting background task {task_id}...")

    num_files_success = 0
    num_files_error = 0
    num_files_skipped = 0
    total_files_attempted = 0
    task_status = "running"

    try:
        base_url = app.config['GDELT_BASE_URL']
        download_folder = app.config['DOWNLOAD_FOLDER']
        os.makedirs(download_folder, exist_ok=True)

        if look_back_days:
            now = datetime.datetime.now(pytz.utc)
            start_dt = now - datetime.timedelta(days=int(look_back_days))
            end_dt = now
            task_range_msg = f"{look_back_days} days ago"
        elif start_date_str and end_date_str:
            start_dt = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=pytz.utc)
            end_dt = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=pytz.utc)
            task_range_msg = f"from {start_date_str} to {end_date_str}"
        else:
            raise ValueError("Either look_back_days or start/end dates must be provided")

        start_dt = start_dt.replace(second=0, microsecond=0)
        minute_adjust = start_dt.minute % 15
        if minute_adjust != 0:
            start_dt = start_dt - datetime.timedelta(minutes=minute_adjust)

        current_dt = start_dt
        app.logger.info(f"Task {task_id}: Patching files {task_range_msg}...")

        estimated_total = max(1, int((end_dt - start_dt).total_seconds() / (15 * 60)))

        while current_dt <= end_dt:
            # 1. Check for cancellation first
            if stop_event.is_set():
                app.logger.info(f"Task {task_id}: Cancellation requested. Aborting...")
                task_status = "aborted"
                break

            timestamp = current_dt.strftime("%Y%m%d%H%M%S")
            file_url = f"{base_url}{timestamp}.gkg.csv.zip"
            local_filename_base = f"{timestamp}.gkg.csv"
            local_filepath = os.path.join(download_folder, local_filename_base) # Path to check/create

            total_files_attempted += 1
            current_progress = min(100, int((total_files_attempted / estimated_total) * 100))
            # Update progress frequently
            with tasks_lock:
                if task_id in background_tasks:
                    background_tasks[task_id].update({
                        "progress": current_progress,
                        "success": num_files_success,
                        "errors": num_files_error,
                        "skipped": num_files_skipped,
                        "message": f"Processing {timestamp} ({total_files_attempted}/{estimated_total})..."
                    })

            # 2. Check Elasticsearch
            app.logger.debug(f"Task {task_id}: Checking Elasticsearch for timestamp {timestamp}...")
            if check_if_data_exists(timestamp):
                app.logger.info(f"Task {task_id}: Data for {timestamp} already in Elasticsearch. Skipping.")
                num_files_skipped += 1
                with tasks_lock:
                     if task_id in background_tasks: background_tasks[task_id]["skipped"] = num_files_skipped
                current_dt += datetime.timedelta(minutes=15)
                time.sleep(0.01)
                continue # Skip to next timestamp

            # 3. *** NEW: Check if local CSV file exists ***
            elif os.path.exists(local_filepath):
                app.logger.info(f"Task {task_id}: CSV file {local_filename_base} already exists locally. Skipping download.")
                num_files_skipped += 1
                with tasks_lock:
                     if task_id in background_tasks: background_tasks[task_id]["skipped"] = num_files_skipped
                current_dt += datetime.timedelta(minutes=15)
                time.sleep(0.01)
                continue # Skip to next timestamp

            # 4. If neither check passed, proceed with download
            else:
                app.logger.info(f"Task {task_id}: Data not found in ES and local CSV missing for {timestamp}. Proceeding with download.")
                app.logger.info(f"Task {task_id}: Attempting download: {file_url}")
                try:
                    response = requests.get(file_url, stream=True, timeout=60)
                    if response.status_code == 200:
                        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                             target_member = f"{timestamp}.gkg.csv"
                             members_to_extract = [m for m in zip_file.namelist() if os.path.basename(m) == target_member]
                             if members_to_extract:
                                 zip_file.extract(members_to_extract[0], download_folder)
                                 extracted_path = os.path.join(download_folder, members_to_extract[0])
                                 # Ensure final name is correct (handles potential subdirs in zip)
                                 if extracted_path != local_filepath and os.path.exists(extracted_path):
                                     shutil.move(extracted_path, local_filepath)
                                 app.logger.info(f"Task {task_id}: Extracted patching file: {local_filename_base}")
                                 num_files_success += 1
                             else:
                                 app.logger.warning(f"Task {task_id}: Target file {target_member} not found in zip: {file_url}")
                                 num_files_error += 1
                    elif response.status_code == 404:
                         app.logger.warning(f"Task {task_id}: File not found (404): {file_url}")
                         # Don't increment error count for 404
                    else:
                        app.logger.error(f"Task {task_id}: Patching download error {response.status_code} for URL: {file_url}")
                        num_files_error += 1
                except requests.exceptions.Timeout:
                     app.logger.error(f"Task {task_id}: Timeout downloading {file_url}")
                     num_files_error += 1
                except requests.exceptions.RequestException as e:
                    app.logger.error(f"Task {task_id}: Request error downloading {file_url}: {e}")
                    num_files_error += 1
                except (zipfile.BadZipFile, IOError) as e:
                     app.logger.error(f"Task {task_id}: Error extracting patching file {local_filename_base}: {e}")
                     num_files_error += 1
                except Exception as e:
                    app.logger.error(f"Task {task_id}: Unexpected error processing {timestamp}: {e}", exc_info=True)
                    num_files_error += 1

            # Increment time for the next iteration
            current_dt += datetime.timedelta(minutes=15)
            time.sleep(0.05) # Small sleep to yield control

        # --- Final Status Update ---
        final_progress = 100 if task_status == "running" else background_tasks.get(task_id, {}).get('progress', 0)
        final_status = "completed" if task_status == "running" else task_status

        attempted_downloads = num_files_success + num_files_error
        success_rate = 100 * (num_files_success / attempted_downloads) if attempted_downloads > 0 else 0

        if final_status == "completed":
            final_message = (f"Patching {task_range_msg} completed. "
                             f"Downloaded: {num_files_success}, Errors: {num_files_error}, Skipped (in ES or local): {num_files_skipped}. " # Updated message
                             f"({success_rate:.1f}% download success)")
        elif final_status == "aborted":
             final_message = (f"Patching {task_range_msg} aborted by user. "
                              f"Downloaded: {num_files_success}, Errors: {num_files_error}, Skipped: {num_files_skipped}.")
        else: # Should not happen
            final_message = "Patching ended unexpectedly."

        app.logger.info(f"Task {task_id}: {final_message}")
        with tasks_lock:
            if task_id in background_tasks:
                 background_tasks[task_id].update({
                    "status": final_status, "message": final_message, "progress": final_progress,
                    "success": num_files_success, "errors": num_files_error, "skipped": num_files_skipped
                })

    except Exception as e:
        error_message = f"Critical error during patching task {task_id}: {e}"
        app.logger.error(error_message, exc_info=True)
        with tasks_lock:
            current_status = background_tasks.get(task_id, {})
            background_tasks[task_id] = {
                **current_status, "status": "error", "message": error_message,
                "progress": current_status.get('progress', 0),
                "success": num_files_success, "errors": num_files_error, "skipped": num_files_skipped # Ensure counts are present
            }

# --- Helper Functions (Log reading, Status calculation) ---
# read_log_tail, get_remaining_time, get_phase_status remain the same...
def read_log_tail(file_path, n=10):
    try:
        if not os.path.exists(file_path): return []
        with open(file_path, "r", encoding='utf-8', errors='ignore') as f:
            return list(deque(f, n))
    except Exception as e:
        app.logger.error(f"Error reading log file {file_path}: {e}")
        return []

def get_remaining_time():
    timestamp_log = current_app.config['TIMESTAMP_LOG_FILE']
    interval_seconds = current_app.config['DOWNLOAD_INTERVAL_SECONDS']
    try:
        last_lines = read_log_tail(timestamp_log, 1)
        if not last_lines: return "N/A (No timestamp)"
        parts = last_lines[0].strip().split(':')
        if len(parts) >= 3:
            last_timestamp_str = f"{parts[0]}:{parts[1]}:{parts[2]}"
            last_timestamp_str = last_timestamp_str.split(" - ")[0]
        else:
            parts = last_lines[0].strip().split(" ")
            if len(parts) >= 2: last_timestamp_str = f"{parts[0]} {parts[1]}"
            else: raise ValueError("Timestamp format not recognized")
        sg_tz = pytz.timezone("Asia/Singapore")
        try: last_run_dt_local = datetime.datetime.strptime(last_timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
        except ValueError:
             try: last_run_dt_local = datetime.datetime.strptime(last_timestamp_str, "%Y-%m-%d %H:%M:%S")
             except ValueError:
                app.logger.error(f"Could not parse timestamp: {last_timestamp_str}")
                raise
        last_run_dt_aware = sg_tz.localize(last_run_dt_local)
        next_run_dt_aware = last_run_dt_aware + datetime.timedelta(seconds=interval_seconds)
        now_aware = datetime.datetime.now(sg_tz)
        remaining_delta = next_run_dt_aware - now_aware
        remaining_seconds = max(0, int(remaining_delta.total_seconds()))
        minutes, seconds = divmod(remaining_seconds, 60)
        return f"{minutes}m {seconds}s"
    except Exception as e:
        app.logger.error(f"Error calculating remaining time: {e}", exc_info=True)
        return "N/A (Error)"

def get_phase_status(log_file_path, activity_keywords, error_keywords, phase_name):
    status = "idle"; most_recent_event_time = None; found_error = False; found_activity = False
    try:
        recent_logs = read_log_tail(log_file_path, TAIL_LOG_LINES)
        if not recent_logs: return status
        now = datetime.datetime.now()
        time_window_seconds = current_app.config['DOWNLOAD_INTERVAL_SECONDS'] * (2.5 if phase_name == "Load" else 2.0)
        for line in reversed(recent_logs):
            lower_line = line.lower(); log_dt = None
            try:
                if line.startswith("["):
                    ts_match = line.split("]")[0].strip("[ ")
                    ts_match = ts_match.replace("T", " ")
                    try: log_dt = datetime.datetime.strptime(ts_match, '%Y-%m-%d %H:%M:%S,%f')
                    except ValueError:
                        try: log_dt = datetime.datetime.strptime(ts_match, '%Y-%m-%d %H:%M:%S')
                        except ValueError: pass
                if log_dt is None:
                    try:
                        ts_match = line.split(" - ")[0]
                        log_dt = datetime.datetime.strptime(ts_match, '%Y-%m-%d %H:%M:%S,%f')
                    except ValueError:
                         try: log_dt = datetime.datetime.strptime(ts_match, '%Y-%m-%d %H:%M:%S')
                         except ValueError: continue
                    except IndexError: continue
                if log_dt is None: continue
                is_recent = now - log_dt < datetime.timedelta(seconds=time_window_seconds)
                if is_recent:
                    is_error = any(err in lower_line for err in error_keywords)
                    is_activity = any(act in lower_line for act in activity_keywords)
                    current_event_time = log_dt
                    if is_error and (most_recent_event_time is None or current_event_time >= most_recent_event_time):
                        found_error = True; found_activity = False; most_recent_event_time = current_event_time
                    if is_activity and not found_error and (most_recent_event_time is None or current_event_time >= most_recent_event_time):
                        found_activity = True; most_recent_event_time = current_event_time
            except Exception: continue
        if found_error: status = "error"
        elif found_activity: status = "running"
        return status
    except Exception as e:
        app.logger.error(f"Error determining status for {phase_name} from {log_file_path}: {e}")
        return "unknown"

# --- Flask Routes ---
@app.route('/')
def dashboard(): return render_template("dashboard.html")

@app.route("/remaining")
def remaining(): return jsonify(remaining=get_remaining_time())

@app.route('/logs/main')
def get_main_logs():
    lines = read_log_tail(current_app.config['MAIN_ETL_LOG_FILE'], MAX_LOG_LINES_DISPLAY)
    return jsonify({"lines": lines})

@app.route('/logs/load')
def get_load_logs():
    lines = read_log_tail(current_app.config['LOGSTASH_LOG_FILE'], MAX_LOG_LINES_DISPLAY)
    return jsonify({"lines": lines})

@app.route('/status')
def get_status():
    main_etl_log_path = current_app.config['MAIN_ETL_LOG_FILE']
    logstash_log_path = current_app.config['LOGSTASH_LOG_FILE']
    extract_activity = ["checking gdelt", "found gdelt file urls", "attempting download", "extracted successfully"]
    extract_errors = ["extraction failure", "download/extraction failure", "error downloading/extracting"]
    transform_activity = ["starting spark pipeline", "spark job finished", "successfully processed", "moved and renamed json output"]
    transform_errors = ["spark analysis error", "spark pipeline error", "failed to process csv file", "no json part file found"]
    load_activity = ["fetched sender batch", "sending batch", "successfully sent batch", "pipeline started", "reading file", "processing", "[file] starting", "pushing batch", "created flush timer"]
    load_errors = ["failed", "error", "exception", "dead letter queue", "[dlq]", "connection refused", "connectexception", "timeout", "mapper_parsing_exception", "illegal_argument_exception", "response: 4", "response: 5"]
    extract_status = get_phase_status(main_etl_log_path, extract_activity, extract_errors, "Extract")
    transform_status = get_phase_status(main_etl_log_path, transform_activity, transform_errors, "Transform")
    load_status = get_phase_status(logstash_log_path, load_activity, load_errors, "Load")
    return jsonify({"extract": extract_status, "transform": transform_status, "load": load_status})

# --- Task Routes ---
@app.route('/patch/start', methods=['POST'])
def start_patching():
    global background_tasks; task_id = f"patch_{int(time.time())}"
    try: look_back_days = int(request.form.get("look_back_days", DEFAULT_PATCH_LOOKBACK_DAYS)); assert look_back_days >= 1
    except (ValueError, AssertionError): return jsonify({"error": "Invalid look back days"}), 400
    stop_event = threading.Event()
    with tasks_lock:
        background_tasks[task_id] = {
            "status": "starting", "message": "Patching task initializing...",
            "progress": 0, "errors": 0, "success": 0, "skipped": 0, "_event": stop_event
        }
    thread = threading.Thread(target=run_patching_task, args=(task_id, stop_event), kwargs={'look_back_days': look_back_days}, daemon=True)
    thread.start()
    app.logger.info(f"Dispatched patching task {task_id} for {look_back_days} days.")
    return jsonify({"message": "Patching task started.", "task_id": task_id})

@app.route('/archive/start', methods=['POST'])
def start_archive_ingestion():
    global background_tasks; task_id = f"archive_{int(time.time())}"; start_date = request.form.get('start_date'); end_date = request.form.get('end_date')
    if not start_date or not end_date: return jsonify({"error": "Start and end date required."}), 400
    try: datetime.datetime.strptime(start_date, "%Y-%m-%d"); datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError: return jsonify({"error": "Invalid date format (YYYY-MM-DD)."}), 400
    stop_event = threading.Event()
    with tasks_lock:
        background_tasks[task_id] = {
            "status": "starting", "message": "Archive download task initializing...",
            "progress": 0, "errors": 0, "success": 0, "skipped": 0, "_event": stop_event
        }
    thread = threading.Thread(target=run_patching_task, args=(task_id, stop_event), kwargs={'start_date_str': start_date, 'end_date_str': end_date}, daemon=True)
    thread.start()
    app.logger.info(f"Dispatched archive download task {task_id} for range {start_date} to {end_date}.")
    return jsonify({"message": "Archive download task started.", "task_id": task_id})

@app.route('/task/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    with tasks_lock: task_data = background_tasks.get(task_id)
    if not task_data: return jsonify({"error": "Task not found or expired"}), 404
    status_response = {k: v for k, v in task_data.items() if k != '_event'}
    return jsonify(status_response)

@app.route('/task/cancel/<task_id>', methods=['POST'])
def cancel_task(task_id):
    global background_tasks
    with tasks_lock:
        task_data = background_tasks.get(task_id)
        if not task_data: return jsonify({"error": "Task not found"}), 404
        current_status = task_data.get("status")
        if current_status not in ["running", "starting"]:
             return jsonify({"message": f"Task already {current_status}. Cannot cancel.", "status": current_status}), 400
        stop_event = task_data.get("_event")
        if stop_event:
            stop_event.set()
            task_data["status"] = "aborting"; task_data["message"] = "Cancellation requested..."
            app.logger.info(f"Cancellation signal sent for task {task_id}")
            return jsonify({"message": "Cancellation requested.", "task_id": task_id})
        else:
            app.logger.error(f"Task {task_id} found but has no cancellation event!")
            return jsonify({"error": "Internal error: Cannot find cancellation signal for task."}), 500

# --- Main Execution ---
if __name__ == '__main__':
    if es_client is None: print("CRITICAL: Elasticsearch client failed to initialize. Aborting flask app.")
    else:
        print("Attempting to start Flask app...")
        app.run(host='0.0.0.0', port=7979, debug=False, threaded=True)
        print("Flask app finished.")