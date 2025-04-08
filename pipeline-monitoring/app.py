import os
import datetime
import requests
import zipfile
from io import BytesIO
from flask import Flask, render_template, jsonify, request
import threading
import pytz
from elasticsearch import Elasticsearch  # Currently not used

app = Flask(__name__)

# Path directories and additional parameters
LOG_FILE = os.environ.get("LOG_FILE_PATH", "./logs/log.txt")
SCRAPING_LOG_FILE = os.environ.get("SCRAPING_FILE_PATH", "./logs/scraping_log.txt")
INGESTION_LOG_FILE = os.environ.get("INGESTION_FILE_PATH", "./logs/ingestion_log.txt")
TIMESTAMP_LOG_FILE = os.environ.get("TIMESTAMP_FILE_PATH", "./logs/timestamp_log.txt")
DOWNLOAD_FOLDER = "./csv"

INTERVAL = 15 * 60  # 15 minutes delay

# Global variables for background tasks and cancellation events
patching_thread = None
patching_cancel_event = threading.Event()

archive_thread = None
archive_cancel_event = threading.Event()

# Global progress trackers for tasks
patching_progress = {"percent": 0, "message": ""}
archive_progress = {"percent": 0, "message": ""}

############################ Helper Functions ############################

def write(content):
    """
    Append log data into the log file with a current timestamp (Asia/Singapore).
    """
    if not content:
        return

    timezone = pytz.timezone("Asia/Singapore")
    current_time_gmt8 = datetime.datetime.now(timezone)
    current_time = current_time_gmt8.strftime("%Y-%m-%d %H:%M:%S") + ": "
    with open(LOG_FILE, "a") as f:
        f.write(current_time + content + "\n")

def displaying_logs(file_path, n=6):
    with open(file_path, "r") as f:
        lines = [line for line in f.readlines() if line.strip()]
    return lines[-n:]

def get_remaining_time():
    """
    Reads the TIMESTAMP_LOG_FILE to get the latest run timestamp,
    then calculates and returns the remaining time until the next run.
    """
    try:
        with open(TIMESTAMP_LOG_FILE, "r") as f:
            # Read the last non-empty line
            lines = [line.strip() for line in f if line.strip()]
            if not lines:
                return None
            last_timestamp = lines[-1].rstrip(':')
            last_run_dt = datetime.datetime.strptime(last_timestamp, "%Y-%m-%d %H:%M:%S")
            next_run_dt = last_run_dt + datetime.timedelta(seconds=INTERVAL)
            now_dt = datetime.datetime.now() + datetime.timedelta(hours=8)
            remaining_seconds = max(0, int((next_run_dt - now_dt).total_seconds()))
            minutes, seconds = divmod(remaining_seconds, 60)
            return f"{minutes} min {seconds} sec"
    except Exception as e:
        print(f"Error reading log file: {e}")
        return None

def get_pipeline_status(pipeline, respective_log_file):
    """
    Determines the pipeline status by parsing the provided log file.
    Returns 'running', 'error', or 'idle'.
    """
    status = "idle"
    if not os.path.exists(respective_log_file):
        return status

    with open(respective_log_file, "r") as f:
        for line in f:
            if pipeline.lower() in line.lower():
                lower_line = line.lower()
                if "error" in lower_line:
                    status = "error"
                else:
                    status = "running"
    return status

def patching_task(look_back_days=3, base_url="http://data.gdeltproject.org/gdeltv2/"):
    """
    Downloads CSV files from the GDELT archive based on a look-back period.
    Checks for cancellation at each 15-minute interval.
    Updates patching_progress with the percent complete.
    """
    global patching_progress
    num_files_success, num_files_error = 0, 0
    now = datetime.datetime.now()
    start = now - datetime.timedelta(days=int(look_back_days))
    start = start.replace(second=0, microsecond=0)
    start_adjust = start.minute % 15
    if start_adjust != 0:
        start = start - datetime.timedelta(minutes=start_adjust)
    current = start
    write(f"Patching files from {look_back_days} days ago...")
    
    # Reset progress and cancellation flag at start
    patching_progress = {"percent": 0, "message": "Task started..."}
    patching_cancel_event.clear()
    total_steps = int((now - start).total_seconds() / (15*60)) + 1
    step_count = 0

    while current <= now:
        # If cancellation has been requested, log and update progress then exit the loop.
        if patching_cancel_event.is_set():
            write("Patching task cancelled by user.")
            patching_progress["message"] = "Patching task cancelled."
            break

        timestamp = current.strftime("%Y%m%d%H%M%S")
        local_filename = f"{timestamp}.gkg.csv"

        # Check if file has already been downloaded/processed
        if local_filename in os.listdir(DOWNLOAD_FOLDER):
            write(f"Extraction skipped: {local_filename} already exists.")
            continue

        write(f"Extracting patching file: {local_filename}...")
        file_url = f"{base_url}{timestamp}.gkg.csv.zip"
        try:
            response = requests.get(file_url, stream=True, timeout=10)
            if response.status_code == 200:
                zip_file = zipfile.ZipFile(BytesIO(response.content))
                zip_file.extract(local_filename, DOWNLOAD_FOLDER)
                num_files_success += 1
                write(f"Extracting patching file completed: {local_filename}.")
            else:
                write(f"Patching error {response.status_code} for URL: {file_url}")
                num_files_error += 1
        except Exception as e:
            num_files_error += 1
            write(f"Error extracting patching file {local_filename}: {e}")
        current += datetime.timedelta(minutes=15)
        step_count += 1
        patching_progress["percent"] = int((step_count / total_steps) * 100)
        patching_progress["message"] = f"Processing file {step_count} of {total_steps} ({patching_progress['percent']}%)."

    write(f"Patching files from {look_back_days} days ago completed.")
    msg = f'''Number of patching files extracted: {num_files_success}
Number of patching file errors: {num_files_error}
Extraction status:  {100*(num_files_success / (num_files_error + num_files_success)):.2f}% SUCCESSFUL'''
    write(msg)
    patching_progress["message"] = "Patching task completed."
    patching_progress["percent"] = 100

def patching_task_range(start_date_str, end_date_str, base_url="http://data.gdeltproject.org/gdeltv2/"):
    """
    Downloads CSV files from the GDELT archive within a custom date range.
    Checks for cancellation requests during the download process.
    Updates archive_progress with the percent complete.
    """
    global archive_progress
    num_files_success, num_files_error = 0, 0
    try:
        start = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
        end = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
        end = end + datetime.timedelta(days=1) - datetime.timedelta(microseconds=1)
    except Exception as e:
        write(f"Error parsing dates: {e}")
        archive_progress["message"] = "Error parsing dates."
        return

    start = start.replace(second=0, microsecond=0)
    start_adjust = start.minute % 15
    if start_adjust != 0:
        start = start - datetime.timedelta(minutes=start_adjust)
    current = start
    write(f"Patching files from {start_date_str} to {end_date_str}...")
    
    # Reset progress and cancellation flag at start
    archive_progress = {"percent": 0, "message": "Task started..."}
    archive_cancel_event.clear()
    total_steps = int((end - start).total_seconds() / (15*60)) + 1
    step_count = 0

    while current <= end:
        if archive_cancel_event.is_set():
            write("Archive download task cancelled by user.")
            archive_progress["message"] = "Archive download task cancelled."
            break

        timestamp = current.strftime("%Y%m%d%H%M%S")
        local_filename = f"{timestamp}.gkg.csv"
        if local_filename in os.listdir(DOWNLOAD_FOLDER):
            write(f"Extraction skipped: {local_filename} already exists.")
            continue

        write(f"Extracting archive file: {local_filename}...")
        file_url = f"{base_url}{timestamp}.gkg.csv.zip"
        try:
            response = requests.get(file_url, stream=True, timeout=10)
            if response.status_code == 200:
                zip_file = zipfile.ZipFile(BytesIO(response.content))
                zip_file.extract(local_filename, DOWNLOAD_FOLDER)
                write(f"Extracting archive file completed: {local_filename}.")
                num_files_success += 1
            else:
                write(f"Archival download error {response.status_code} for URL: {file_url}")
                num_files_error += 1
        except Exception as e:
            write(f"Error extracting archive file {local_filename}: {e}")
            num_files_error += 1
        current += datetime.timedelta(minutes=15)
        step_count += 1
        archive_progress["percent"] = int((step_count / total_steps) * 100)
        archive_progress["message"] = f"Processing file {step_count} of {total_steps} ({archive_progress['percent']}%)."

    write(f"Patching files from {start_date_str} to {end_date_str} completed.")
    msg = f'''Number of archive files extracted: {num_files_success}
Number of archive file errors: {num_files_error}
Extraction status:  {100*(num_files_success / (num_files_error + num_files_success)):.2f}% SUCCESSFUL'''
    write(msg)
    archive_progress["message"] = "Archive download task completed."
    archive_progress["percent"] = 100

############################ Flask Routes ############################

@app.route('/')
def dashboard():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            data = f.read().split("\n")
    else:
        data = []
    return render_template("dashboard.html", data=data)

@app.route("/remaining")
def remaining():
    remaining_time = get_remaining_time()
    if remaining_time is None:
        return jsonify(error="Error reading log file"), 500
    return jsonify(remaining=remaining_time)

@app.route('/logs')
def get_logs():
    logs = displaying_logs(LOG_FILE, 500)
    logs = [line.rstrip() for line in logs]
    return jsonify({"lines": logs})

@app.route('/scraping_logs', methods=['GET'])
def displaying_scraping_logs():
    scraping_logs = displaying_logs(SCRAPING_LOG_FILE)
    return jsonify({"lines": scraping_logs})

@app.route('/ingestion_logs', methods=['GET'])
def displaying_ingestion_logs():
    ingestion_logs = displaying_logs(INGESTION_LOG_FILE)
    return jsonify({"lines": ingestion_logs})

@app.route('/status', methods=['GET'])
def status():
    # Note: the extract, transform, and load statuses are determined by log files.
    # For brevity, these remain unchanged.
    scraping_status = get_pipeline_status("scraping", SCRAPING_LOG_FILE)
    ingestion_status = get_pipeline_status("ingestion", INGESTION_LOG_FILE)
    return jsonify({
        "extract": scraping_status,
        "transform": "idle",
        "load": ingestion_status
    })

# Start patching task in background thread
@app.route('/patching', methods=['POST'])
def patch_missing():
    look_back_days = request.form.get("look_back_days", 3)
    global patching_thread, patching_cancel_event, patching_progress
    patching_cancel_event.clear()  # Reset any previous cancellation
    # Reset progress before starting the task
    patching_progress = {"percent": 0, "message": "Task started..."}
    patching_thread = threading.Thread(target=patching_task, args=(look_back_days,))
    patching_thread.start()
    return jsonify({"status":"running", "message": "Patching started", "look_back_days": look_back_days})

# Cancel the patching task
@app.route('/patch_cancel', methods=['POST'])
def patch_cancel():
    global patching_cancel_event
    patching_cancel_event.set()
    return jsonify({"message": "Patching cancellation initiated."})

# Start archival download task in background thread
@app.route('/archive', methods=['POST'])
def archive_download():
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    try:
        datetime.datetime.strptime(start_date, "%Y-%m-%d")
        datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except Exception as e:
        return jsonify({"error": "Invalid date format."})
    global archive_thread, archive_cancel_event, archive_progress
    archive_cancel_event.clear()  # Reset any previous cancellation
    archive_progress = {"percent": 0, "message": "Task started..."}
    archive_thread = threading.Thread(target=patching_task_range, args=(start_date, end_date))
    archive_thread.start()
    return jsonify({"message": "Archive download started", "start_date": start_date, "end_date": end_date})

# Cancel the archival download task
@app.route('/archive_cancel', methods=['POST'])
def archive_cancel():
    global archive_cancel_event
    archive_cancel_event.set()
    return jsonify({"message": "Archive download cancellation initiated."})

@app.route('/get_archive_files', methods=['POST'])
def get_archive_files():
    """
    For compatibility with the previous implementation,
    this route now only lists downloaded files after the archive task runs.
    """
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    try:
        datetime.datetime.strptime(start_date, "%Y-%m-%d")
        datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except Exception as e:
        return jsonify({"files": [], "error": "Invalid date format."})
    try:
        all_files = os.listdir(DOWNLOAD_FOLDER)
    except Exception as e:
        return jsonify({"files": [], "error": str(e)})
    
    available_files = []
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    end_dt = end_dt + datetime.timedelta(days=1) - datetime.timedelta(microseconds=1)
    
    for f in all_files:
        try:
            file_date = datetime.datetime.strptime(f[:8], "%Y%m%d")
            if start_dt <= file_date <= end_dt:
                available_files.append(f)
        except Exception:
            continue
    
    return jsonify({"files": available_files})

# New endpoints for progress tracking

@app.route('/patch_progress', methods=['GET'])
def patch_progress_endpoint():
    return jsonify(patching_progress)

@app.route('/archive_progress', methods=['GET'])
def archive_progress_endpoint():
    return jsonify(archive_progress)

############################ Main ############################

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7979, debug=True)
