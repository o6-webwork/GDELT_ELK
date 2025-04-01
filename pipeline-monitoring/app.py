import os
import datetime
import requests
import zipfile
from io import BytesIO
from flask import Flask, render_template, jsonify, request
import threading
import pytz

app = Flask(__name__)

# Path directories to visit
log_file = os.environ.get("LOG_FILE_PATH", "./logs/log.txt")
scraping_log_file = os.environ.get("SCRAPING_FILE_PATH", "./logs/scraping_log.txt")
ingestion_log_file = os.environ.get("INGESTION_FILE_PATH", "./logs/ingestion_log.txt")
timestamp_log_file = os.environ.get("TIMESTAMP_FILE_PATH", "./logs/timestamp_log.txt")
download_folder = "./csv"

#Additional parameters
INTERVAL = 15 * 60 #15 minutes delay

############################ Helper Functions ############################

def write(content):
    """
    Write log data into the log file.
    Note: This uses a static timestamp; consider updating to get the current time each call.
    """
    timezone = pytz.timezone("Asia/Singapore")
    current_time_gmt8 = datetime.datetime.now(timezone)
    current_time = current_time_gmt8.strftime("%Y-%m-%d %H:%M:%S") + ": "
    with open(log_file, "a") as f:
        f.append(current_time + content + "\n")

def displaying_logs(file_path, n=6):
    with open(file_path, "r") as f:
        lines = [line for line in f.readlines() if line.strip()]
    return lines[-n:]

def get_remaining_time():
    """
    Reads the log file to get the latest timestamp,
    then calculates and returns the remaining time (in seconds)
    until the next run.
    """
    try:
        with open(timestamp_log_file, "r") as f:
            # Read the last non-empty line
            lines = [line.strip() for line in f if line.strip()]
            if not lines:
                return None  # or handle as needed
            last_timestamp = lines[-1]
            # Remove any trailing colon
            last_timestamp = last_timestamp.rstrip(':')
            
            # Parse the last timestamp into a datetime object
            last_run_dt = datetime.datetime.strptime(last_timestamp, "%Y-%m-%d %H:%M:%S")
            # Calculate the next scheduled run time
            next_run_dt = last_run_dt + datetime.timedelta(seconds=INTERVAL)
            now_dt =  datetime.datetime.now() + datetime.timedelta(hours=8)
            
            # Calculate remaining seconds (ensure it's not negative)
            remaining_seconds = max(0, int((next_run_dt - now_dt).total_seconds()))
            # Convert seconds to minutes and seconds
            minutes, seconds = divmod(remaining_seconds, 60)
            
            return f"{minutes} min {seconds} sec"
    except Exception as e:
        print(f"Error reading log file: {e}")
        return None
    
def get_pipeline_status(pipeline, respective_log_file):
    """
    Parse the log file to determine the status of the given pipeline.
    Returns 'running', 'error', or 'idle' based on the log entries.
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
    Files are expected at 15-minute intervals.
    """
    num_files_success, num_files_error = 0, 0
    now = datetime.datetime.now()
    start = now - datetime.timedelta(days=int(look_back_days))
    start = start.replace(second=0, microsecond=0)
    start_adjust = start.minute % 15
    if start_adjust != 0:
        start = start - datetime.timedelta(minutes=start_adjust)
    current = start
    write(f"Patching files from {look_back_days} days ago...")
    while current <= now:
        timestamp = current.strftime("%Y%m%d%H%M%S")
        file_url = f"{base_url}{timestamp}.gkg.csv.zip"
        local_filename = f"{timestamp}.gkg.csv"
        write(f"Extracting {local_filename}...")
        try:
            response = requests.get(file_url, stream=True, timeout=10)
            if response.status_code == 200:
                zip_file = zipfile.ZipFile(BytesIO(response.content))
                zip_file.extract(local_filename, download_folder)
                num_files_success += 1
                write(f"Extracted {local_filename}.")
            else:
                write(f"File not found or error {response.status_code} for URL: {file_url}")
        except Exception as e:
            num_files_error += 1
            write(f"Error extracting {local_filename}: {e}")
        current += datetime.timedelta(minutes=15)
    write(f"Patching files from {look_back_days} days ago completed.")
    msg = f'''Number of files ingested: {num_files_success}
    Number of file errors: {num_files_error}
    Ingestion status:  {100*(num_files_success / (num_files_error + num_files_success)):.2f}% success'''
    write(msg)
    # Note: Returning a JSON response here isn’t used when running in a background thread.
    return jsonify({"message": f"Patching files from {look_back_days} days ago completed."})

def patching_task_range(start_date_str, end_date_str, base_url="http://data.gdeltproject.org/gdeltv2/"):
    """
    Downloads CSV files from the GDELT archive within a custom date range.
    Expects start_date_str and end_date_str in the format "YYYY-MM-DD".
    Files are expected at 15-minute intervals.
    """
    num_files_success, num_files_error = 0, 0
    try:
        start = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
        end = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
        end = end + datetime.timedelta(days=1) - datetime.timedelta(microseconds=1)
    except Exception as e:
        write(f"Error parsing dates: {e}")
        return jsonify({"message": "Invalid date format."})
    
    # Ensure start time is aligned: reset seconds/microseconds and round minutes down to nearest 15
    start = start.replace(second=0, microsecond=0)
    start_adjust = start.minute % 15
    if start_adjust != 0:
        start = start - datetime.timedelta(minutes=start_adjust)
    current = start
    write(f"Patching files from {start_date_str} to {end_date_str}...")
    while current <= end:
        # Create a timestamp string: YYYYMMDDHHMMSS (seconds always "00")
        timestamp = current.strftime("%Y%m%d%H%M%S")
        file_url = f"{base_url}{timestamp}.gkg.csv.zip"
        local_filename = f"{timestamp}.gkg.csv"
        write(f"Extracting {local_filename}...")
        
        try:
            response = requests.get(file_url, stream=True, timeout=10)
            if response.status_code == 200:
                zip_file = zipfile.ZipFile(BytesIO(response.content))
                zip_file.extract(local_filename, download_folder)
                write(f"Extracted {local_filename}.")
                num_files_success += 1
            else:
                write(f"File not found or error {response.status_code} for URL: {file_url}")
                num_files_error += 1
        except Exception as e:
            write(f"Error extracting {local_filename}: {e}")
            num_files_error += 1
        
        current += datetime.timedelta(minutes=15)
    
    write(f"Patching files from {start_date_str} to {end_date_str} completed.")
    msg = f'''Number of files ingested: {num_files_success}
Number of file errors: {num_files_error}
Ingestion status:  {100*(num_files_success / (num_files_error + num_files_success)):.2f}% success'''
    write(msg)
    return jsonify({"message": f"Patching files from {start_date_str} to {end_date_str} completed."})

def write_to_json():
    '''
    Writes CSV file data to a JSON file in required directory,
    and deletes the used CSV file.
    '''
    pass

############################ Flask Routes ############################

@app.route('/')
def dashboard():
    """
    Loads the HTML dashboard page.
    Reads the log file and passes its content (as a list of lines) to the template.
    """
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
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
    logs = displaying_logs(log_file,500)
    logs = [line.rstrip() for line in logs]
    return jsonify({"lines": logs})

@app.route('/scraping_logs', methods=['GET'])
def displaying_scraping_logs():
    scraping_logs = displaying_logs(scraping_log_file)
    return jsonify({"lines": scraping_logs})

@app.route('/ingestion_logs', methods=['GET'])
def displaying_ingestion_logs():
    ingestion_logs = displaying_logs(ingestion_log_file)
    return jsonify({"lines": ingestion_logs})

@app.route('/status', methods=['GET'])
def status():
    """
    Reads the scraping and ingestion log files to determine pipeline statuses,
    and returns the statuses as JSON.
    """
    scraping_status = get_pipeline_status("scraping", scraping_log_file)
    ingestion_status = get_pipeline_status("ingestion", ingestion_log_file)
    return jsonify({
        "scraping": scraping_status,
        "ingestion": ingestion_status
    })

@app.route('/patching', methods=['POST'])
def patch_missing():
    """
    Starts a background thread to patch missing files.
    """
    look_back_days = request.form.get("look_back_days", 3)
    threading.Thread(target=patching_task, args=(look_back_days,)).start()
    return jsonify({"message": "Patching started", "look_back_days": look_back_days})

@app.route('/get_archive_files', methods=['POST'])
def get_archive_files():
    """
    Uses the patching_task_range function to download files within a custom date range,
    then returns a list of available archive files (downloaded to the download folder) 
    along with the patching status message.
    Expects 'start_date' and 'end_date' (format: YYYY-MM-DD) from the form.
    """
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    try:
        # Validate date format
        datetime.datetime.strptime(start_date, "%Y-%m-%d")
        datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except Exception as e:
        return jsonify({"files": [], "error": "Invalid date format."})
    
    # Call patching_task_range synchronously to download files for the given range.
    patch_response = patching_task_range(start_date, end_date)
    patch_status = patch_response.get_json().get("message", "")
    
    try:
        all_files = os.listdir(download_folder)
    except Exception as e:
        return jsonify({"files": [], "error": str(e)})
    
    available_files = []
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    end_dt = end_dt + datetime.timedelta(days=1) - datetime.timedelta(microseconds=1)

    # Look for files named like "YYYYMMDDHHMMSS.gkg.csv"
    for f in all_files:
        try:
            file_date = datetime.datetime.strptime(f[:8], "%Y%m%d")
            if start_dt <= file_date <= end_dt:
                available_files.append(f)
        except Exception:
            continue
    
    return jsonify({"files": available_files, "patch_status": patch_status})

############################ Main ############################

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7979, debug=True)
