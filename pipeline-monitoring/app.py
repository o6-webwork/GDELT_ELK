import os
import datetime
import requests
import zipfile
from io import BytesIO
import threading
import pytz
from elasticsearch import Elasticsearch

from fastapi import FastAPI, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import uvicorn

# Create FastAPI app instance
app = FastAPI()

# Setup Jinja2 template directory
templates = Jinja2Templates(directory="templates")

# Path directories and additional parameters
LOG_FILE = os.environ.get("LOG_FILE_PATH", "./logs/log.txt")
SCRAPING_LOG_FILE = os.environ.get("SCRAPING_FILE_PATH", "./logs/scraping_log.txt")
INGESTION_LOG_FILE = os.environ.get("INGESTION_FILE_PATH", "./logs/ingestion_log.txt")
TIMESTAMP_LOG_FILE = os.environ.get("TIMESTAMP_FILE_PATH", "./logs/timestamp_log.txt")
JSON_LOG_FILE = os.environ.get("JSON_FILE_PATH", "./logs/json_log.txt")
DOWNLOAD_FOLDER = "./csv"
LOGSTASH_FOLDER = "./logstash_ingest_data/json"
PYSPARK_LOG_FILE = "./logs/pyspark_log.txt"

INTERVAL = 15 * 60  # 15 minutes delay

# Global variables for background tasks and cancellation events
patching_thread = None
patching_cancel_event = threading.Event()

archive_thread = None
archive_cancel_event = threading.Event()

# Global progress trackers for tasks
patching_progress = {"percent": 0, "message": ""}
archive_progress = {"percent": 0, "message": ""}

# Global lists to track downloaded/created files during tasks
patching_downloaded_files = []
archive_downloaded_files = []


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


def get_pipeline_status(respective_log_file):
    """
    Determines the pipeline status by parsing the provided log file.
    Returns 'running' or 'error'.
    """
    status = "running"
    with open(respective_log_file, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    last_lines = lines[-5:]
    for line in last_lines:
        if "error" in line.lower():
            return "error"
        status = "running"
    return status


def es_client_setup():
    """
    Sets up client to connect to Elasticsearch.
    """
    es_client = Elasticsearch(
        "https://es01:9200",
        basic_auth=("elastic", "changeme"),
        verify_certs=True,  # Set to True if using trusted certs
        ca_certs="./certs/ca/ca.crt",
        request_timeout=30
    )
    return es_client


def es_check_data(timestamp_str):
    """
    Queries Elasticsearch to check if data for given timestamp exists.
    """
    client = es_client_setup()
    query_body = {"query": {"term": {"GkgRecordId.Date": timestamp_str}}}
    response = client.count(index='gkg*', body=query_body, request_timeout=10)
    return response.get('count', 0) > 0


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
    
    patching_progress = {"percent": 0, "message": "Task started..."}
    patching_cancel_event.clear()
    total_steps = int((now - start).total_seconds() / (15*60)) + 1
    step_count = 0

    while current <= now:
        if patching_cancel_event.is_set():
            write("Patching task cancelled by user.")
            patching_progress["message"] = "Patching task cancelled."
            break

        timestamp = current.strftime("%Y%m%d%H%M%S")
        local_filename = f"{timestamp}.gkg.csv"

        if local_filename in os.listdir(DOWNLOAD_FOLDER) or es_check_data(timestamp) or f"{timestamp}.json" in os.listdir("./logstash_ingest_data/json"):
            write(f"Extraction skipped: {local_filename} already exists.")
            current += datetime.timedelta(minutes=15)
            step_count += 1
            patching_progress["percent"] = int((step_count / total_steps) * 100)
            patching_progress["message"] = f"Skipped {step_count} of {total_steps} files ({patching_progress['percent']}%)."
            continue

        write(f"Extracting patching file: {local_filename}...")
        file_url = f"{base_url}{timestamp}.gkg.csv.zip"
        try:
            response = requests.get(file_url, stream=True, timeout=10)
            if response.status_code == 200:
                zip_file = zipfile.ZipFile(BytesIO(response.content))
                zip_file.extract(local_filename, DOWNLOAD_FOLDER)
                patching_downloaded_files.append(local_filename)
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
        patching_progress["message"] = f"Extracting file {step_count} of {total_steps} ({patching_progress['percent']}%)."

    if not patching_cancel_event.is_set():
        patching_progress["message"] = "Patching task completed."
        patching_progress["percent"] = 100

    write(f"Patching files from {look_back_days} days ago completed.")
    msg = f'''Number of patching files extracted: {num_files_success}
                     Number of patching file errors: {num_files_error}
                     Extraction status:  {100*(num_files_success / (num_files_error + num_files_success)):.2f}% SUCCESSFUL'''
    write(msg)


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
        if local_filename in os.listdir(DOWNLOAD_FOLDER) or es_check_data(timestamp) or f"{timestamp}.json" in os.listdir("./logstash_ingest_data/json"):
            write(f"Extraction skipped: {local_filename} already exists.")
            current += datetime.timedelta(minutes=15)
            step_count += 1
            archive_progress["percent"] = int((step_count / total_steps) * 100)
            archive_progress["message"] = f"Skipped {step_count} of {total_steps} files ({archive_progress['percent']}%)."
            continue

        write(f"Extracting archive file: {local_filename}...")
        file_url = f"{base_url}{timestamp}.gkg.csv.zip"
        try:
            response = requests.get(file_url, stream=True, timeout=10)
            if response.status_code == 200:
                zip_file = zipfile.ZipFile(BytesIO(response.content))
                zip_file.extract(local_filename, DOWNLOAD_FOLDER)
                archive_downloaded_files.append(local_filename)
                num_files_success += 1
                write(f"Extracting archive file completed: {local_filename}.")
            else:
                write(f"Archival download error {response.status_code} for URL: {file_url}")
                num_files_error += 1
        except Exception as e:
            write(f"Error extracting archive file {local_filename}: {e}")
            num_files_error += 1
        current += datetime.timedelta(minutes=15)
        step_count += 1
        archive_progress["percent"] = int((step_count / total_steps) * 100)
        archive_progress["message"] = f"Extracting file {step_count} of {total_steps} ({archive_progress['percent']}%)."

    if not archive_cancel_event.is_set():
        archive_progress["message"] = "Archive download task completed."
        archive_progress["percent"] = 100
        
    write(f"Patching files from {start_date_str} to {end_date_str} completed.")
    msg = f'''Number of archive files extracted: {num_files_success}
                     Number of archive file errors: {num_files_error}
                     Extraction status:  {100*(num_files_success / (num_files_error + num_files_success)):.2f}% SUCCESSFUL'''
    write(msg)


############################ FastAPI Routes ############################

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            data = f.read().split("\n")
    else:
        data = []
    return templates.TemplateResponse("dashboard.html", {"request": request, "data": data})


@app.get("/remaining")
async def remaining():
    remaining_time = get_remaining_time()
    if remaining_time is None:
        return JSONResponse(content={"error": "Error reading log file"}, status_code=500)
    return {"remaining": remaining_time}


@app.get("/logs")
async def get_logs():
    logs = displaying_logs(LOG_FILE, 500)
    logs = [line.rstrip() for line in logs]
    return {"lines": logs}


@app.get("/scraping_logs")
async def displaying_scraping_logs():
    scraping_logs = displaying_logs(SCRAPING_LOG_FILE)
    return {"lines": scraping_logs}


@app.get("/ingestion_logs")
async def displaying_ingestion_logs():
    ingestion_logs = displaying_logs(JSON_LOG_FILE, 12)
    return {"lines": ingestion_logs}


@app.get("/status")
async def status():
    scraping_status = get_pipeline_status(SCRAPING_LOG_FILE)
    transform_status = get_pipeline_status(JSON_LOG_FILE)
    ingestion_status = get_pipeline_status(INGESTION_LOG_FILE)
    return {
        "extract": scraping_status,
        "transform": transform_status,
        "load": ingestion_status
    }


@app.get("/file_counts")
async def file_counts():
    try:
        csv_files = os.listdir(DOWNLOAD_FOLDER)
        json_files = os.listdir(LOGSTASH_FOLDER)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    csv_count = sum(1 for f in csv_files if f.lower().endswith(".csv"))
    json_count = sum(1 for f in json_files if f.lower().endswith(".json"))
    return {"csv_count": csv_count, "json_count": json_count}


@app.post("/patching")
async def patch_missing(look_back_days: int = Form(3)):
    global patching_thread, patching_cancel_event, patching_progress
    patching_cancel_event.clear()  # Reset previous cancellation
    patching_progress = {"percent": 0, "message": "Task started..."}
    patching_thread = threading.Thread(target=patching_task, args=(look_back_days,))
    patching_thread.start()
    return {"status": "running", "message": "Patching started", "look_back_days": look_back_days}


@app.post("/patch_cancel")
async def patch_cancel():
    global patching_cancel_event
    patching_cancel_event.set()
    return {"message": "Patching cancellation initiated."}


@app.post("/patch_stop_delete")
async def patch_stop_and_delete():
    global patching_cancel_event, patching_downloaded_files, patching_progress
    patching_cancel_event.set()
    deleted_files = []
    try:
        for filename in patching_downloaded_files:
            file_path = os.path.join(DOWNLOAD_FOLDER, filename)
            with open(PYSPARK_LOG_FILE, "r") as f:
                curr = f.read()
            if os.path.exists(file_path) and curr not in file_path:
                os.remove(file_path)
                deleted_files.append(filename)
        patching_downloaded_files = []
        patching_progress["message"] = "Patching task cancelled and downloaded files deleted."
        write("Patching task cancelled and downloaded files deleted by user request.")
        return {"message": f"Patching cancelled. Deleted files: {', '.join(deleted_files)}"}
    except Exception as e:
        write(f"Error during deletion of patch files: {e}")
        patching_progress["message"] = "Patching cancelled, but an error occurred during file deletion."
        return JSONResponse(content={"message": f"Patching cancelled, but an error occurred during file deletion: {e}"}, status_code=500)


@app.post("/archive")
async def archive_download(start_date: str = Form(...), end_date: str = Form(...)):
    try:
        datetime.datetime.strptime(start_date, "%Y-%m-%d")
        datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except Exception as e:
        return JSONResponse(content={"error": "Invalid date format."})
    global archive_thread, archive_cancel_event, archive_progress
    archive_cancel_event.clear()
    archive_progress = {"percent": 0, "message": "Task started..."}
    archive_thread = threading.Thread(target=patching_task_range, args=(start_date, end_date))
    archive_thread.start()
    return {"message": "Archive download started", "start_date": start_date, "end_date": end_date}


@app.post("/archive_cancel")
async def archive_cancel():
    global archive_cancel_event
    archive_cancel_event.set()
    return {"message": "Archive download cancellation initiated."}


@app.post("/archive_stop_delete")
async def archive_stop_and_delete():
    global archive_cancel_event, archive_downloaded_files, archive_progress
    archive_cancel_event.set()
    deleted_files = []
    try:
        for filename in archive_downloaded_files:
            file_path = os.path.join(DOWNLOAD_FOLDER, filename)
            if os.path.exists(file_path):
                os.remove(file_path)
                deleted_files.append(filename)
        archive_downloaded_files = []
        archive_progress["message"] = "Archive task cancelled and downloaded files deleted."
        write("Archive task cancelled and downloaded files deleted by user request.")
        return {"message": f"Archive cancelled. Deleted files: {', '.join(deleted_files)}"}
    except Exception as e:
        write(f"Error during deletion of archive files: {e}")
        archive_progress["message"] = "Archive cancelled, but an error occurred during file deletion."
        return JSONResponse(content={"message": f"Archive cancelled, but an error occurred during file deletion: {e}"}, status_code=500)


@app.post("/get_archive_files")
async def get_archive_files(start_date: str = Form(...), end_date: str = Form(...)):
    try:
        datetime.datetime.strptime(start_date, "%Y-%m-%d")
        datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except Exception as e:
        return {"files": [], "error": "Invalid date format."}
    try:
        all_files = os.listdir(DOWNLOAD_FOLDER)
    except Exception as e:
        return {"files": [], "error": str(e)}
    
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
    
    return {"files": available_files}


@app.get("/patch_progress")
async def patch_progress_endpoint():
    return patching_progress


@app.get("/archive_progress")
async def archive_progress_endpoint():
    return archive_progress


############################ Main ############################

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=7979, reload=True)
