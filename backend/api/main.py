# backend/api/main.py (Modified for Task Management)

import os
import logging
from datetime import date, timedelta, datetime
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any
from collections import deque
import asyncio # For async operations if needed
import threading # For background tasks (alternative: Celery)
import time
import pytz # For timezone handling in tasks
import requests # For downloading GDELT files in tasks
import zipfile # For extracting GDELT files
import shutil # For moving files after extraction
from io import BytesIO # For handling zip bytes

import aiofiles
from fastapi import FastAPI, HTTPException, Query, Path, BackgroundTasks, Body, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from elasticsearch import AsyncElasticsearch, ConnectionError, AuthenticationException, NotFoundError
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# --- Configuration ---
# Elasticsearch Config (as before)
ES_HOST = os.environ.get("ES_HOST", "localhost")
ES_PORT = os.environ.get("ES_PORT", "9200")
ES_USERNAME = os.environ.get("ELASTIC_USER", "elastic")
ES_PASSWORD = os.environ.get("ELASTIC_PASSWORD", "changeme")
ES_SCHEME = os.environ.get("ES_SCHEME", "https")
ES_VERIFY_CERTS_STR = os.environ.get("ES_VERIFY_CERTS", "False").lower()
ES_VERIFY_CERTS = ES_VERIFY_CERTS_STR in ['true', '1', 't', 'y', 'yes']
ES_INDEX_PATTERN = os.environ.get("ES_INDEX_PATTERN", "gkg*") # Index pattern for checks

# Log File Config (as before)
LOG_FOLDER = os.environ.get("PIPELINE_LOG_FOLDER", "/app/logs")
LOG_FILES = {
    "gdelt_etl": os.path.join(LOG_FOLDER, "gdelt_etl.log"),
    "logstash-plain": os.path.join(LOG_FOLDER, "logstash-plain.log"),
    "pipeline_monitor": os.path.join(LOG_FOLDER, "pipeline_monitor.log"), # Log for the *old* Flask app
    "fastapi_api": os.path.join(LOG_FOLDER, "fastapi_api.log") # Log for THIS app
}
MAX_LOG_LINES = 50

# Task Configuration
DOWNLOAD_FOLDER = os.environ.get("GDELT_DOWNLOAD_FOLDER", "/app/csv") # Matching ETL processor volume
GDELT_BASE_URL = os.environ.get("GDELT_BASE_URL", "http://data.gdeltproject.org/gdeltv2/")
DEFAULT_PATCH_LOOKBACK_DAYS = 3

# --- Logging Setup ---
# Ensure log directory exists
os.makedirs(LOG_FOLDER, exist_ok=True)
# Configure logging for this FastAPI app
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler = logging.FileHandler(LOG_FILES["fastapi_api"]) # Log to its own file
log_handler.setFormatter(log_formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(log_handler)
    # Optionally add StreamHandler to see logs in docker output
    # logger.addHandler(logging.StreamHandler())

# --- Elasticsearch Client State & Lifespan (as before) ---
es_state = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Attempting to connect to Elasticsearch at {ES_SCHEME}://{ES_HOST}:{ES_PORT}...")
    if not ES_PASSWORD or ES_PASSWORD == "changeme":
         logger.warning("ELASTIC_PASSWORD environment variable not set or is default 'changeme'. ES checks might fail.")
    es_hosts = [f"{ES_SCHEME}://{ES_HOST}:{ES_PORT}"]
    try:
        client = AsyncElasticsearch(
            hosts=es_hosts, basic_auth=(ES_USERNAME, ES_PASSWORD),
            verify_certs=ES_VERIFY_CERTS, request_timeout=30
        )
        if not await client.ping():
             raise ConnectionError("Elasticsearch ping failed on startup.")
        es_state["es_client"] = client
        logger.info("Successfully connected to Elasticsearch.")
    except (ConnectionError, AuthenticationException) as e:
        logger.critical(f"Failed to connect to Elasticsearch on startup: {e}")
        es_state["es_client"] = None
    except Exception as e:
        logger.critical(f"An unexpected error occurred during Elasticsearch client setup: {e}", exc_info=True)
        es_state["es_client"] = None
    yield
    client = es_state.get("es_client")
    if client:
        logger.info("Closing Elasticsearch connection...")
        await client.close()
        logger.info("Elasticsearch connection closed.")

# --- FastAPI App ---
app = FastAPI(lifespan=lifespan, title="GDELT ELK Backend API", version="1.1.0") # Updated version

# --- Background Task Management (Adapted from Flask app) ---
# NOTE: Using threading here for simplicity, similar to the original Flask app.
# For production, consider Celery or Arq for more robust background task handling,
# especially if you anticipate many concurrent tasks or need better scaling/retries.
background_tasks: Dict[str, Dict[str, Any]] = {}
tasks_lock = threading.Lock() # Use standard threading lock for simplicity with threads

# --- Task Helper Functions ---

async def check_if_data_exists_async(es_client: AsyncElasticsearch, timestamp_str: str) -> bool:
    """Async queries Elasticsearch to check if data for the given timestamp exists."""
    if not es_client:
        logger.error("Elasticsearch client not available, cannot check for existing data.")
        return False
    try:
        query_body = {"query": {"term": {"GkgRecordId.Date": timestamp_str}}}
        # Using await with the async client
        resp = await es_client.count(index=ES_INDEX_PATTERN, body=query_body, request_timeout=15)
        count = resp.get('count', 0)
        logger.debug(f"Async ES check for timestamp {timestamp_str}: Found {count} documents.")
        return count > 0
    except ConnectionError:
        logger.error("Async ES connection error during data check.")
        return False
    except NotFoundError:
        logger.debug(f"Index pattern {ES_INDEX_PATTERN} not found during check for {timestamp_str}. Assuming data doesn't exist.")
        return False
    except Exception as e:
        logger.error(f"Async Error checking ES for timestamp {timestamp_str}: {e}", exc_info=True)
        return False

def run_patching_task_worker(task_id: str, stop_event: threading.Event, **kwargs):
    """
    Worker function (synchronous) for patching/archiving tasks.
    Adapted from Flask app's run_patching_task.
    Runs within a standard thread.
    """
    global background_tasks
    logger.info(f"Starting background task {task_id}...")

    num_files_success = 0
    num_files_error = 0
    num_files_skipped = 0
    total_files_attempted = 0
    task_status = "running"

    try:
        look_back_days = kwargs.get('look_back_days')
        start_date_str = kwargs.get('start_date_str')
        end_date_str = kwargs.get('end_date_str')

        base_url = GDELT_BASE_URL
        download_folder = DOWNLOAD_FOLDER
        os.makedirs(download_folder, exist_ok=True)

        # Determine date range
        if look_back_days:
            now = datetime.now(pytz.utc)
            start_dt = now - timedelta(days=int(look_back_days))
            end_dt = now
            task_range_msg = f"lookback {look_back_days} days"
        elif start_date_str and end_date_str:
            start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=pytz.utc)
            end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=pytz.utc)
            task_range_msg = f"range {start_date_str} to {end_date_str}"
        else:
            raise ValueError("Either look_back_days or start/end dates must be provided")

        # Align start time to nearest previous 15-min interval
        start_dt = start_dt.replace(second=0, microsecond=0)
        minute_adjust = start_dt.minute % 15
        if minute_adjust != 0:
            start_dt = start_dt - timedelta(minutes=minute_adjust)

        current_dt = start_dt
        logger.info(f"Task {task_id}: Processing {task_range_msg}...")

        estimated_total = max(1, int((end_dt - start_dt).total_seconds() / (15 * 60)))

        # Get ES client (synchronously within thread is okay if client handles it)
        # Ideally, pass the async client and use an event loop if running fully async,
        # but for threaded approach, getting sync client or using async client carefully is needed.
        # Here, we'll try using the async client from the main thread's context via asyncio.run
        async_es_client = es_state.get("es_client")
        if not async_es_client:
             raise ConnectionError("Elasticsearch client not initialized for background task.")


        while current_dt <= end_dt:
            # 1. Check for cancellation
            if stop_event.is_set():
                logger.info(f"Task {task_id}: Cancellation requested. Aborting...")
                task_status = "aborted"
                break

            timestamp = current_dt.strftime("%Y%m%d%H%M%S")
            file_url = f"{base_url}{timestamp}.gkg.csv.zip"
            local_filename_base = f"{timestamp}.gkg.csv"
            local_filepath = os.path.join(download_folder, local_filename_base)

            total_files_attempted += 1
            current_progress = min(100, int((total_files_attempted / estimated_total) * 100))

            # Update progress - Use thread lock
            with tasks_lock:
                if task_id in background_tasks:
                    background_tasks[task_id].update({
                        "progress": current_progress, "success": num_files_success,
                        "errors": num_files_error, "skipped": num_files_skipped,
                        "message": f"Processing {timestamp} ({total_files_attempted}/{estimated_total})..."
                    })

            # 2. Check Elasticsearch (Run async check synchronously from thread)
            logger.debug(f"Task {task_id}: Checking ES for {timestamp}...")
            try:
                # Need to run the async function within the thread's context
                # One way is using asyncio.run (may start a new loop, use carefully)
                exists_in_es = asyncio.run(check_if_data_exists_async(async_es_client, timestamp))
            except RuntimeError as e:
                 logger.error(f"Task {task_id}: RuntimeError calling async ES check (might be nested loops): {e}. Assuming data doesn't exist.")
                 exists_in_es = False # Fallback: assume not found if async call fails
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error calling async ES check: {e}. Assuming data doesn't exist.")
                 exists_in_es = False # Fallback


            if exists_in_es:
                logger.info(f"Task {task_id}: Data for {timestamp} already in ES. Skipping.")
                num_files_skipped += 1
            # 3. Check local CSV file
            elif os.path.exists(local_filepath):
                logger.info(f"Task {task_id}: CSV {local_filename_base} exists locally. Skipping download.")
                num_files_skipped += 1
            # 4. Download if checks failed
            else:
                logger.info(f"Task {task_id}: Attempting download: {file_url}")
                try:
                    response = requests.get(file_url, stream=True, timeout=60)
                    if response.status_code == 200:
                         # ... (rest of download and extraction logic is identical to Flask app)
                        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                             target_member = f"{timestamp}.gkg.csv"
                             members_to_extract = [m for m in zip_file.namelist() if os.path.basename(m) == target_member]
                             if members_to_extract:
                                 zip_file.extract(members_to_extract[0], download_folder)
                                 extracted_path = os.path.join(download_folder, members_to_extract[0])
                                 if extracted_path != local_filepath and os.path.exists(extracted_path):
                                     shutil.move(extracted_path, local_filepath)
                                 logger.info(f"Task {task_id}: Extracted file: {local_filename_base}")
                                 num_files_success += 1
                             else:
                                 logger.warning(f"Task {task_id}: Target {target_member} not found in zip: {file_url}")
                                 num_files_error += 1 # Count as error if target missing in downloaded zip
                    elif response.status_code == 404:
                         logger.warning(f"Task {task_id}: File not found (404): {file_url}")
                         # Not an error, just missing source data
                    else:
                        logger.error(f"Task {task_id}: Download error {response.status_code} for URL: {file_url}")
                        num_files_error += 1
                except requests.exceptions.Timeout:
                     logger.error(f"Task {task_id}: Timeout downloading {file_url}")
                     num_files_error += 1
                except requests.exceptions.RequestException as e:
                    logger.error(f"Task {task_id}: Request error downloading {file_url}: {e}")
                    num_files_error += 1
                except (zipfile.BadZipFile, IOError) as e:
                     logger.error(f"Task {task_id}: Error extracting file {local_filename_base}: {e}")
                     num_files_error += 1
                except Exception as e:
                    logger.error(f"Task {task_id}: Unexpected error processing {timestamp}: {e}", exc_info=True)
                    num_files_error += 1

            # Update counts in shared dict
            with tasks_lock:
                if task_id in background_tasks:
                     background_tasks[task_id].update({
                         "success": num_files_success, "errors": num_files_error, "skipped": num_files_skipped
                     })

            current_dt += timedelta(minutes=15)
            time.sleep(0.05) # Small sleep

        # --- Final Status Update ---
        final_progress = 100 if task_status == "running" else background_tasks.get(task_id, {}).get('progress', 0)
        final_status = "completed" if task_status == "running" else task_status
        attempted_downloads = num_files_success + num_files_error
        success_rate = f"{100 * (num_files_success / attempted_downloads):.1f}%" if attempted_downloads > 0 else "N/A"

        if final_status == "completed": final_message = f"Task ({task_range_msg}) completed. Downloaded: {num_files_success}, Errors: {num_files_error}, Skipped: {num_files_skipped}. ({success_rate} success)"
        elif final_status == "aborted": final_message = f"Task ({task_range_msg}) aborted. Downloaded: {num_files_success}, Errors: {num_files_error}, Skipped: {num_files_skipped}."
        else: final_message = "Task ended unexpectedly."

        logger.info(f"Task {task_id}: {final_message}")
        with tasks_lock:
            if task_id in background_tasks:
                 background_tasks[task_id].update({
                    "status": final_status, "message": final_message, "progress": final_progress
                })

    except Exception as e:
        error_message = f"Critical error during task {task_id}: {e}"
        logger.error(error_message, exc_info=True)
        with tasks_lock:
            current_status = background_tasks.get(task_id, {})
            background_tasks[task_id] = {**current_status, "status": "error", "message": error_message}


# --- Existing Helper Functions (Log reading) ---
async def get_es_client() -> Optional[AsyncElasticsearch]:
    return es_state.get("es_client")

async def read_log_tail(file_path: str, n=MAX_LOG_LINES) -> List[str]:
    # (Log reading function remains the same as before)
    if not os.path.exists(file_path):
         logger.warning(f"Log file does not exist: {file_path}")
         return [f"Log file not found at: {file_path}"]
    if not os.path.isfile(file_path):
        logger.warning(f"Log path is not a file: {file_path}")
        return [f"Log path is not a file: {file_path}"]
    try:
        async with aiofiles.open(file_path, mode='r', encoding='utf-8', errors='ignore') as f:
            q = deque(maxlen=n)
            async for line in f: q.append(line.strip())
            return list(q)
    except Exception as e:
        logger.error(f"Error reading log file {file_path}: {e}", exc_info=True)
        return [f"Error reading log file: {e}"]

# --- API Endpoints ---

# Existing Endpoints (Status, Logs, Trends)
@app.get("/status", summary="Check API and Elasticsearch connection status")
async def get_status(es_client: AsyncElasticsearch = Depends(get_es_client)):
    # (Status endpoint remains the same)
    status_data = {"api_status": "ok", "elasticsearch_status": "unavailable"}
    if not es_client: raise HTTPException(status_code=503, detail="ES client not initialized")
    try:
        if await es_client.ping(): status_data["elasticsearch_status"] = "ok"
        else: status_data["elasticsearch_status"] = "error: ping failed"
        return status_data
    except Exception as e:
        logger.error(f"Status check error: {e}", exc_info=True)
        raise HTTPException(status_code=503, detail=f"ES connection error: {str(e)}")


@app.get("/logs/{log_name}", summary="Get tail logs from specified pipeline component")
async def get_logs(
    log_name: str = Path(..., description=f"Log file name. Valid: {', '.join(LOG_FILES.keys())}"),
    lines: int = Query(MAX_LOG_LINES, ge=1, le=1000)
):
    # (Logs endpoint remains the same)
    if log_name not in LOG_FILES: raise HTTPException(status_code=404, detail="Log file not recognized.")
    file_path = LOG_FILES[log_name]
    log_lines = await read_log_tail(file_path, n=lines)
    return {"log_name": log_name, "lines": log_lines}

class TrendsQuery(BaseModel):
    start_date: date
    end_date: date
    index: str = "gkg*"
    entity_field: str = "V21AllNames.Name.keyword"
    date_field: str = "V2ExtrasXML.PubTimestamp"
    max_entities: int = Query(500, ge=10, le=10000)

@app.get("/trends", summary="Fetch aggregated entity trends from Elasticsearch")
async def get_trends(
    start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: date = Query(..., description="End date (YYYY-MM-DD)"),
    index: str = Query("gkg*", description="ES index pattern"),
    entity_field: str = Query("V21AllNames.Name.keyword", description="Entity field"),
    date_field: str = Query("V2ExtrasXML.PubTimestamp", description="Timestamp field"),
    max_entities: int = Query(500, ge=10, le=10000),
    es_client: AsyncElasticsearch = Depends(get_es_client)
):
    # (Trends endpoint remains the same)
    if end_date < start_date: raise HTTPException(status_code=400, detail="End date must be >= start date")
    if not es_client: raise HTTPException(status_code=503, detail="ES client not available")
    end_date_exclusive = end_date + timedelta(days=1)
    query = { # Query remains the same
        "size": 0, "query": {"range": {date_field: {"gte": start_date.strftime("%Y-%m-%d"),"lt": end_date_exclusive.strftime("%Y-%m-%d")}}},
        "aggs": {"entities_over_time": {"date_histogram": {"field": date_field,"calendar_interval": "1d","min_doc_count": 0},
                 "aggs": {"top_entities": {"terms": {"field": entity_field,"size": max_entities}}}}}
    }
    try:
        logger.info(f"Executing trends query on index '{index}' for {start_date} to {end_date}")
        response = await es_client.search(index=index, body=query, request_timeout=120)
        buckets = response.get('aggregations', {}).get('entities_over_time', {}).get('buckets', [])
        logger.info(f"Trends query successful, received {len(buckets)} daily buckets.")
        return {"buckets": buckets}
    except Exception as e:
        logger.error(f"Trends query error: {e}", exc_info=True)
        detail_msg = f"An error occurred fetching trends: {str(e)}"
        # (Error detail extraction logic remains the same)
        if hasattr(e, 'info') and isinstance(e.info, dict):
            try: reason = e.info.get('error', {}).get('root_cause', [{}])[0].get('reason', 'Unknown ES error')
            except: pass
            else: detail_msg = f"ES query failed: {reason}"
        raise HTTPException(status_code=500, detail=detail_msg)


# --- NEW Task Management Endpoints ---

class PatchRequest(BaseModel):
    look_back_days: int = Field(DEFAULT_PATCH_LOOKBACK_DAYS, ge=1)

@app.post("/api/v1/tasks/patch", status_code=202, summary="Start data patching task")
async def start_patching_task_endpoint(patch_request: PatchRequest):
    """Starts a background task to download missing GDELT files for the last N days."""
    global background_tasks
    task_id = f"patch_{int(time.time())}"
    stop_event = threading.Event()
    task_info = {
        "status": "starting", "message": "Patching task initializing...", "progress": 0,
        "errors": 0, "success": 0, "skipped": 0, "_event": stop_event
    }
    with tasks_lock: background_tasks[task_id] = task_info

    # Start the synchronous worker function in a standard thread
    thread = threading.Thread(
        target=run_patching_task_worker,
        args=(task_id, stop_event),
        kwargs={'look_back_days': patch_request.look_back_days},
        daemon=True # Allow app to exit even if tasks are running
    )
    thread.start()

    logger.info(f"Dispatched patching task {task_id} for {patch_request.look_back_days} days.")
    return {"message": "Patching task accepted.", "task_id": task_id}

class ArchiveRequest(BaseModel):
    start_date: date
    end_date: date

@app.post("/api/v1/tasks/archive", status_code=202, summary="Start data archive download task")
async def start_archive_task_endpoint(archive_request: ArchiveRequest):
    """Starts a background task to download GDELT files for a specific date range."""
    if archive_request.end_date < archive_request.start_date:
         raise HTTPException(status_code=400, detail="End date must be on or after start date")

    global background_tasks
    task_id = f"archive_{int(time.time())}"
    stop_event = threading.Event()
    task_info = {
        "status": "starting", "message": "Archive download task initializing...", "progress": 0,
        "errors": 0, "success": 0, "skipped": 0, "_event": stop_event
    }
    with tasks_lock: background_tasks[task_id] = task_info

    # Start the synchronous worker function in a standard thread
    thread = threading.Thread(
        target=run_patching_task_worker,
        args=(task_id, stop_event),
        kwargs={
            'start_date_str': archive_request.start_date.strftime("%Y-%m-%d"),
            'end_date_str': archive_request.end_date.strftime("%Y-%m-%d")
        },
        daemon=True
    )
    thread.start()

    logger.info(f"Dispatched archive task {task_id} for range {archive_request.start_date} to {archive_request.end_date}.")
    return {"message": "Archive download task accepted.", "task_id": task_id}


@app.get("/api/v1/tasks/status/{task_id}", summary="Get status of a background task")
async def get_task_status_endpoint(task_id: str):
    """Retrieves the current status, progress, and message for a given task ID."""
    with tasks_lock: task_data = background_tasks.get(task_id)

    if not task_data: raise HTTPException(status_code=404, detail="Task not found or expired")

    # Return all data except the internal event object
    status_response = {k: v for k, v in task_data.items() if k != '_event'}
    return status_response


@app.post("/api/v1/tasks/cancel/{task_id}", status_code=200, summary="Cancel a running task")
async def cancel_task_endpoint(task_id: str):
    """Requests cancellation of a running patching or archive task."""
    global background_tasks
    with tasks_lock:
        task_data = background_tasks.get(task_id)
        if not task_data: raise HTTPException(status_code=404, detail="Task not found")

        current_status = task_data.get("status")
        if current_status not in ["running", "starting"]:
             # Return 200 but indicate it wasn't running
             return {"message": f"Task already {current_status}. Cannot cancel.", "task_id": task_id}

        stop_event = task_data.get("_event")
        if stop_event and isinstance(stop_event, threading.Event):
            stop_event.set() # Signal the thread to stop
            task_data["status"] = "aborting"
            task_data["message"] = "Cancellation requested..."
            logger.info(f"Cancellation signal sent for task {task_id}")
            return {"message": "Cancellation requested.", "task_id": task_id}
        else:
            logger.error(f"Task {task_id} found but has no cancellation event or invalid event type!")
            raise HTTPException(status_code=500, detail="Internal error: Cannot find cancellation signal.")


# Root endpoint (as before)
@app.get("/", summary="API Root")
async def read_root():
    return {"message": "Welcome to the GDELT ELK Backend API V1.1"}

# --- Uvicorn run command (for direct execution, usually handled by Docker) ---
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)