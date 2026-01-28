# GDELT_ELK/polling_service/polling_service.py
import os
import time
import datetime
import pytz
from sqlalchemy.orm import Session
from elasticsearch import Elasticsearch
import sys
import pandas as pd

# Assuming database.py and alert_generation.py are now in a 'shared_code' subdirectory
# and that directory is in PYTHONPATH (see Dockerfile)
# If you structured it differently (e.g., as an installable package), adjust imports
try:
    from shared_code.database import SessionLocal, MonitoredTask, AlertHistory # Get your SQLAlchemy models and session
    from shared_code.alert_generation import check_alerts_for_query # Your core alerting logic
except ModuleNotFoundError:
    print("ERROR: Could not import shared_code. Ensure alert_generation.py and database.py are accessible.")
    print("       Check Dockerfile COPY paths and PYTHONPATH environment variable if running in Docker.")
    # Fallback for local testing if files are in a sibling directory structure
    sys.path.append(os.path.join(os.path.dirname(__file__), '../reports-ui/backend'))
    from database import SessionLocal, MonitoredTask, AlertHistory
    from alert_generation import check_alerts_for_query


ES_HOST = "https://es01:9200"
ES_USERNAME = "elastic"
ES_PASSWORD = "changeme"

es_client = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USERNAME, ES_PASSWORD),
    verify_certs=False 
)

POLL_INTERVAL_SECONDS = 60 # Check for due tasks every 60 seconds
INGESTION_GRACE_PERIOD_SECONDS = 120
SGT_TIMEZONE = pytz.timezone("Asia/Singapore")

def get_latest_bucket_end_time(reference_time: datetime.datetime, interval_minutes: int) -> datetime.datetime:
    """Calculates the end time of the bucket that the reference_time falls into."""
    if interval_minutes <= 0:
        raise ValueError("Interval must be positive.")
    
    interval_seconds = interval_minutes * 60

    # Converts the input datetime object into a Unix timestamp (the total number of seconds that have passed since the 1970 epoch).
    reference_timestamp = reference_time.timestamp()

    # seconds that have passed since the start of the current bucket.x
    remainder = reference_timestamp % interval_seconds

    current_bucket_start_ts = reference_timestamp - remainder
    last_completed_bucket_end_ts = current_bucket_start_ts
    return datetime.datetime.fromtimestamp(last_completed_bucket_end_ts, tz=pytz.utc)

def fetch_active_monitored_tasks(db: Session) -> list[MonitoredTask]:
    """Fetches all active tasks from the database."""
    return db.query(MonitoredTask).filter(MonitoredTask.is_active).all()
    # active_tasks = db.query(MonitoredTask).filter(MonitoredTask.is_active == True).order_by(MonitoredTask.last_checked_at).all()


def process_single_task(db: Session, task: MonitoredTask):
    """
    Checks for and processes all completed buckets for a single task since its last check.
    """
    current_time_sgt_str = datetime.datetime.now(SGT_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{current_time_sgt_str}] Processing task ID {task.id}: '{task.query_string[:50]}...'")

    interval_minutes = task.interval_minutes
    now_utc = datetime.datetime.now(pytz.utc)

    # We can only check buckets that have fully passed PLUS a grace period for data ingestion
    check_limit_time = now_utc - datetime.timedelta(seconds=INGESTION_GRACE_PERIOD_SECONDS)

    # Determine the end time of the single most recent, fully completed bucket
    latest_completed_bucket_end = get_latest_bucket_end_time(check_limit_time, interval_minutes)

    # Get the last check time and ensure it's timezone-aware (UTC)
    last_checked_utc = None
    if task.last_checked_at:
        last_checked_utc = pd.to_datetime(task.last_checked_at).astimezone(pytz.utc)

    # Check completed bucket
    if last_checked_utc is None or latest_completed_bucket_end > last_checked_utc:
        (f"Task {task.id}: New completed bucket found ending at {latest_completed_bucket_end.astimezone(SGT_TIMEZONE).isoformat()}. Proceeding with check.")
        
        # Convert UTC bucket end time to SGT for the API call, as check_alerts_for_query expects SGT
        end_datetime_for_check_sgt = latest_completed_bucket_end.astimezone(SGT_TIMEZONE) - datetime.timedelta(seconds = 1)

        task_custom_params = {k: v for k, v in task.__dict__.items() if k.startswith('custom_') and v is not None}
        if not task_custom_params:
            task_custom_params = None

        try:
            alert_result, start_time, end_time = check_alerts_for_query(
                es_client,
                {"query": {"query_string": {"query": task.query_string, "fields": ["*"]}}},
                end_datetime_for_check_sgt,
                interval_str=f"{interval_minutes}m",
                task_custom_params=task_custom_params,
            )
            
            time_of_check = datetime.datetime.now(pytz.utc)
            if not alert_result:
                print(f"Task ID {task.id}: No alert result structure returned for bucket check.")

            elif alert_result.get('alert_triggered'):
                print(f"ALERT TRIGGERED for task {task.id}: {alert_result.get('alert_type')} - {alert_result.get('reason')}")
                # save alert to the AlertHistory table
                alert_ts_iso = alert_result.get('timestamp')
                db_alert_timestamp_utc = datetime.datetime.now(pytz.utc) # Default
                if alert_ts_iso:
                    try:
                        sgt_alert_timestamp = datetime.datetime.fromisoformat(alert_ts_iso)
                        db_alert_timestamp_utc = sgt_alert_timestamp.astimezone(pytz.utc)
                    except ValueError:
                        print(f"Could not parse alert timestamp '{alert_ts_iso}'.")
                
                drop_keys = ['timestamp', 'alert_triggered', 'params_used', 'timeseries_data']
                for key in drop_keys:
                    alert_result.pop(key, None)

                new_alert = AlertHistory(task_id=task.id, alert_timestamp=db_alert_timestamp_utc, recorded_at=time_of_check, **alert_result)
                db.add(new_alert)
                print(f"Saved alert to AlertHistory for task ID {task.id}")
            else:
                print(f"No alert for task {task.id} in bucket ending at {end_datetime_for_check_sgt.isoformat()}.")

            # task.last_checked_at = latest_completed_bucket_end # This is a UTC datetime
            task.last_checked_at = time_of_check # This is a UTC datetime

            db.commit()
            print(f"Task {task.id}: Updated last_checked_at to {latest_completed_bucket_end.isoformat()}")

        except Exception as e:
            print(f"ERROR processing task ID {task.id} for bucket {latest_completed_bucket_end.isoformat()}: {e}")
            import traceback
            traceback.print_exc()
            db.rollback()
    else:
        print(f"Task {task.id}: No new completed bucket to check. Last checked at {last_checked_utc.isoformat() if last_checked_utc else 'never'}.")

    # ----- 

def main_polling_loop():
    print(f"Polling service started at {datetime.datetime.now(SGT_TIMEZONE).isoformat()}. Checking for due tasks every {POLL_INTERVAL_SECONDS} seconds.")
    while True:
        db = SessionLocal() # Create a new session for this polling cycle
        try:
            print("--- Starting new polling cycle ---")
            active_tasks = fetch_active_monitored_tasks(db)
            print(f"Found {len(active_tasks)} active tasks to check.")

            if active_tasks:
                for task in active_tasks:
                    process_single_task(db, task) 
            else: 
                print(f"No tasks due for checking at {datetime.datetime.now(SGT_TIMEZONE).isoformat()}.")
        
        except Exception as e:
            print(f"CRITICAL Error in main polling loop: {e}")
            import traceback
            traceback.print_exc()
            if db: 
                db.rollback()
        finally:
            if db: # Ensure session is closed
                db.close()
            print(f"--- Cycle finished. Sleeping for {POLL_INTERVAL_SECONDS}s ---")
        
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    print("Polling service attempting to start...")
    # Brief delay to allow other services (like DB) to potentially start up if launched together
    # In Docker with depends_on and healthcheck, this might be less critical but doesn't hurt.
    initial_wait = int(os.getenv("POLLING_SERVICE_INITIAL_WAIT", "15"))
    print(f"Polling service waiting {initial_wait}s for DB and ES to be ready...")
    time.sleep(initial_wait)
    
    # Test DB connection once before starting loop
    try:
        db_test_session = SessionLocal()
        db_test_session.query(MonitoredTask).first() # Simple query to test connection
        db_test_session.close()
        print("Database connection successful.")
    except Exception as e:
        print(f"CRITICAL: Database connection test failed: {e}")
        print("Ensure PostgreSQL is running and DATABASE_URL is correct.")
        print(f"DATABASE_URL used: {os.getenv('DATABASE_URL', 'Not Set')}")
        sys.exit(1)

    # Test ES connection
    try:
        if not es_client.ping():
            raise ConnectionError("Elasticsearch ping failed.")
        print("Elasticsearch connection successful.")
    except Exception as e:
        print(f"CRITICAL: Elasticsearch connection test failed: {e}")
        print(f"Ensure Elasticsearch is running at {ES_HOST}.")
        sys.exit(1)

    main_polling_loop()