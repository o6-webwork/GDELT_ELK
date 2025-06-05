# GDELT_ELK/polling_service/polling_service.py
import os
import time
import datetime
import pytz
from sqlalchemy.orm import Session
from elasticsearch import Elasticsearch
import sys

# Assuming database.py and alert_generation.py are now in a 'shared_code' subdirectory
# and that directory is in PYTHONPATH (see Dockerfile)
# If you structured it differently (e.g., as an installable package), adjust imports
try:
    from shared_code.database import SessionLocal, MonitoredTask, AlertHistory # Get your SQLAlchemy models and session
    from shared_code.alert_generation import check_alerts_for_query, sanitize_alert_info # Your core alerting logic
except ModuleNotFoundError:
    print("ERROR: Could not import shared_code. Ensure alert_generation.py and database.py are accessible.")
    print("       Check Dockerfile COPY paths and PYTHONPATH environment variable if running in Docker.")
    # Fallback for local testing if files are in a sibling directory structure
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), '../reports-ui/backend'))
    from database import SessionLocal, MonitoredTask, AlertHistory
    from alert_generation import check_alerts_for_query, sanitize_alert_info


ES_HOST = "https://es01:9200"
ES_USERNAME = "elastic"
ES_PASSWORD = "changeme"

es_client = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USERNAME, ES_PASSWORD),
    verify_certs=False 
)

POLL_INTERVAL_SECONDS = 60 # Check for due tasks every 60 seconds
SGT_TIMEZONE = pytz.timezone("Asia/Singapore")


def get_due_tasks(db: Session) -> list[MonitoredTask]:
    """
    Fetches tasks that are due for an alert check.
    A task is due if:
    1. last_checked_at IS NULL (never checked)
    OR
    2. last_checked_at + interval_minutes <= now_utc
    This is equivalent to: last_checked_at < now_utc - interval_minutes
    """
    now_utc = datetime.datetime.now(pytz.utc)
    print(f"[{now_utc.isoformat()}] Getting due tasks...")

    
    active_tasks = db.query(MonitoredTask).filter(MonitoredTask.is_active == True).order_by(MonitoredTask.last_checked_at).all()
    
    due_tasks_list = []
    for task in active_tasks:
        # Ensure task.last_checked_at is UTC aware if it came from DB without explicit tz handling by SQLAlchemy for this query
        is_due = False
        if task.last_checked_at is None:
            is_due = True
            print(f"  Task ID {task.id} is due (never checked).")

        else:
            # Ensure task.last_checked_at is UTC aware
            last_checked_utc = task.last_checked_at
            if last_checked_utc.tzinfo is None: # Should ideally be stored as aware by SQLAlchemy
                last_checked_utc = pytz.utc.localize(last_checked_utc)
            else:
                last_checked_utc = last_checked_utc.astimezone(pytz.utc)
            
            due_time = last_checked_utc + datetime.timedelta(minutes=task.interval_minutes)
            if now_utc >= due_time:
                is_due = True
                print(f"  Task ID {task.id} is due (Last checked: {last_checked_utc.isoformat()}, Interval: {task.interval_minutes}m, Due at: {due_time.isoformat()})")
        
        if is_due:
            due_tasks_list.append(task)

    if not due_tasks_list and active_tasks: # Only print if there were active tasks but none due
        print(f"  No tasks currently due among {len(active_tasks)} active tasks.")

    return due_tasks_list


def process_single_task(db: Session, task: MonitoredTask):
    """Processes a single monitoring task."""
    current_time_sgt_str = datetime.datetime.now(SGT_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{current_time_sgt_str}] Processing task ID {task.id}: '{task.query_string[:50]}...'")

    try:
        user_query_payload = {"query": {"query_string": {"query": task.query_string, "fields": ["*"]}}}
        
        # For live monitoring, end_datetime is always "now" in SGT for consistency with frontend expectation
        end_datetime_for_check = datetime.datetime.now(SGT_TIMEZONE)
        
        interval_string = f"{task.interval_minutes}m"

        # custom parameters from the MonitoredTask ORM object ---
        task_custom_params_from_db = {
            "custom_baseline_window_pd_str": task.custom_baseline_window_pd_str,
            "custom_min_periods_baseline": task.custom_min_periods_baseline,
            "custom_min_count_for_alert": task.custom_min_count_for_alert,
            "custom_spike_threshold": task.custom_spike_threshold,
            "custom_build_window_periods_count": task.custom_build_window_periods_count,
            "custom_build_threshold": task.custom_build_threshold,
        }
        # Filter out None values, so only explicitly set custom params are passed
        # This ensures that if a custom param is NULL in DB (None in ORM object),
        # the alert_generation logic will use its own internal defaults from PARAM_SETS.
        task_specific_params_for_alert_gen = {
            k: v for k, v in task_custom_params_from_db.items() if v is not None
        }
        if not task_specific_params_for_alert_gen: # If all custom params were None
            task_specific_params_for_alert_gen = None # Pass None to use full defaults for interval
        
        print(f" Task ID {task.id}: Using custom params: {task_specific_params_for_alert_gen if task_specific_params_for_alert_gen else 'None (will use defaults for interval)'}")

        alert_result, start_time, end_time = check_alerts_for_query(
            es_client,
            user_query_payload,
            end_datetime_for_check,
            interval_str=interval_string,
            task_custom_params=task_specific_params_for_alert_gen 
        )

        print(f"polling alert result: {alert_result}")

        if not alert_result: # check_alerts_for_query might return None or a dict
            print(f"  Task ID {task.id}: No alert result structure returned from check_alerts_for_query.")
            task.last_checked_at = datetime.datetime.now(pytz.utc) # Still update last_checked
            db.commit()
            return

        if alert_result.get('alert_triggered'):
            print(f"  ALERT TRIGGERED for task {task.id}: {alert_result.get('alert_type')} - {alert_result.get('reason')}")
            
            alert_ts_iso = alert_result.get('timestamp')
            # Convert alert timestamp from SGT (as returned by check_alerts_for_query) to UTC for DB
            db_alert_timestamp_utc = datetime.datetime.now(pytz.utc) # Default to now_utc if parsing fails
            if alert_ts_iso:
                try:
                    # The timestamp from alert_info should be SGT aware from alert_generation's processing
                    sgt_alert_timestamp = datetime.datetime.fromisoformat(alert_ts_iso)
                    db_alert_timestamp_utc = sgt_alert_timestamp.astimezone(pytz.utc)
                except ValueError:
                    print(f"  Warning: Could not parse alert timestamp '{alert_ts_iso}'. Using current UTC time for alert record.")


            new_alert = AlertHistory(
                task_id=task.id,
                alert_timestamp=db_alert_timestamp_utc,
                recorded_at=datetime.datetime.now(pytz.utc),
                alert_type=alert_result.get('alert_type'),
                reason=alert_result.get('reason'),
                count=alert_result.get('count'),
                baseline_mean=alert_result.get('baseline_mean'),
                baseline_std=alert_result.get('baseline_std'),
                z_score=alert_result.get('z_score'),
                build_ratio=alert_result.get('build_ratio'),
                extended_build_ratio=alert_result.get('extended_build_ratio'),
                current_interval_used=alert_result.get('current_interval')
            )
            db.add(new_alert)
            print(f"  Saved alert to AlertHistory for task ID {task.id}")
        else:
            print(f"  No alert for task {task.id}. Message: {alert_result.get('message', 'OK')}")

        task.last_checked_at = datetime.datetime.now(pytz.utc)
        db.commit()
        print(f"  Updated last_checked_at for task ID {task.id}")

    except Exception as e:
        print(f"  ERROR processing task ID {task.id}: {e}")
        import traceback
        traceback.print_exc()
        db.rollback() # Rollback session on error for this task
    finally:
        pass


def main_polling_loop():
    print(f"Polling service started at {datetime.datetime.now(SGT_TIMEZONE).isoformat()}. Checking for due tasks every {POLL_INTERVAL_SECONDS} seconds.")
    while True:
        db = None # Initialize db to None
        try:
            db = SessionLocal() # Create a new session for this polling cycle
            due_tasks = get_due_tasks(db)
            if due_tasks:
                print(f"Found {len(due_tasks)} due task(s) at {datetime.datetime.now(SGT_TIMEZONE).isoformat()}.")
                for task in due_tasks:
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
        print(f"Ensure Elasticsearch is running at {ES_HOST_URL}.")
        sys.exit(1)

    main_polling_loop()