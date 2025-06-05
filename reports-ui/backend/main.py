from fastapi import FastAPI, Query, Body, Depends, HTTPException
from typing import Optional, List
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import pymannkendall as mk
from datetime import datetime
from datetime import time
import datetime as dt
from elasticsearch import Elasticsearch
import pytz
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session


from data_loader import get_fields_from_elasticsearch, load_data_from_elasticsearch
from alert_generation import check_alerts_for_query, get_interval_timedelta, PARAM_SETS
from database import SessionLocal, engine, Base, MonitoredTask, AlertHistory, get_db, create_db_tables

ES_INDEX = "gkg"
ES_HOST = "https://es01:9200"
ES_USERNAME = "elastic"
ES_PASSWORD = "changeme"

class AlertAcknowledgeResponse(BaseModel):
    alert_id: int
    task_id: int
    is_acknowledged: bool
    acknowledged_at: dt.datetime | None = None # Optional: timestamp of acknowledgement

class MonitoredTaskBase(BaseModel):
    query_string: str
    interval_minutes: int = Field(default=15, ge=1) # Default to 15 min, ensure >= 15 min

    # Custom Alert Parameters 
    custom_baseline_window_pd_str: str | None = Field(default=None, examples=["7d", "1d", "24h"], title="Custom Baseline Window (Pandas Offset String)")
    custom_min_periods_baseline: int | None = Field(default=None, ge=1, title="Custom Min Periods for Baseline")
    custom_min_count_for_alert: int | None = Field(default=None, ge=0, title="Custom Min Count for Alert")
    custom_spike_threshold: float | None = Field(default=None, ge=0, title="Custom Spike Threshold (Z-score)")
    # Consolidated "Build Detection" parameters
    custom_build_window_periods_count: int | None = Field(default=None, ge=1, title="Custom Build Window (No. of Intervals)")
    custom_build_threshold: float | None = Field(default=None, ge=0, title="Custom Build Threshold (Ratio)")
    
class MonitoredTaskCreate(MonitoredTaskBase): 
    user_identifier: str | None = None # for user management; to eventually store user ID or username. optional for now

class LatestAlertInfo(BaseModel):
    alert_id: int | None = Field(None, alias='id') 
    task_id: int 
    count: int
    alert_type: str | None = None
    reason: str | None = None
    alert_timestamp: dt.datetime | None = None
    recorded_at: dt.datetime | None = None # When the alert was logged in DB
    is_acknowledged: bool | None = None

    class Config:
        from_attributes = True

class MonitoredTaskResponse(MonitoredTaskBase):
    id: int
    last_checked_at: dt.datetime | None
    created_at: dt.datetime
    is_active: bool
    user_identifier: str | None = None
    latest_alert: LatestAlertInfo | None = None 

    class Config:
        from_attributes = True

# Creating Elasticsearch client
es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USERNAME, ES_PASSWORD),
    verify_certs=False  
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/fields")
def get_fields():
    fields = get_fields_from_elasticsearch(ES_INDEX)
    return fields

@app.get("/trends")
def get_trends(
    start_date: str = Query(..., example="2025-04-01"),
    end_date: str = Query(..., example="2025-04-09"),
    trend_type: str = Query("increasing", enum=["increasing", "decreasing"]),
    top_n: int = Query(10, ge=1, le=50),
    entity_field: str = Query(..., example="V21AllNames.Name.keyword")
):
    df = load_data_from_elasticsearch(start_date, end_date, entity_field)

    pivot_df = df.pivot_table(index="date", columns="top_entity", values="count", fill_value=0)
    pivot_df.index = pd.to_datetime(pivot_df.index)

    trend_results = []
    for col in pivot_df.columns:
        result = mk.original_test(pivot_df[col])
        trend_results.append({
            "entity": col,
            "trend": result.trend,
            "p": result.p,
            "trend strength": result.Tau
        })

    trend_df = pd.DataFrame(trend_results)
    trend_df = trend_df[trend_df["p"] < 0.3]

    if trend_type == "increasing":
        trend_df = trend_df[trend_df["trend strength"] > 0].sort_values(by="trend strength", ascending=False)
    else:
        trend_df = trend_df[trend_df["trend strength"] < 0].sort_values(by="trend strength", ascending=True)

    return trend_df.head(top_n).to_dict(orient="records")

@app.get("/timeseries")
def get_timeseries(
    entities: str = "",  
    start_date: str = Query(..., example="2025-03-25"),
    end_date: str = Query(..., example="2025-04-09"),
    top_n: int = Query(10, ge=1, le=50),
    entity_field: str = Query(..., example="V21AllNames.Name.keyword")
):
    df = load_data_from_elasticsearch(start_date, end_date, entity_field)
    
    if df.empty:
        return {"error": "No data found for the given date range."}
    
    if not entities:
        trends = get_trends(start_date, end_date, "increasing", top_n, entity_field)
        entities = [trend["entity"] for trend in trends]
    else:
        entities = [e.strip() for e in entities.split(",") if e.strip()]

    df_subset = df[df['top_entity'].isin(entities)]
    if df_subset.empty:
        return {"error": "No data found for the specified entities."}

    df_pivot = df_subset.pivot_table(index="date", columns="top_entity", values="count", fill_value=0)
    if df_pivot.empty:
        return {"error": "No data found after pivoting."}

    df_pivot.reset_index(inplace=True)
    df_pivot["date"] = pd.to_datetime(df_pivot["date"]).dt.strftime("%Y-%m-%d")
    
    return df_pivot.to_dict(orient="records")


# Database Query Function for Historical Alerts 
async def get_historical_alerts_for_task_in_window( 
    db: Session, # Use synchronous Session to match get_db
    task_id: int, 
    start_dt_utc: Optional[datetime], 
    end_dt_utc: Optional[datetime]
) -> List[LatestAlertInfo]: # Return Pydantic schemas
    if not task_id or not start_dt_utc or not end_dt_utc:
        return []
    
    if start_dt_utc.tzinfo is not None:
        start_dt_utc = start_dt_utc.replace(tzinfo=None)
    if end_dt_utc.tzinfo is not None:
        end_dt_utc = end_dt_utc.replace(tzinfo=None)

    alerts_orm = db.query(AlertHistory).filter(
        AlertHistory.task_id == task_id,
        AlertHistory.alert_timestamp >= start_dt_utc,
        AlertHistory.alert_timestamp <= end_dt_utc
    ).order_by(AlertHistory.alert_timestamp.asc()).all()
    
    return [LatestAlertInfo.from_orm(alert) for alert in alerts_orm]


@app.post("/check_gdelt_alerts")
async def check_gdelt_alerts(
    user_query: dict = Body(...), 
    date_mode: str = Body(...),
    end_date: str = Body(...),  # Date string or "now"
    interval: str = Body(...),   # e.g., "1d" or "15m"  
    specific_alert_timestamp: Optional[str] = Body(None),
    task_id: Optional[int] = Body(None), 
    custom_baseline_window_pd_str: Optional[str] = Body(None),
    custom_min_periods_baseline: Optional[int] = Body(None),
    custom_min_count_for_alert: Optional[int] = Body(None),
    custom_spike_threshold: Optional[float] = Body(None),
    custom_build_window_periods_count: Optional[int] = Body(None),
    custom_build_threshold: Optional[float] = Body(None),
    db: Session = Depends(get_db) # Changed to synchronous Session based on your get_db
):
    """
    Endpoint to receive an Elasticsearch query from the frontend,
    run it, and check for alerts.
    """
    parsed_end_datetime = None
    sg_timezone = pytz.timezone('Asia/Singapore')
    utc_timezone = pytz.utc # Use UTC for backend processing and ES

    sg_timezone = pytz.timezone('Asia/Singapore')
    utc_timezone = pytz.utc # Recommended for internal processing if ES is in UTC

    query_end_datetime_sgt: datetime # This will be passed to check_alerts_for_query

    task_custom_params_dict = {
        "custom_baseline_window_pd_str": custom_baseline_window_pd_str,
        "custom_min_periods_baseline": custom_min_periods_baseline,
        "custom_min_count_for_alert": custom_min_count_for_alert,
        "custom_spike_threshold": custom_spike_threshold,
        "custom_build_window_periods_count": custom_build_window_periods_count,
        "custom_build_threshold": custom_build_threshold,
    }
    # Filter out None values so defaults in alert_generation.py apply
    task_custom_params_dict = {k: v for k, v in task_custom_params_dict.items() if v is not None}

    if specific_alert_timestamp:
        # Focused graph for a specific alert 
        try:
            alert_focus_dt_naive = pd.to_datetime(specific_alert_timestamp)
            
            # Ensure it's SGT as check_alerts_for_query expects end_datetime_obj in SGT
            if alert_focus_dt_naive.tzinfo is None:
                alert_focus_dt_sgt = utc_timezone.localize(alert_focus_dt_naive).astimezone(sg_timezone)
            else:
                alert_focus_dt_sgt = alert_focus_dt_naive.astimezone(sg_timezone)

            # Set the effective end time for the query slightly AFTER the alert
            # to ensure the alert data point and subsequent interval are well within the graph.
            interval_delta = get_interval_timedelta(interval)
            query_end_datetime_sgt = alert_focus_dt_sgt + interval_delta
            
            print(f"Focused graph mode: Original alert time (SGT)={alert_focus_dt_sgt}, Query End (SGT)={query_end_datetime_sgt}")

        except Exception as e:
            print(f"Error parsing specific_alert_timestamp '{specific_alert_timestamp}': {e}.")
            raise HTTPException(status_code=400, detail=f"Invalid specific_alert_timestamp format: {specific_alert_timestamp}")
    
    else:
        parsed_end_datetime_sgt: datetime
        if date_mode == "Live Monitoring":
            parsed_end_datetime_sgt = datetime.now(sg_timezone)
        else:  # Custom Date
            max_time_val: time
            now_in_sgt_for_calc = datetime.now(sg_timezone)
            custom_date_obj_for_check = datetime.strptime(end_date, "%Y-%m-%d").date()

            if interval != '1d':
                if custom_date_obj_for_check == now_in_sgt_for_calc.date(): # If selected date is today
                    max_time_val = now_in_sgt_for_calc.time() # Use current time
                else: # If selected date is in the past
                    max_time_val = time(23, 59, 59) # Use end of day
            else: # For '1d' interval
                max_time_val = time(23, 59, 59)
            
            native_datetime = datetime.combine(custom_date_obj_for_check, max_time_val)
            parsed_end_datetime_sgt = sg_timezone.localize(native_datetime)
        query_end_datetime_sgt = parsed_end_datetime_sgt
        print(f"General check mode: Query End (SGT)={query_end_datetime_sgt}")
    
    historical_alerts_response_data = [] 

    try:
        alert_check_main_result, query_start_time, query_end_time = check_alerts_for_query( # Assuming check_alerts_for_query is async
            es,
            user_query_dict=user_query, # Parameter name used in alert_generation.py
            end_datetime_obj=query_end_datetime_sgt, # This is in SGT
            interval_str=interval,
            task_custom_params=task_custom_params_dict if task_custom_params_dict else None
        )

        # If task_id is provided and we have a valid query window, fetch historical alerts
        if task_id and query_start_time and query_end_time:
            # Make get_historical_alerts_for_task_in_window async if db session is async
            # For now, assuming synchronous db session from get_db
            historical_alerts_response_data = await get_historical_alerts_for_task_in_window(
                db, task_id, query_start_time, query_end_time
            )

        # Construct the final response
        response_payload = {
            "timeseries_data": [], 
            "alert_triggered": False, 
            "reason": "No alert conditions met or no data.", 
            "metrics": {},
            "timestamp": datetime.now(sg_timezone).isoformat() # Default timestamp
        }

        print(f"response payload: {response_payload}")

        if alert_check_main_result: # If check_alerts_for_query returned some data/alert
            response_payload["timeseries_data"] = alert_check_main_result.get("timeseries_data", [])
            response_payload["alert_triggered"] = alert_check_main_result.get("alert_triggered", False)
            response_payload["reason"] = alert_check_main_result.get("reason")
            response_payload["metrics"] = {k: v for k, v in alert_check_main_result.items() if k not in ["timeseries_data", "alert_triggered", "reason", "timestamp", "current_interval", "params_used"]}
            if 'timestamp' in alert_check_main_result: # Timestamp of the primary alert, if any
                 response_payload["timestamp"] = alert_check_main_result.get("timestamp")
        
        response_payload["historical_alerts_data"] = historical_alerts_response_data
        response_payload["message"] = "Alert check completed."

        return response_payload

    except HTTPException: # Re-raise HTTPExceptions
        raise
    except Exception as e:
        print(f"Error calling check_alerts_for_query: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error during alert processing: {str(e)}")
    

@app.post("/monitoring/tasks", response_model=MonitoredTaskResponse, status_code=201, tags=["Monitoring Tasks"])
async def create_monitoring_task(task_in: MonitoredTaskCreate, db: Session = Depends(get_db)):
    """
    Add a new GDELT query to be monitored periodically.
    """
    print(f"Received request to create monitoring task for query: {task_in.query_string}")
    # Optional: Check for existing active task with the same query string
    existing_task = db.query(MonitoredTask).filter(
        MonitoredTask.query_string == task_in.query_string,
        MonitoredTask.is_active == True
    ).first()
    if existing_task:
        raise HTTPException(
            status_code=400, 
            detail=f"An active monitoring task with query '{task_in.query_string}' already exists (ID: {existing_task.id})."
        )
    
    # Create a dictionary from the input Pydantic model, excluding unset fields
    # This will correctly include all custom_... parameters if they were provided by the client.
    task_data_to_create = task_in.model_dump(exclude_unset=True) 

    # Add/override fields specific to the ORM model that are not directly in MonitoredTaskCreate's base
    # or that need to be set by the server.
    # last_checked_at will be NULL by default due to model change in database.py (nullable=True, no Python default)
    task_data_to_create['created_at'] = datetime.now(pytz.utc) #
    task_data_to_create['is_active'] = True #
    # user_identifier is already part of MonitoredTaskCreate

    db_task_orm = MonitoredTask(**task_data_to_create) # Create SQLAlchemy model instance
    
    db.add(db_task_orm) #
    db.commit() #
    db.refresh(db_task_orm) #
    print(f"Created new monitoring task ID: {db_task_orm.id} with custom params if provided.")

    # Construct the response data explicitly to ensure all fields are handled
    # This also ensures that latest_alert is None for a new task.
    response_data = {
        "id": db_task_orm.id, #
        "query_string": db_task_orm.query_string, #
        "interval_minutes": db_task_orm.interval_minutes, #
        "last_checked_at": db_task_orm.last_checked_at, # Will be None
        "created_at": db_task_orm.created_at, #
        "is_active": db_task_orm.is_active, #
        "user_identifier": db_task_orm.user_identifier, #
        "latest_alert": None, # Explicitly None for a newly created task
        
        # Add new custom parameters from the ORM object to the response
        "custom_baseline_window_pd_str": db_task_orm.custom_baseline_window_pd_str,
        "custom_min_periods_baseline": db_task_orm.custom_min_periods_baseline,
        "custom_min_count_for_alert": db_task_orm.custom_min_count_for_alert,
        "custom_spike_threshold": db_task_orm.custom_spike_threshold,
        "custom_build_window_periods_count": db_task_orm.custom_build_window_periods_count,
        "custom_build_threshold": db_task_orm.custom_build_threshold,
        # Removed extended build params as per simplification
    }
    return MonitoredTaskResponse.model_validate(response_data) # Use model_validate for Pydantic V2

@app.get("/monitoring/tasks", response_model=list[MonitoredTaskResponse], tags=["Monitoring Tasks"])
async def read_monitoring_tasks(
    skip: int = Query(0, ge=0), 
    limit: int = Query(100, ge=1, le=200), 
    active_only: bool = Query(True), 
    db: Session = Depends(get_db)
):
    """
    Retrieve a list of monitored GDELT queries.
    For each task, it will include the latest UNACKNOWLEDGED alert if one exists.
    If all alerts for a task are acknowledged, 'latest_alert' will be null.
    """
    query_obj = db.query(MonitoredTask)
    if active_only:
        query_obj = query_obj.filter(MonitoredTask.is_active == True)

    tasks_db = query_obj.order_by(MonitoredTask.created_at.desc()).offset(skip).limit(limit).all()
    
    response_tasks = []
    for task_db_orm in tasks_db: # Renamed task_db to task_db_orm for clarity
        latest_alert_db_obj = db.query(AlertHistory)\
                            .filter(AlertHistory.task_id == task_db_orm.id)\
                            .filter(AlertHistory.is_acknowledged == False)\
                            .order_by(AlertHistory.recorded_at.desc())\
                            .first() #
        
        # Construct the dictionary explicitly to ensure all fields for MonitoredTaskResponse are present
        task_response_data = { #
            "id": task_db_orm.id, #
            "query_string": task_db_orm.query_string, #
            "interval_minutes": task_db_orm.interval_minutes, #
            "last_checked_at": task_db_orm.last_checked_at, #
            "created_at": task_db_orm.created_at, #
            "is_active": task_db_orm.is_active, #
            "user_identifier": task_db_orm.user_identifier, #
            "latest_alert": latest_alert_db_obj, #
            
            # Add new custom parameters from the ORM object to the response dictionary
            "custom_baseline_window_pd_str": task_db_orm.custom_baseline_window_pd_str,
            "custom_min_periods_baseline": task_db_orm.custom_min_periods_baseline,
            "custom_min_count_for_alert": task_db_orm.custom_min_count_for_alert,
            "custom_spike_threshold": task_db_orm.custom_spike_threshold,
            "custom_build_window_periods_count": task_db_orm.custom_build_window_periods_count,
            "custom_build_threshold": task_db_orm.custom_build_threshold,
        }
        response_tasks.append(MonitoredTaskResponse.model_validate(task_response_data)) 
    return response_tasks 
    
@app.delete("/monitoring/tasks/{task_id}", response_model=MonitoredTaskResponse, tags=["Monitoring Tasks"])
async def deactivate_monitoring_task(task_id: int, db: Session = Depends(get_db)):
    """
    Deactivate (soft delete) a monitoring task by its ID.
    """
    db_task = db.query(MonitoredTask).filter(MonitoredTask.id == task_id).first()
    if db_task is None:
        raise HTTPException(status_code=404, detail="Monitoring task not found")
    if not db_task.is_active:
        # Optionally, you could just return the task if it's already inactive
        raise HTTPException(status_code=400, detail="Task is already inactive") 
    
    db_task.is_active = False
    db.commit()
    db.refresh(db_task)
    print(f"Deactivated monitoring task ID: {task_id}")
    return db_task

@app.post("/monitoring/alerts/{alert_id}/acknowledge", response_model=AlertAcknowledgeResponse, tags=["Monitoring Alerts"])
async def acknowledge_alert(alert_id: int, db: Session = Depends(get_db)):
    """
    Mark a specific alert as acknowledged.
    """
    alert_db = db.query(AlertHistory).filter(AlertHistory.id == alert_id).first()

    if not alert_db:
        raise HTTPException(status_code=404, detail="Alert not found")

    if alert_db.is_acknowledged:

        print(f"Alert ID {alert_id} was already acknowledged.")
    
    alert_db.is_acknowledged = True
    
    db.commit()
    db.refresh(alert_db)
    
    print(f"Acknowledged alert ID: {alert_id} for task ID: {alert_db.task_id}")
    
    response_data = {
        "alert_id": alert_db.id,
        "task_id": alert_db.task_id,
        "is_acknowledged": alert_db.is_acknowledged,
    }
    return response_data

@app.get("/api/v1/config/alert_param_sets")
async def get_alert_param_sets():
    return PARAM_SETS