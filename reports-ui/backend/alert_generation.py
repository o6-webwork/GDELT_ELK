import datetime
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from dateutil.relativedelta import relativedelta
import pytz
from datetime import timedelta


# --- Configuration ---
INDEX_NAME = "gkg"
TIMESTAMP_FIELD = "V21Date" 

PARAM_SETS = {
    "1d": {
        "BASELINE_WINDOW": '7d', # Baseline window for rolling
        "MIN_PERIODS_BASELINE": 3,
        "DEFAULT_STD_DEV": 0.1,
        "MIN_STD_DEV": 0.5,
        "MIN_COUNT_FOR_ALERT": 5,
        "SPIKE_THRESHOLD": 2.0, # Your original default for daily
        # Consolidated Build Detection parameters
        "BUILD_WINDOW_PERIODS_COUNT": 3,  # Default: e.g., 3 days for daily interval
        "BUILD_THRESHOLD": 2.0,
    },
    "15m": {
        "BASELINE_WINDOW": '1d', # Default: Baseline window for rolling e.g., 1 day of 15-min data for baseline.
        "MIN_PERIODS_BASELINE": 12, # e.g., 3 hours of 15-min data
        "DEFAULT_STD_DEV": 0.05,
        "MIN_STD_DEV": 0.2,
        "MIN_COUNT_FOR_ALERT": 1, 
        "SPIKE_THRESHOLD": 2.5,    # Default for 15m
        # Consolidated Build Detection parameters
        "BUILD_WINDOW_PERIODS_COUNT": 8,  # Default: e.g., 8 * 15min = 2 hours for 15m interval
        "BUILD_THRESHOLD": 2.2,
    }
}

def get_interval_timedelta(interval_str: str) -> timedelta:
    if "m" in interval_str:
        return 0.99*timedelta(minutes=int(interval_str.replace("m", "")))
    elif "h" in interval_str:
        return timedelta(hours=int(interval_str.replace("h", "")))
    elif "d" in interval_str:
        return timedelta(days=int(interval_str.replace("d", "")))
    else:
        raise ValueError(f"Unsupported interval format: {interval_str}")
    

def sanitize_alert_info(alert_info_dict):
    """
    Converts NumPy numeric types in the main level of alert_result
    to Python native types for JSON serialization.
    """
    if alert_info_dict is None:
        return None

    sanitized_dict = {}
    for key, value in alert_info_dict.items():
        if isinstance(value, np.integer): # Catches np.int64, np.int32, etc.
            sanitized_dict[key] = int(value)
        elif isinstance(value, np.floating): # Catches np.float64, np.float32, etc.
            # Convert numpy NaN to None for better JSON compatibility, otherwise to float
            sanitized_dict[key] = None if np.isnan(value) else float(value)
        elif isinstance(value, float) and np.isnan(value): # Handle standard float NaN
            sanitized_dict[key] = None
        else:
            sanitized_dict[key] = value
    return sanitized_dict


def calculate_baseline_and_alerts(timeseries_df: pd.DataFrame, 
                                  current_interval_str: str, 
                                  task_custom_params: dict | None = None):

    # 1. Determine effective parameters
    # Start with defaults for the given interval
    effective_params = PARAM_SETS.get(current_interval_str, PARAM_SETS["15m"]).copy() # Default to "1d" if interval not in PARAM_SETS

    # Override with task-specific custom parameters if provided
    if task_custom_params:
        print(f"Task custom params received: {task_custom_params}")
        if task_custom_params.get("custom_baseline_window_pd_str") is not None:
            effective_params["BASELINE_WINDOW"] = task_custom_params["custom_baseline_window_pd_str"]
        if task_custom_params.get("custom_min_periods_baseline") is not None:
            effective_params["MIN_PERIODS_BASELINE"] = task_custom_params["custom_min_periods_baseline"]
        if task_custom_params.get("custom_min_count_for_alert") is not None:
            effective_params["MIN_COUNT_FOR_ALERT"] = task_custom_params["custom_min_count_for_alert"]
        
        # Spike params
        if task_custom_params.get("custom_spike_threshold") is not None:
            effective_params["SPIKE_THRESHOLD"] = task_custom_params["custom_spike_threshold"]
        
        # Build params (consolidated)
        if task_custom_params.get("custom_build_window_periods_count") is not None:
            effective_params["BUILD_WINDOW_PERIODS_COUNT"] = task_custom_params["custom_build_window_periods_count"]
        if task_custom_params.get("custom_build_threshold") is not None:
            effective_params["BUILD_THRESHOLD"] = task_custom_params["custom_build_threshold"]
        
        # Add other custom params overrides here if you defined more (e.g., DEFAULT_STD_DEV, MIN_STD_DEV)
        # For example:
        # if task_custom_params.get("custom_default_std_dev") is not None:
        #     effective_params["DEFAULT_STD_DEV"] = task_custom_params["custom_default_std_dev"]
        # if task_custom_params.get("custom_min_std_dev") is not None:
        #     effective_params["MIN_STD_DEV"] = task_custom_params["custom_min_std_dev"]


    print(f"Effective parameters for interval '{current_interval_str}': {effective_params}")

    # Use these effective_params throughout the function
    min_periods_baseline_val = effective_params["MIN_PERIODS_BASELINE"]
    baseline_window_for_rolling_val = effective_params["BASELINE_WINDOW"]
    default_std_dev_val = effective_params["DEFAULT_STD_DEV"]
    min_std_dev_val = effective_params["MIN_STD_DEV"]
    min_count_for_alert_val = effective_params["MIN_COUNT_FOR_ALERT"]
    spike_threshold_val = effective_params["SPIKE_THRESHOLD"]
    build_window_periods_val = effective_params["BUILD_WINDOW_PERIODS_COUNT"]
    build_threshold_val = effective_params["BUILD_THRESHOLD"]


    if timeseries_df.empty or len(timeseries_df) < min_periods_baseline_val + 1:
        print("Insufficient data for baseline calculation.")
        return None

    # Ensure timestamp is the index for rolling calculations
    if not isinstance(timeseries_df.index, pd.DatetimeIndex):
        timeseries_df['timestamp'] = pd.to_datetime(timeseries_df['timestamp'])
        timeseries_df = timeseries_df.set_index('timestamp')


    # Calculate rolling mean and std dev for the baseline
    # shift(1) ensures baseline uses past data only
    baseline_mean = timeseries_df['count'].rolling(
        window=baseline_window_for_rolling_val, min_periods=min_periods_baseline_val
    ).mean().shift(1)


    baseline_std = timeseries_df['count'].rolling(
        window=baseline_window_for_rolling_val, min_periods=min_periods_baseline_val
    ).std().shift(1)


    # Handle potential NaN values at the start or zero std dev
    baseline_std = baseline_std.fillna(default_std_dev_val)
    baseline_std[baseline_std < min_std_dev_val] = min_std_dev_val  # Enforce minimum std dev
    baseline_mean = baseline_mean.fillna(timeseries_df['count'].mean())  # Fill initial NaNs


    # Get the latest data point and its corresponding baseline values
    latest_data = timeseries_df.iloc[-1]
    latest_mean = baseline_mean.iloc[-1]
    latest_std = baseline_std.iloc[-1]
    latest_count = latest_data['count']


    alert_info = {
        'timestamp': latest_data.name.isoformat(),
        'count': latest_count,
        'baseline_mean': latest_mean,
        'baseline_std': latest_std,
        'alert_triggered': False,
        'alert_type': None,
        'reason': None,                  
        'current_interval': current_interval_str,
        'params_used': effective_params # Store the actual parameters used for this check
    }

    if pd.isna(latest_count) or latest_count < min_count_for_alert_val: # Use effective value
        message = f"Latest count ({latest_count}) is below MIN_COUNT_FOR_ALERT ({min_count_for_alert_val}) or invalid. No formal alert."
        print(message)
        alert_info['message'] = message
        return sanitize_alert_info(alert_info)

    # --- Alerting Logic ---
    # 1. Spike Detection (Z-score)
    if latest_std > 0:  # Avoid division by zero
        z_score = (latest_count - latest_mean) / latest_std
        alert_info['z_score'] = z_score
        if z_score > spike_threshold_val:
            alert_info['alert_triggered'] = True
            alert_info['alert_type'] = 'Spike (Z-Score)'
            alert_info['reason'] = f"Z-score ({z_score:.2f}) exceeded threshold ({spike_threshold_val})"
            print(f"--- ALERT DETECTED ---")
            print(f"Timestamp: {alert_info['timestamp']}")
            print(f"Type: {alert_info['alert_type']}")
            print(f"Reason: {alert_info['reason']}")
            print(
                f"Count: {latest_count}, Baseline Mean: {latest_mean:.2f}, Baseline StdDev: {latest_std:.2f}")
            return alert_info  # Return first triggered alert for simplicity
        
    # 2. Short Build Detection (Type 2) 
    # Ensure enough data points for the build window and valid latest_mean
    if len(timeseries_df) >= build_window_periods_val and pd.notna(latest_mean) and latest_mean > 0:
        # Sum of counts over the BUILD_WINDOW_PERIODS including the latest point
        # min_periods=1 ensures a sum is calculated even if the window is not full (at the start of the series)
        # though with len(timeseries_df) >= BUILD_WINDOW_PERIODS, it's likely full.
        recent_sum = timeseries_df['count'].rolling(window=build_window_periods_val, min_periods=1).sum().iloc[-1]
        expected_sum = latest_mean * build_window_periods_val # Expected sum based on the baseline mean for the *current* latest point

        if expected_sum > 0: # Avoid division by zero
            build_ratio = recent_sum / expected_sum
            alert_info['build_ratio'] = build_ratio # Store build_ratio
            if build_ratio > build_threshold_val:
                alert_info['alert_triggered'] = True
                alert_info['alert_type'] = 'Short Build'
                alert_info['reason'] = f"Build ratio ({build_ratio:.2f}) over {build_window_periods_val} periods exceeded threshold ({build_threshold_val})"
                print(f"--- ALERT DETECTED ---")
                print(f"Timestamp: {alert_info['timestamp']}")
                print(f"Type: {alert_info['alert_type']}")
                print(f"Reason: {alert_info['reason']}")
                print(f"Count: {latest_count}, Recent Sum ({build_window_periods_val} periods): {recent_sum:.2f}, Expected Sum: {expected_sum:.2f}, Baseline Mean: {latest_mean:.2f}")
                return alert_info # Return if short build is detected

    return None  # No alert

def get_es_lookback_duration(interval_str: str, custom_baseline_window_str: str | None = None) -> relativedelta:
    # Use custom baseline window if provided, otherwise use default from PARAM_SETS for the interval
    if custom_baseline_window_str:
        effective_baseline_window_str = custom_baseline_window_str
        print(f"Using custom baseline window for ES lookback: {effective_baseline_window_str}")
    else:
        params_for_interval = PARAM_SETS.get(interval_str, PARAM_SETS["1d"])
        effective_baseline_window_str = params_for_interval["BASELINE_WINDOW"]
        print(f"Using default baseline window for ES lookback ('{interval_str}' -> '{effective_baseline_window_str}')")

    # Convert pandas time string to relativedelta for ES query
    # Add a buffer (e.g., 1 day or a few hours for smaller intervals)
    buffer = relativedelta(days=1) if interval_str == "1d" else relativedelta(hours=6) # General buffer
    
    if 'd' in effective_baseline_window_str:
        days = int(effective_baseline_window_str.replace('d', ''))
        return relativedelta(days=days) + buffer
    elif 'h' in effective_baseline_window_str:
        hours = int(effective_baseline_window_str.replace('h', ''))
        # Buffer for hourly might be smaller if baseline is very short
        buffer_h = relativedelta(hours=max(1, hours // 4)) # e.g. 1/4th of baseline, min 1h
        return relativedelta(hours=hours) + buffer_h
    else: 
        print(f"Warning: Could not parse effective_baseline_window_str '{effective_baseline_window_str}', defaulting ES lookback to 7 days + buffer.")
        return relativedelta(days=7) + buffer

def check_alerts_for_query(es: Elasticsearch, 
                           user_query_dict: dict, 
                           end_datetime_obj: datetime.datetime, # SGT time
                           interval_str: str = "1d", 
                           task_custom_params: dict | None = None
                           ) -> tuple[dict | None, datetime.datetime | None, datetime.datetime | None]: # Return: alert_result, query_start_utc, query_end_utc

    
    """
    Connects to Elasticsearch, runs the user query with aggregations,
    and checks for alerts based on the results.

    Args:
        es (Elasticsearch): Elasticsearch client instance.
        user_query_dict (dict): The Elasticsearch query dictionary from Streamlit.
                                (e.g., {"query": {"query_string": {"query": "...", "fields": ["*"]}}})
    """
    try:
        # to remove
        print(user_query_dict)

        # 1. Extract the user's query string and fields
        user_query_string = user_query_dict.get("query", {}).get("query_string", {}).get("query")
        fields = user_query_dict.get("query", {}).get("query_string", {}).get("fields", ["*"])

        # to remove
        print('query string', user_query_string)
        print('fields:', fields)

        if not user_query_string:
            print("Error: No query string provided in the Elasticsearch query.")
            return

        print('end time', end_datetime_obj)
        print('interval str: ', interval_str)

        effective_interval_str = interval_str # Placeholder if custom interval becomes a thing

        # Get ES lookback based on effective baseline window (custom or default for the interval)
        custom_baseline_win_str = task_custom_params.get("custom_baseline_window_pd_str") if task_custom_params else None
        lookback_duration = get_es_lookback_duration(effective_interval_str, custom_baseline_win_str)
        start_datetime_obj = end_datetime_obj - lookback_duration
    

        query_body = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        user_query_dict["query"],
                        {
                            "range": {
                                TIMESTAMP_FIELD: {
                                    "gte": start_datetime_obj.isoformat(),
                                    "lte": end_datetime_obj.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "counts_over_time": {
                    "date_histogram": {
                        "field": TIMESTAMP_FIELD,
                        # Use fixed_interval for '15m', calendar_interval for '1d'
                        ("fixed_interval" if "m" in interval_str or "h" in interval_str else "calendar_interval"): interval_str,
                        "min_doc_count": 0,
                        "time_zone": "Asia/Singapore"
                    }
                }
            }
        }

        print(query_body)

        # 4. Execute the search query
        response = es.search(index=INDEX_NAME, body=query_body)


        # 5. Extract aggregation results and process data
        buckets = response['aggregations']['counts_over_time']['buckets']

        # For plotting of graph on streamlit
        timeseries_data_for_graph = [] 


        if not buckets:
            print("No data returned from Elasticsearch for the given query and time range.")
            return 

        data = []
        for bucket in buckets:
            # ES returns timestamp in ms if it's from epoch, or string. pd.to_datetime handles it.
            # 'key_as_string' is usually preferred for date_histogram if available and formatted.
            # 'key' is the epoch milliseconds.
            dt_from_key = pd.to_datetime(bucket['key'], unit='ms', utc=True).tz_convert('Asia/Singapore')
            count = bucket['doc_count']
            data.append((dt_from_key, count))
            timeseries_data_for_graph.append({'timestamp': dt_from_key.isoformat(), 'count': count})

            
        timeseries_df = pd.DataFrame(data, columns=['timestamp', 'count'])
        # Ensure it's sorted, though date_histogram usually returns sorted buckets
        timeseries_df = timeseries_df.sort_values(by='timestamp').reset_index(drop=True)

        print("\nTime Series Data:")
        print(timeseries_df.tail())


        # 6. Calculate baseline and check for alerts
        alert_result = calculate_baseline_and_alerts(timeseries_df, interval_str, task_custom_params)

        sanitized_alert_result = sanitize_alert_info(alert_result)

        # to remove
        print(f"alert result: {alert_result}")
        print(sanitized_alert_result)


        if sanitized_alert_result:
            print("\n--- Alert Summary ---")
            # Integrate with your alerting system here
            print(f"ALERT: {alert_result['alert_type']} triggered at {alert_result['timestamp']}")
            print(f"Details: {alert_result['reason']}")
            
            sanitized_alert_result['timeseries_data'] = timeseries_data_for_graph
            return sanitized_alert_result, start_datetime_obj, end_datetime_obj
    
        else:
            return {"timeseries_data": timeseries_data_for_graph, "alert_triggered": False}, start_datetime_obj, end_datetime_obj
            print("\nNo alert conditions met for the latest interval.")


    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()


# if __name__ == "__main__":
#     # # Example Usage (for testing the module directly)
#     # es = Elasticsearch(ELASTICSEARCH_HOSTS)
#     # if not es.ping():
#     #     raise ValueError("Connection to Elasticsearch failed")


#     # # Example user query (mimicking Streamlit input)
#     # user_query_from_streamlit = {
#     #     "query": {
#     #         "query_string": {
#     #             "query": "V2EnhancedThemes.V2Theme:(ARMEDCONFLICT OR TERROR)",
#     #             "fields": ["V2EnhancedThemes.V2Theme"]
#     #         }
#     #     }
#     # }

#     check_alerts_for_query(es, user_query_from_streamlit)