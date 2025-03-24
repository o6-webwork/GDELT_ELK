from flask import Flask, render_template, jsonify, request
from datetime import datetime
import threading, time, os

app = Flask(__name__)

# Global logs list (in production consider using a thread-safe structure)
logs = []
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def simulate_task(task_name):
    """Simulate a task by appending log messages over time."""
    for i in range(1, 6):
        log_entry = f"{task_name}: Step {i}/5 completed"
        logs.append(log_entry)
        time.sleep(1)  # simulate time-consuming work
    logs.append(f"{task_name}: Completed")

@app.route('/')
def dashboard():
    # Example data for status panels (last 5 files)
    scraping_files = [
        {"filename": "scraped_file_1.csv", "timestamp": "2025-03-23 10:00:00", "error": False},
        {"filename": "scraped_file_2.csv", "timestamp": "2025-03-23 09:50:00", "error": False},
        {"filename": "scraped_file_3.csv", "timestamp": "2025-03-23 09:40:00", "error": True},
        {"filename": "scraped_file_4.csv", "timestamp": "2025-03-23 09:30:00", "error": False},
        {"filename": "scraped_file_5.csv", "timestamp": "2025-03-23 09:20:00", "error": False},
    ]
    ingestion_files = [
        {"filename": "ingested_file_1.csv", "timestamp": "2025-03-23 11:00:00", "error": False},
        {"filename": "ingested_file_2.csv", "timestamp": "2025-03-23 10:50:00", "error": False},
        {"filename": "ingested_file_3.csv", "timestamp": "2025-03-23 10:40:00", "error": True},
        {"filename": "ingested_file_4.csv", "timestamp": "2025-03-23 10:30:00", "error": False},
        {"filename": "ingested_file_5.csv", "timestamp": "2025-03-23 10:20:00", "error": False},
    ]
    return render_template("dashboard.html",
                           scraping_files=scraping_files,
                           ingestion_files=ingestion_files,
                           logs=logs)

@app.route('/patch_missing', methods=['POST'])
def patching_task(look_back_period):
    logs.append(f"{current_time} - Starting patching missing data with look back period of {look_back_period}")
    logs.append(f"{current_time} - Ending patching missing data with look back period of {look_back_period}")
    
def patch_missing():
    look_back_period = request.form.get("look_back_period")
    # Start a background thread for patching simulation
    threading.Thread(target=patching_task, look_back_period=look_back_period).start()
    return jsonify({"message": "Patching started", "look_back_period": look_back_period})

@app.route('/ingest_archival', methods=['POST'])
def ingestion_task(start_date, end_date):
    logs.append(f"{current_time} - Ingesting data from {start_date} to {end_date}")
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    files_within_date = []
    for file in os.listdir("archival_data"):
        if file.endswith(".CSV"):
            file_date_part = file[:8]
            file_date = datetime.strptime(file_date_part, "%Y%m%d")
            if start_date_obj <= file_date <= end_date_obj:
                files_within_date.append(file)
    for file in files_within_date:
        logs.append(f"{current_time} - Ingesting {file}")
        time.sleep(1)
    logs.append(f"{current_time} - Ingesting data from {start_date} to {end_date} completed")

def ingest_archival():
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    threading.Thread(target=ingestion_task, start_date=start_date, end_date=end_date).start()
    return jsonify({"message": "Ingestion started", "start_date": start_date, "end_date": end_date})

@app.route('/logs')
def get_logs():
    # Return the logs as a JSON list
    return jsonify(logs)

if __name__ == '__main__':
    app.run(debug=True)
