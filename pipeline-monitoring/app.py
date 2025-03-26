import os, datetime, requests, logging, time, zipfile
from io import BytesIO
from flask import Flask, render_template, jsonify, request
import threading 
app = Flask(__name__)

log_file = os.environ.get("LOG_FILE_PATH", "./logs/log.txt")
download_folder = "./csv"

def write(content):
    """Write log data into log file."""
    with open(log_file, "a") as f:
        f.write(content + "\n")

@app.route('/')
def dashboard():
    with open(log_file, "r") as f:
        data = f.read().split("\n")
    return render_template("dashboard.html", data=data)

def get_pipeline_status(pipeline):
    """
    Parse the log file to determine the status of the given pipeline.
    The function looks for log entries containing the pipeline name and specific keywords:
      - "started" indicates the process is running.
      - "completed" indicates the process is idle.
      - "error" indicates an error occurred.
    """
    status = "idle"
    if not os.path.exists(log_file):
        return status

    with open(log_file, "r") as f:
        for line in f:
            if pipeline.lower() in line.lower():
                lower_line = line.lower()
                if "error" in lower_line:
                    status = "error"
                elif "extracting" or "processing" in lower_line:
                    status = "running"
                elif "completed" in lower_line:
                    status = "idle"
    return status

@app.route('/status',methods=['GET'])
def status():
    # Read the log file and determine each pipeline's status
    scraping_status = get_pipeline_status("scraping")
    ingestion_status = get_pipeline_status("ingestion")
    return jsonify({
        "scraping": scraping_status,
        "ingestion": ingestion_status
    })

def patching_task(look_back_days=3, base_url="http://data.gdeltproject.org/gdeltv2/"):
    
    now = datetime.datetime.now()
    start = now - datetime.timedelta(days=int(look_back_days))
    
    start = start.replace(second=0, microsecond=0)
    start_adjust = start.minute % 15
    if start_adjust != 0:
        start = start - datetime.timedelta(minutes=start_adjust)
    
    current = start
    while current <= now:
        # Create a timestamp string: YYYYMMDDHHMMSS with seconds always "00"
        timestamp = current.strftime("%Y%m%d%H%M%S")
        file_url = f"{base_url}{timestamp}.export.CSV.zip"
        local_filename = f"{timestamp}.export.CSV"
        write(f"Patching {local_filename}...")
        
        try:
            response = requests.get(file_url, stream=True, timeout=10)
            if response.status_code == 200:
                zip_file = zipfile.ZipFile(BytesIO(response.content))
                zip_file.extract(local_filename, download_folder)
                write(f'Patched {local_filename}.')
            else:
                write(f"File not found or error {response.status_code} for URL: {file_url}")
        except Exception as e:
            write(f"Error patching {local_filename}: {e}")
        
        current += datetime.timedelta(minutes=15)
    
    write(f"Patching files from {look_back_days} days ago completed.")
    return jsonify({"message": f"Patching files from {look_back_days} days ago completed."})

@app.route('/patching', methods=['POST'])
def patch_missing():
    look_back_days = request.form.get("look_back_days")
    threading.Thread(target=patching_task, args=(look_back_days,)).start()
    return jsonify({"message": "Patching started", "look_back_days": look_back_days})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7979, debug=True)
