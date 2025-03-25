import os
import logging
from flask import Flask, render_template, jsonify

app = Flask(__name__)

log_file = os.environ.get("LOG_FILE_PATH", "./logs/log.txt")

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
                elif "started" in lower_line:
                    status = "running"
                elif "completed" in lower_line:
                    status = "idle"
    return status

@app.route('/status')
def status():
    # Read the log file and determine each pipeline's status
    scraping_status = get_pipeline_status("scraping")
    ingestion_status = get_pipeline_status("ingestion")
    return jsonify({
        "scraping": scraping_status,
        "ingestion": ingestion_status
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7979, debug=True)
