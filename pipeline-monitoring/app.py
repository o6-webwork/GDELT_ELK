from flask import Flask, render_template, jsonify, request
from datetime import datetime
import threading, time, os

app = Flask(__name__)
logs = []
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@app.route('/')
def dashboard():
    log_dir = ".\\app\\logs\\sincedb.txt"

    with open(log_dir, "r") as f:
        data = f.read().split("\n")

    data = [i.split(" ") for i in data]
    scraping_files = [
        {"filename": "scraped_file_1.csv", "timestamp": "2025-03-23 10:00:00", "error": False}
    ]
    ingestion_files = [
        {"filename": "ingested_file_1.csv", "timestamp": "2025-03-23 11:00:00", "error": False}
    ]
    return render_template("dashboard.html",
                           scraping_files=scraping_files,
                           ingestion_files=ingestion_files,
                           logs=logs)
    
if __name__ == '__main__':
    app.run(debug=True)
