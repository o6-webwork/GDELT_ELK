from flask import Flask, render_template, jsonify, request
from datetime import datetime
import threading, time, os

app = Flask(__name__)
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@app.route('/')
def dashboard():
    log_dir = ".\\app\\logs\\log.txt"

    with open(log_dir, "r") as f:
        data = f.read()
    return render_template("dashboard.html", data=data)
    
if __name__ == '__main__':
    app.run(debug=True)
