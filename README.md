# GDELT ELK Pipeline (Refactored)

## Overview

This repository provides a comprehensive system for processing GDELT GKG data and visualizing trends. It automates the download of GDELT data, transformation using PySpark, ingestion into Elasticsearch via Logstash, and provides a web interface for monitoring, control, and analysis.

The project uses Docker Compose to manage the entire stack.

## Features

* **Automated ETL:** Periodically checks for new GDELT GKG data, downloads CSVs, transforms them to JSON using PySpark, and prepares them for ingestion.
* **Elasticsearch & Kibana:** Ingests processed data into Elasticsearch and allows exploration/visualization via Kibana.
* **Web Frontend (Streamlit):**
    * **Pipeline Monitoring:** View the status of backend components (API, Elasticsearch) and access ETL/Logstash logs.
    * **Manual Tasks:** Trigger background tasks to download missing GDELT data for recent days ("Patching") or specific historical date ranges ("Archive Download"). Monitor the progress of these tasks.
    * **Trend Analysis:** Analyze and visualize trends in entity mentions over selected time periods using the Mann-Kendall test.
* **Backend API (FastAPI):** Provides data and control endpoints for the frontend (status, logs, trends data, task management).

## Prerequisites

Before you begin, ensure you have the following installed:

* Docker
* Docker Compose (usually included with Docker Desktop)

## Setup & Running

1.  **Create `.env` File:**
    * Create a file named `.env` in the project root directory.
    * Define the necessary environment variables in this file. Refer to the `docker-compose.yml` file for a full list of variables used by the services. Essential variables include:
        ```dotenv
        # .env file content example

        # --- Required ---
        # Elastic Stack Version (e.g., 8.7.1 - ensure compatibility across components)
        STACK_VERSION=8.7.1

        # Elasticsearch Credentials (Choose strong passwords)
        ELASTIC_PASSWORD=your_strong_elastic_password

        # Kibana Credentials (Password for the kibana_system user)
        KIBANA_PASSWORD=your_strong_kibana_password

        # Kibana Encryption Key (Generate a random strong key, e.g., 32+ characters)
        ENCRYPTION_KEY=your_strong_random_encryption_key

        # --- Optional / Defaults ---
        # Cluster Name
        # CLUSTER_NAME=gdelt-elk-cluster

        # Elasticsearch User (Defaults to 'elastic')
        # ELASTIC_USER=elastic

        # Ports (Defaults shown, change if needed)
        # ES_PORT=9200
        # KIBANA_PORT=5601

        # Memory Limits for ELK stack components (adjust based on system resources)
        # ES_MEM_LIMIT=4g
        # KB_MEM_LIMIT=2g
        # LS_MEM_LIMIT=2g
        # ES_JAVA_OPTS="-Xms2g -Xmx2g" # Example for Elasticsearch JVM heap size
        # LS_JAVA_OPTS="-Xms1g -Xmx1g" # Example for Logstash JVM heap size
        ```

2.  **Build and Run Services:**
    * Open a terminal or command prompt in the project root directory (where `docker-compose.yml` is located).
    * Run the following command:
        ```bash
        docker-compose up --build -d
        ```
        * `--build`: Builds the images for custom services (frontend, backend, etl) if they don't exist or if their source code/Dockerfile has changed.
        * `-d`: Runs the containers in detached mode (in the background). Omit this flag if you want to see the combined logs in your terminal.

3.  **Access Components:**
    * **Frontend Application:** Open your web browser to `http://localhost:8501`
    * **Kibana:** Access Kibana at `http://localhost:5601` (or the port specified by `KIBANA_PORT` in `.env`). Log in using the `elastic` user and the `ELASTIC_PASSWORD` you set.
    * **Backend API (Docs):** The API documentation (Swagger UI) is available at `http://localhost:8000/docs`.
    * **Elasticsearch:** Accessible programmatically or via tools like `curl` at `https://localhost:9200` (or `ES_PORT`). Use the `elastic` user and password. Note: Uses HTTPS with self-signed certificates by default; you may need to bypass verification (`-k` in curl).

## Troubleshooting

* **Authentication Error:** Ensure the `ELASTIC_PASSWORD` in your `.env` file is correctly picked up by all services (Elasticsearch, Kibana, Logstash, Backend API) as defined in their `environment` sections in `docker-compose.yml`.
* **Logstash Not Processing Data:**
    * Check Logstash container logs: `docker-compose logs logstash`
    * Verify the Logstash pipeline configuration (`backend/etl_pipeline/pipeline/logstash.conf`) correctly specifies the input path (usually `/usr/share/logstash/ingest_json` inside the container).
    * Check file permissions if issues persist.
* **Frontend Errors:** Check the frontend container logs (`docker-compose logs frontend-app`) and the browser's developer console for errors. Ensure the `BACKEND_API_URL` environment variable is correctly set for the `frontend-app` service in `docker-compose.yml` (usually `http://backend-api:8000`).
* **ETL Errors:** Check the ETL processor logs (`docker-compose logs etl_processor`) for issues during download or processing.