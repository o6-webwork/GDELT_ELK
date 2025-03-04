## Overview
This repository automates the process of converting CSV data to JSON format, flattening the JSON using Logstash, and indexing the flattened data into Elasticsearch. The project uses Docker Compose to manage the entire stack, which includes:

- Python Script to convert CSV files into JSON using PySpark
- Logstash to process and flatten the JSON data
- Elasticsearch to index the processed data
- Kibana to visualize and query the indexed data

## Prerequisites
Before you begin, ensure you have the following installed:
Docker (including Docker Compose)
Python 3.x

## Data Processing
1) In the `/data_processing` folder, run `python(3) main.py`, adjusting the `raw_file_path` parameter as needed. The script outs a json and parquet file in the same directory.
2) Copy the json folder into /logstash/logstash_ingest_data. This directory will be mounted to the logstash container later for ingestion

## Running Services
Use the following Docker command to run the entire ELK stack. Adjust the parameters in `docker-compose.yml` as needed.

```docker compose up``` 


### Empty Fields in Kibana:
Delete the sincedb.txt in logstash/logstash_ingest_data 

