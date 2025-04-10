# Logstash Ingestion Pipeline Setup

This document outlines how to set up a Logstash ingestion pipeline using Docker to ingest newline-delimited JSON files into Elasticsearch. The key points of this configuration are:

## Key Configuration Points

1. **Same Docker Network:**
   - Logstash runs on the same Docker network as Elasticsearch and Kibana for easy service discovery.

2. **Using Elasticsearch IP:**
   - The configuration uses the Elasticsearch container’s IP address to ensure connectivity.

3. **Volume Mounts:**
   - Two volumes are used:
     - One for the Logstash configuration files.
     - One for the JSON ingestion folder.

---

## Prerequisites

- **Docker:** Installed and running.
- **Docker Network:** Create a Docker network (e.g., `elastic`) so that all containers (Elasticsearch, Kibana, and Logstash) can communicate.
- **Elasticsearch and Kibana:** Running on the same network.

---

## Docker Network Setup

Create a Docker network (if you haven’t already):

```bash
docker network create elastic
```

---

## Running Logstash

Use the following Docker command to run Logstash. Adjust the host paths and parameters as needed:

```bash
docker run --rm -it --name log01 --network elastic \
  -v ~/pipeline/:/usr/share/logstash/pipeline/ \
  -v /home/mushroom/OTB-ElasticSearch/pyspark_gdelt/transformed_gkg.json/:/usr/share/logstash/data/ \
  docker.elastic.co/logstash/logstash:8.17.2
```

### Explanation:

- `--rm -it`: Runs the container interactively and removes it when stopped.
- `--name log01`: Names the container `log01`.
- `--network elastic`: Connects the container to the Docker network named `elastic`.
- `-v ~/pipeline/:/usr/share/logstash/pipeline/`: Mounts the host directory (containing Logstash configuration files) to the container’s pipeline directory.
- `-v /home/otb-02/code/logstash_ingest/:/usr/share/logstash/data/`: Mounts the host directory (containing JSON files) to the container’s data directory.
- `docker.elastic.co/logstash/logstash:8.17.2`: Specifies the Logstash Docker image.

---

## Logstash Configuration File

Save the following configuration file (e.g., `gdelt_ingest.conf`) in your configuration directory (e.g., `~/pipeline/`):

```ruby
input {
  file {
    path => "/usr/share/logstash/data/*.json"  # JSON files in the mounted ingestion folder
    start_position => "beginning"              # Read new files from the beginning
    sincedb_path => "/usr/share/logstash/data/sincedb.txt"  # Persistent file to remember ingestion state
    codec => json                             # Parse each line as a JSON object
  }
}

output {
  elasticsearch {
    hosts => ["https://<ES_IP>:9200"]          # Replace <ES_IP> with your Elasticsearch container's IP (e.g., 172.20.0.2)
    index => "<INDEX_NAME>"                    # Replace <INDEX_NAME> with your desired index name (e.g., gdelt_test)
    user => "<ES_USER>"                        # Replace <ES_USER> with your Elasticsearch username (e.g., elastic)
    password => "<ES_PASSWORD>"                # Replace <ES_PASSWORD> with your Elasticsearch password
    ssl_certificate_verification => false     # Disable certificate verification (for self-signed certs)
  }
}
```

### Generalizing Parameters:

- `<ES_IP>`: The IP address of your Elasticsearch container.
- `<INDEX_NAME>`: The name of the Elasticsearch index (e.g., `gdelt_test`).
- `<ES_USER>` & `<ES_PASSWORD>`: Your Elasticsearch credentials.

---

## How It Works

1. **File Input:**  
   Logstash monitors the `/usr/share/logstash/data/` directory for JSON files.
   - Files are read from the beginning (`start_position => "beginning"`).
   - The `sincedb` file tracks which files have been ingested to prevent duplicate ingestion on restarts.

2. **Event Parsing:**  
   Each line in a JSON file is parsed as a separate JSON object using the JSON codec.

3. **Elasticsearch Output:**  
   Parsed events are sent to Elasticsearch over HTTPS using the provided credentials and indexed under the specified index.

4. **Data Persistence:**  
   The `sincedb` file persists ingestion state, so Logstash remembers which files it has already processed even after a restart.

---

## Troubleshooting

### No Data in Kibana:

- Check that your JSON files are in the correct mounted directory.
- Verify that Logstash is processing events by checking its logs:

  ```bash
  docker logs -f log01
  ```

- Check the document count in Elasticsearch:

  ```bash
  docker run --rm --network elastic curlimages/curl \
    curl -u <ES_USER>:<ES_PASSWORD> -X GET "https://<ES_IP>:9200/<INDEX_NAME>/_count" -k
  ```

### License Checker Warnings:

These warnings from the X-Pack license checker do not affect data ingestion. They can be silenced by configuring or disabling X-Pack monitoring in a custom `logstash.yml`.

---

## Conclusion

This setup allows Logstash to monitor a specified folder for new JSON files, ingest them into Elasticsearch, and remember which files have been processed using a persistent `sincedb` file. Adjust the parameters as needed for your environment, and ensure all services are on the same Docker network for smooth communication.

Feel free to modify or extend this configuration based on your specific use case.

---

_End of README._

