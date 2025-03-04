#!/bin/bash

SINCE_DB="/usr/share/logstash/data/sincedb.txt"
LOGSTASH_CMD="logstash -f /usr/share/logstash/pipeline/logstash.conf --pipeline.ordered=false"

echo "Watching for new data files in $SINCE_DB..."

while true; do
    inotifywait -e modify,create,move,delete "$SINCE_DB"
    echo "Change detected in $SINCE_DB. Running Logstash..."
    $LOGSTASH_CMD
    echo "Logstash execution finished. Watching for further changes..."
done