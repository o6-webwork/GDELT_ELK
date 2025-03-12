#!/bin/bash

echo "Waiting for Elasticsearch to be ready..."
until curl -k -u elastic:changeme -s "https://127.0.0.1:9200/_cluster/health?wait_for_status=yellow"; do
    sleep 5
done

echo "Creating Ingest Pipeline..."
curl -X PUT -k -u elastic:changeme "https://127.0.0.1:9200/_ingest/pipeline/remove_text_duplicates" -H "Content-Type: application/json" -d '{
  "description": "Remove duplicates from text fields",
  "processors": [
    {
      "script": {
        "lang": "painless",
        "source": "for (entry in ctx.keySet()) { if (ctx[entry] instanceof String) { def words = new HashSet(Arrays.asList(ctx[entry].split(\",\"))); ctx[entry] = String.join(\",\", words); } }"
      }
    }
  ]
}'
