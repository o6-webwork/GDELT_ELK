from elasticsearch import Elasticsearch
from datetime import date, timedelta, datetime
import pandas as pd

# --- CONFIG ---
ES_HOST = "https://es01:4848"
ES_USERNAME = "elastic"
ES_PASSWORD = "changeme"
ES_INDEX = "gkg"
# ENTITY_FIELD = "V21AllNames.Name.keyword"
DATE_FIELD = "V2ExtrasXML.PubTimestamp"
MAX_ENTITIES = 1000

# Creating Elasticsearch client :D
es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USERNAME, ES_PASSWORD),
    verify_certs=True,
    ca_certs="/app/certs/ca/ca.crt"
)
def get_fields_from_elasticsearch(index_pattern: str) -> list:
    array_fields = set()

    try:
        mappings = es.indices.get_mapping(index=index_pattern)

        def extract_array_fields(properties, prefix=""):
            for field, field_mapping in properties.items():
                # Check for .keyword sub-fields
                if 'fields' in field_mapping and 'keyword' in field_mapping['fields']:
                    array_fields.add(prefix + field + '.keyword')
                
                # If it's a nested object, recurse
                if 'properties' in field_mapping:
                    extract_array_fields(field_mapping['properties'], prefix + field + '.')

        # Iterate through every index found (e.g., gkg-1, gkg-2)
        for actual_index_name in mappings:
            properties = mappings[actual_index_name]['mappings'].get('properties', {})
            extract_array_fields(properties)

        return list(array_fields)

    except Exception as e:
        print(f"Error fetching fields: {e}")
        return []

def load_data_from_elasticsearch(start_date: str, end_date: str, entity_field: str) -> pd.DataFrame:
    """Query Elasticsearch and return a DataFrame with date, entity, and count."""
    end_date_es = (date.fromisoformat(end_date) + timedelta(days=1)).isoformat()

    query = {
        "size": 0,
        "query": {
            "range": {
                DATE_FIELD: {
                    "gte": start_date,
                    "lt": end_date_es
                }
            }
        },
        "aggs": {
            "entities_over_time": {
                "date_histogram": {
                    "field": DATE_FIELD,
                    "calendar_interval": "1d",
                    "min_doc_count": 0
                },
                "aggs": {
                    "top_entities": {
                        "terms": {
                            "field": entity_field,
                            "size": MAX_ENTITIES
                        }
                    }
                }
            }
        }
    }

    response = es.search(index=ES_INDEX, body=query)
    buckets = response['aggregations']['entities_over_time']['buckets']

    data = []
    for bucket in buckets:
        date_str = datetime.fromtimestamp(bucket['key'] / 1000).strftime('%Y-%m-%d')
        for entity in bucket.get('top_entities', {}).get('buckets', []):
            data.append({
                "date": date_str,
                "top_entity": entity["key"],
                "count": entity["doc_count"]
            })

    df = pd.DataFrame(data)
    return df

#############################################################################################################################################
def load_json_from_elasticsearch(query: str, ) -> dict:
    return es.search(index=ES_INDEX, body=query)