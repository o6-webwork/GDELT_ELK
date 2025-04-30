from fastapi import APIRouter, Query, Body
from sentence_transformers import SentenceTransformer, util
from sklearn.metrics.pairwise import cosine_similarity
from pydantic import BaseModel
from typing import Dict, Any
import numpy as np
import json, faiss

from routers.data_loader import load_json_from_elasticsearch

router = APIRouter()


class FilterRequest(BaseModel):
    data: Dict[str, Any]
    top_tags: Dict[str, float]

@router.post("/response")
def get_response(es_query: dict = Body(...)):
    return load_json_from_elasticsearch(es_query)

@router.get("/vector_store")
def vect_store(user_query: str, no_results: int):
    # Step 1: Load tags
    with open("./routers/gdelt_lookup.txt", "r") as f:
        tags = [line.strip() for line in f if line.strip()]

    # Step 2: Embed tags
    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = model.encode(tags, convert_to_numpy=True)

    # Step 3: Normalize embeddings (recommended for cosine similarity)
    embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)

    # Step 4: Build FAISS index (cosine similarity → IndexFlatIP)
    dimension = embeddings.shape[1]
    index = faiss.IndexFlatIP(dimension)
    index.add(embeddings)
    
    # Embed query
    query_embedding = model.encode([user_query])
    query_embedding = query_embedding / np.linalg.norm(query_embedding)

    # Search
    D, I = index.search(query_embedding, no_results)

    # # Print top-k tags
    # for i, score in zip(I[0], D[0]):
    #     print(f"{tags[i]} (score: {score:.4f})")
    results = {tags[i]: float(score) for i, score in zip(I[0], D[0])}
    return results

@router.post("/filter")
def filter_results(req: FilterRequest):
    data = req.data
    top_tags = req.top_tags
    hits = data.get("hits", {}).get("hits", [])
    if not hits or not isinstance(top_tags, dict):
        return None

    # Filter articles based on presence of top tags
    filtered_hits = [
        article for article in hits
        if any(tag in top_tags for tag in article["_source"].get("V2EnhancedThemes", {}).get("V2Theme", []))
    ]

    if not filtered_hits:
        return None

    # Build new JSON structure with filtered hits
    filtered_data = {
        "took": data.get("took"),
        "timed_out": data.get("timed_out"),
        "_shards": data.get("_shards"),
        "hits": {
            "total": {
                "value": len(filtered_hits),
                "relation": "eq"
            },
            "max_score": 1.0,
            "hits": filtered_hits
        }
    }

    return filtered_data


@router.get("/sem_search")
def sem_search(data: dict, user_prompt: str):
    # Extract relevant article data into a list of dicts
    articles = []
    for hit in data["hits"]["hits"]:
        source = hit["_source"]
        articles.append({
            "title": source["V2ExtrasXML"]["Title"],
            "RecordId": source["RecordId"],
            "V21Date": source["V21Date"],
            "V2DocId": source["V2DocId"],
            "V2Theme": source.get("V2EnhancedThemes", {}).get("V2Theme", [])
        })

    # Load sentence-transformer model
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Encode titles
    titles = [article["title"] for article in articles]
    title_embeddings = model.encode(titles)

    # Your semantic query
    query_embedding = model.encode([user_prompt])

    # Calculate similarity
    similarities = cosine_similarity(query_embedding, title_embeddings)[0]
    ranked_indices = np.argsort(similarities)[::-1]

    results = []
    for idx in ranked_indices:
        article = articles[idx]
        score = similarities[idx]
        results.append({
            "score": round(float(score), 4),
            "title": article['title'],
            "RecordId": article['RecordId'],
            "V21Date": article['V21Date'],
            "V2DocId": article['V2DocId'],
            "V2Theme": article['V2Theme']
        })
    return results