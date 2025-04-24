from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sentence_transformers import SentenceTransformer, util
import numpy as np
import json, faiss

from data_loader import load_data_from_elasticsearch

ES_INDEX = "gkg"

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/response")
def get_response(es_query: str):
    return load_data_from_elasticsearch(es_query)

@app.get("/vector_store")
def vect_store(user_query: str, no_results: int):
    # Step 1: Load tags
    with open("gdelt_lookup.txt", "r") as f:
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

    # Print top-k tags
    for i, score in zip(I[0], D[0]):
        print(f"{tags[i]} (score: {score:.4f})")