from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sentence_transformers import SentenceTransformer, util
import numpy as np

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
def vect_store(user_query: str):
    # Load a pre-trained model
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Read tags from file
    with open("gdelt_lookup.txt", "r") as f:
        tags = [line.strip() for line in f.readlines()]

    # Encode all tags into embeddings
    tag_embeddings = model.encode(tags, convert_to_tensor=True)

    # Encode the query
    query_embedding = model.encode(user_query, convert_to_tensor=True)

    # Compute cosine similarities
    cosine_scores = util.cos_sim(query_embedding, tag_embeddings)[0]

    # Get the top N most similar tags
    top_n = 5
    top_results = np.argpartition(-cosine_scores, range(top_n))[:top_n]

    # Store results in a dictiona# Load a pre-trained model
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Read tags from file
    with open("gdelt_lookup.txt", "r") as f:
        tags = [line.strip() for line in f.readlines()]

    # Encode all tags into embeddings
    tag_embeddings = model.encode(tags, convert_to_tensor=True)

    # Encode the query
    query_embedding = model.encode(user_query, convert_to_tensor=True)

    # Compute cosine similarities
    cosine_scores = util.cos_sim(query_embedding, tag_embeddings)[0]

    # Get the top N most similar tags
    top_n = 5
    top_results = np.argpartition(-cosine_scores, range(top_n))[:top_n]

    # Store results in a dictionary
    top_tags_with_scores = {
        tags[idx]: float(cosine_scores[idx]) for idx in top_results
    }

    # Optional: sort the dictionary by score (descending)
    top_tags_with_scores = dict(sorted(top_tags_with_scores.items(), key=lambda item: item[1], reverse=True))

    # Display results
    print(f"\nTop {top_n} tags for query: '{user_query}'\n")
    for idx in top_results:
        print(f"{tags[idx]} - Score: {cosine_scores[idx]:.4f}")