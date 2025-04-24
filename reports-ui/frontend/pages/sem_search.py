import streamlit as st
import requests

st.set_page_config(page_title="GDELT Search", layout="wide")
BACKEND_URL = "http://reportsui_backend:8000" 


st.title("🌍 GDELT GKG Search Interface")

# Search input
user_prompt = st.text_input("Enter your GDELT-style query:", placeholder="e.g., Ukraine AND Conflict AND SourceCommonName:BBC")
no_results = st.number_input("How many results do you want to retrieve?", min_value=1, max_value=1000, value=5)

# Search button
if st.button("Search"):
    if user_prompt.strip() == "":
        st.warning("Please enter a query to search.")
        st.stop()

    st.success(f"Query submitted: `{user_prompt}`") 
    es_query = requests.get(
        f"{BACKEND_URL}/query_gen",
        params={
            "user_prompt": user_prompt,
        }
    )
    if es_query.status_code != 200:
        st.warning(f"Unable to get query, status code: {es_query.status_code}")
        st.stop()   
    es_query_json = es_query.json()

    response = requests.get(
        f"{BACKEND_URL}/response",
        params={"es_query": es_query_json}
    )
    if response.status_code != 200:
        st.warning(f"Unable to get response, status code: {response.status_code}")
        st.stop()
    response_json = response.json()
    
    response = requests.get(
        f"{BACKEND_URL}/vector_store",
        params={"es_query": es_query_json}
    )

        
