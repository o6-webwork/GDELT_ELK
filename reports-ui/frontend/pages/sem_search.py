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

    # Get N relevant data
    vect_store = requests.get(
        f"{BACKEND_URL}/sem_search_backend/vector_store",
        params={"user_query": user_prompt,
                "no_results": no_results
                }
    )
    if vect_store.status_code != 200:
        st.warning(f"Unable to get query, status code: {vect_store.status_code}")
        st.stop()   
    vect_store_json = vect_store.json()

    # {
    # "Health": 0.8234,
    # "Medicine": 0.8176,
    # "Pharmaceutical": 0.8002
    # }

    es_query = {
        "_source": [
            "RecordId",
            "V21Date",
            "V2ExtrasXML.Title",
            "V2DocId",
            "V2EnhancedThemes.V2Theme"
        ],
        "query": {
            "match_all": {}  # You can replace this with your desired query
        }
    }

    # Get ES response
    response = requests.post(
        f"{BACKEND_URL}/sem_search_backend/response",
        json=es_query,
        # params={"es_query": es_query}
    )
    if response.status_code != 200:
        st.warning(f"Unable to get response, status code: {response.status_code}")
        st.stop()
    response_json = response.json()

    # Filter ES response
    filtered_response = requests.post(
        f"{BACKEND_URL}/sem_search_backend/filter",
        json={
            "data": response_json,
            "top_tags": vect_store_json
        }
    )
    if filtered_response.status_code != 200:
        st.warning(f"Unable to get response, status code: {filtered_response.status_code}")
        st.stop()
    elif filtered_response == None:
        st.warning("Query has returned zero results")
        st.stop()
    filtered_json = filtered_response.json()

    # Perform one more semantic search
    result = requests.get(
        f"{BACKEND_URL}/sem_search_backend/sem_search",
        params={"data": filtered_json,
                "user_prompt": user_prompt
                }
    )
    if result.status_code != 200:
        st.warning(f"Unable to get response, status code: {result.status_code}")
        st.stop()
    result_json = result.json()

    st.write(result_json)
