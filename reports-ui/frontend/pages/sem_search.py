import streamlit as st
import requests

st.set_page_config(page_title="GDELT Search", layout="wide")
BACKEND_URL = "http://reportsui_backend:8000" 


st.title("🌍 GDELT GKG Search Interface")

# Search input
user_prompt = st.text_input("Enter your GDELT-style query:", placeholder="e.g., Ukraine AND Conflict AND SourceCommonName:BBC")
no_results = st.number_input("How many results do you want to retrieve?", min_value=1, max_value=1000, value=5)

response_json = None

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

    # Get ES response
    response = requests.post(
        f"{BACKEND_URL}/sem_search_backend/sem_search",
        json={
            "user_prompt": user_prompt,
            "top_tags": dict(vect_store_json)
        }
    )
    if response.status_code != 200:
        st.warning(f"Unable to get response, status code: {response.status_code}")
        st.stop()
    response_json = response.json()


st.subheader("🔍 Search Results")
if response_json:
    for idx, article in enumerate(response_json):
        with st.expander(f"{idx+1}. {article['title']} (Score: {article['score']})"):
            st.markdown(f"**🧾 Record ID:** {article['RecordId']}")
            st.markdown(f"**📅 Date:** {article['V21Date']}")
            st.markdown(f"**🔗 Link:** [{article['V2DocId']}]({article['V2DocId']})")
            
            # Truncate long theme lists for brevity
            themes = article['V2Theme']
            theme_preview = ", ".join(themes[:10])
            more_themes = f" and {len(themes) - 10} more..." if len(themes) > 10 else ""
            st.markdown(f"**🏷 Themes:** {theme_preview}{more_themes}")
else:
    st.warning("No results found.")