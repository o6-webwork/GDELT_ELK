import streamlit as st

st.set_page_config(page_title="GDELT Search", layout="wide")
BACKEND_URL = "http://reportsui_backend:8000" 


st.title("🌍 GDELT GKG Search Interface")

# Search input
query = st.text_input("Enter your GDELT-style query:", placeholder="e.g., Ukraine AND Conflict AND SourceCommonName:BBC")

# Search button
if st.button("Search"):
    if query.strip() == "":
        st.warning("Please enter a query to search.")
    else:
        st.success(f"Query submitted: `{query}`")
        
        # You could call a backend function like:
        # results = search_gdelt(query, filter_by)
        # st.write(results)