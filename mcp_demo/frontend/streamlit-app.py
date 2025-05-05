import streamlit as st
import requests

# MCP server base URL
MCP_BASE_URL = "http://mcp:8000"  # change if hosted elsewhere

st.title("🔍 GKG Search via MCP Tool")

# Input field
keyword = st.text_input("Enter a keyword to search the Elasticsearch index")

# Send request to MCP when button is clicked
if st.button("Search GKG"):
    if not keyword:
        st.warning("Please enter a keyword.")
    else:
        # Prepare payload
        payload = {"keyword": keyword}

        try:
            # Make POST request to MCP tool
            response = requests.post(f"{MCP_BASE_URL}/tools/query_es", json=payload)

            if response.status_code == 200:
                result = response.json()
                st.success("✅ Query Successful!")
                st.json(result)
            else:
                st.error(f"❌ Error {response.status_code}: {response.text}")

        except Exception as e:
            st.error(f"⚠️ Exception occurred: {e}")
