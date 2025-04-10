# frontend/Home.py
import streamlit as st

st.set_page_config(
    page_title="GDELT Pipeline Home",
    layout="wide"
)

st.title("Welcome to the GDELT Pipeline Dashboard")

st.sidebar.success("Select a page above.")

st.markdown(
    """
    This application provides tools to monitor and analyze the GDELT ELK pipeline.

    **👈 Select a page from the sidebar** to access different functionalities:

    - **Pipeline Monitoring:** View system status, check logs, and manually trigger data patching or archive downloads.
    - **Trend Analysis:** Analyze and visualize trends in entity mentions over time based on the processed GDELT data.

    ---
    Ensure the backend services are running via `docker-compose up`.
    """
)