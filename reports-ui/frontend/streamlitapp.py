import streamlit as st
import requests

st.set_page_config(layout='wide')

reports_ui = st.Page("pages/reports_ui.py", title="Reports UI")
sem_search = st.Page("pages/sem_search.py", title="Semantic Search")
page_dict = {
    "Reports UI": reports_ui,
    "Processing": sem_search,
}

selected_page = st.navigation(page_dict)
selected_page.run() 