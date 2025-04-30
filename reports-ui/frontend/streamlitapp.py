import streamlit as st

# st.set_page_config(layout='wide')

# Define the pages with sections
pages = {
    "Main Features": [
        st.Page("pages/reports_ui.py", title="Reports UI"),
        st.Page("pages/sem_search.py", title="Semantic Search")
    ]
}

# Create the navigation
selected_page = st.navigation(pages)

# Run the selected page
selected_page.run()
