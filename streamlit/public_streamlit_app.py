import logging

import streamlit as st

import public.frontend.app_landing as public_landing
import public.frontend.app_mappings_review as mappings
import public.frontend.app_snowpark_review_executions as review


st.set_page_config(layout="wide")

logging.info("Starting portal")

st.session_state["disable_private_version"] = True
if  "Page" in st.session_state:
    if st.session_state["Page"] == "Review":
        review.run_public_review()
    elif  st.session_state["Page"] == "Mappings":
        mappings.open_reviews()
    else:
        public_landing.run_portal()
else:
    public_landing.run_portal()
