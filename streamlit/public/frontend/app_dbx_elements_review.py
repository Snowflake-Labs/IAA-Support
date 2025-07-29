import streamlit as st

import public.frontend.empty_screen as emptyScreen
import public.frontend.error_handling as errorHandling

from public.backend import dbx_elements_backend
from public.backend.globals import *


SEARCH_BY_ELEMENT_KEY = "search_by_element"
SEARCH_BY_CATEGORY_KEY = "search_by_category"
SEARCH_BY_KIND_KEY = "search_by_kind"
SEARCH_BY_SUPPORTED_KEY = "search_by_supported"
SEARCH_BY_AUTOMATED_KEY = "search_by_automated"
SEARCH_BY_STATUS_KEY = "search_by_status"


def validate_search_key(key, value):
    if key not in st.session_state:
        st.session_state[key] = value


def initialize_search_keys():
    validate_search_key(SEARCH_BY_ELEMENT_KEY, "")
    validate_search_key(SEARCH_BY_CATEGORY_KEY, ALL_KEY)
    validate_search_key(SEARCH_BY_KIND_KEY, ALL_KEY)
    validate_search_key(SEARCH_BY_SUPPORTED_KEY, ALL_KEY)
    validate_search_key(SEARCH_BY_AUTOMATED_KEY, ALL_KEY)
    validate_search_key(SEARCH_BY_STATUS_KEY, ALL_KEY)


def print_dbx_table(df) -> None:
    if df is None or df.shape[0] <= 0:
        st.info("No dbx elements detected.")
        return
    if df.shape[0] > 1000:
        st.warning(f"{df.shape[0]} rows found. Showing first 1000.")
        files_df_to_show = df.head(1000)
    else:
        files_df_to_show = df

    st.dataframe(files_df_to_show, use_container_width=True, hide_index=True)


@errorHandling.executeFunctionWithErrorHandling
def dbx_elements_review(execution_list) -> None:
    if execution_list is None or len(execution_list) <= 0:
        emptyScreen.show()
    else:
        st.markdown("""This table shows the list of dbx elements collected.""")
        initialize_search_keys()

        df = dbx_elements_backend.get_dbx_elements(execution_list)
        categories = [ALL_KEY] + df[COLUMN_CATEGORY].unique().tolist()
        kinds = [ALL_KEY] + df[FRIENDLY_NAME_TYPE].unique().tolist()
        status_list = [ALL_KEY] + df[COLUMN_STATUS].unique().tolist()

        with st.container():
            col1, col2, col3, col4, col5, col6 = st.columns(6)

            with col1:
                element = st.text_input("Search by Element", key=SEARCH_BY_ELEMENT_KEY).strip()

            with col2:
                category = st.selectbox("Search by Category", key=SEARCH_BY_CATEGORY_KEY, options=categories)

            with col3:
                kind = st.selectbox("Search by element type", key=SEARCH_BY_KIND_KEY, options=kinds)

            with col4:
                supported = st.selectbox(
                    "Search by supported",
                    key=SEARCH_BY_SUPPORTED_KEY,
                    options=[ALL_KEY, "True", "False"],
                )

            with col5:
                automated = st.selectbox(
                    "Search by automated",
                    key=SEARCH_BY_AUTOMATED_KEY,
                    options=[ALL_KEY, "True", "False"],
                )

            with col6:
                status = st.selectbox("Search by status", key=SEARCH_BY_STATUS_KEY, options=status_list)

        df = dbx_elements_backend.get_dbx_elements_filtered(df, element, category, kind, supported, automated, status)
        print_dbx_table(df)
