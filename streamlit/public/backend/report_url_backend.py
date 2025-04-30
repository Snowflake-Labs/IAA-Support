import streamlit as st

from public.backend import tables_backend


@st.cache_data(show_spinner=True)
def get_table_report_url(execution_id_list):
    report_url_table_data = tables_backend.get_report_url_table_date_by_execution_id(execution_id_list)
    report_url_table_data_collected = report_url_table_data.collect()
    return report_url_table_data_collected
