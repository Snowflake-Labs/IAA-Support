import streamlit as st

import public.backend.app_snowpark_utils as utils

from public.backend import executions_backend
from public.backend.globals import *


def showData(df):
    if df is not None and df.shape[0] > 0:
        df = utils.reset_index(df)
        df = df.drop_duplicates()
        edited_df = st.data_editor(
            df,
            column_config={
                FRIENDLY_NAME_TOOL_NAME: None,
            },
            disabled=[
                FRIENDLY_NAME_EXECUTION_ID,
                FRIENDLY_NAME_EXECUTION_TIMESTAMP,
                FRIENDLY_NAME_READINESS_SCORE,
                FRIENDLY_NAME_SAS_READINESS_SCORE,
                FRIENDLY_NAME_CLIENT_EMAIL,
                FRIENDLY_NAME_TOTAL_CODE_FILES,
                FRIENDLY_NAME_LINES_OF_CODE,
                FRIENDLY_NAME_PROJECT_ID,
                FRIENDLY_NAME_PROJECT_NAME,
                COLUMN_COMPANY,
            ],
        )
        found_executions = list(
            df.loc[edited_df[edited_df[FRIENDLY_NAME_SELECT] == True].index][FRIENDLY_NAME_EXECUTION_ID],
        )
        return found_executions
    st.info("No executions found.")
    return []


def show(use_expander=True):
    with st.container():
        search_value = st.text_input(
            "Search",
            placeholder="Enter an Execution ID, Email, Company Name or Project Name to search for executions, Leave it empty and Press Enter to get executions from the last 15 days",
            key="execution",
        )
        input_value = search_value.strip().lower()
        if use_expander:
            with st.expander("Executions available", expanded=True):
                found_executions = get_found_executions(input_value)
        else:
            found_executions = get_found_executions(input_value)
        found_executions_count = len(found_executions)
        if found_executions_count > 0:
            st.info(f"Currently viewing: {found_executions_count} execution(s).")
        return found_executions


def get_found_executions(input_value: int):
    found_executions = []
    if len(input_value) == 0:
        df = executions_backend.get_last_two_weeks_executions()
        found_executions = showData(df)
    elif utils.looks_like_email(input_value):
        df = executions_backend.get_all_executions_by_email(input_value.lower())
        found_executions = showData(df)
    elif utils.looks_like_guid(input_value):
        df = executions_backend.get_execution_by_id(input_value.lower())
        found_executions = showData(df)
    elif len(input_value) > 0:
        df = executions_backend.get_execution_by_company_or_project_name_or_email(
            input_value.lower(),
        )
        found_executions = showData(df)
    return found_executions
