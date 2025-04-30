import pandas as pd
import plotly.express as px
import streamlit as st

import public.backend.app_snowpark_utils as utils
import public.frontend.error_handling as errorHandling

from public.backend import tables_backend
from public.backend.globals import *
from snowflake.snowpark.functions import col


@st.cache_data(show_spinner=True)
def get_treemap_data(execution_id_list, technology=utils.technologies):
    input_files_table_data = tables_backend.get_input_files_inventory_table_data()

    input_files_selected_columns = (
        input_files_table_data.where(
            (col(COLUMN_EXECUTION_ID).isin(execution_id_list))
            & (col(COLUMN_IGNORED) == FALSE_KEY)
            & (col(COLUMN_LINES_OF_CODE) > 0),
        )
        .select(COLUMN_ELEMENT, COLUMN_LINES_OF_CODE)
        .withColumnRenamed(COLUMN_ELEMENT, COLUMN_SOURCE_FILE)
    )

    input_files_selected_columns_collected = input_files_selected_columns.collect()
    input_files_dataframe = (
        pd.DataFrame(input_files_selected_columns_collected)
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return input_files_dataframe


def add_space(column):
    def append_space(row):
        if row is not None and isinstance(row, str):
            return row + " "
        return row

    return column.apply(append_space)


@errorHandling.executeFunctionWithErrorHandling
def buildTreemap(executionIds, technology=utils.technologies):
    files = get_treemap_data(executionIds, technology)
    files = pd.concat(
        [files[COLUMN_SOURCE_FILE].str.split("/", expand=True), files], axis=1,
    ).drop(columns=[COLUMN_SOURCE_FILE])

    files = files.fillna(SNOW_CONVERT_TEMPORATY_FOLDER_NAME)
    cols = list(files.columns)
    cols.pop()
    files = (
        files.groupby(cols)
        .agg({COLUMN_LINES_OF_CODE: "sum"})
        .reset_index()
        .replace(SNOW_CONVERT_TEMPORATY_FOLDER_NAME, None)
    )
    files = files.apply(add_space)
    fig = px.treemap(files, path=cols, values=COLUMN_LINES_OF_CODE)

    return fig
