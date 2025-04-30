import streamlit as st

from pandas import DataFrame

import public.backend.app_snowpark_utils as utils


def with_table_quality_handler(table: DataFrame, table_name: str):  # pragma:no cover
    column_metadata: list = utils.get_table_metadata(table_name)
    column_names = [col_name for (col_name, col_type) in column_metadata]
    try:
        df = table.select(column_names)
        for actual_type, (col_name, expected_type) in zip(df.dtypes, column_metadata, strict=False):
            if actual_type[1] != expected_type:
                msg = (f"Error handler: Missmatch, column {col_name} \n" +
                       f"{actual_type[1]} == {expected_type} on table {table_name}")
                st.exception(Exception(msg))
        return df
    except Exception as e:
        st.exception(Exception(e))
