import io

import pandas as pd
import streamlit as st

from pandas import DataFrame

import public.backend.app_snowpark_utils as utils
import public.backend.app_style_values as style

from public.backend.globals import *
from snowflake.snowpark.functions import col, lit


def getReadinessBackAndForeColorsStyle(readinessValue: float, is_sas_score: bool = False) -> str:
    top = 90 if is_sas_score else 80
    mid = 70 if is_sas_score else 60
    if mid <= readinessValue < top:
        backColor = style.WARNING_COLOR
        foreColor = style.BLACK_COLOR
    elif readinessValue >= top:
        backColor = style.SUCCESS_COLOR
        foreColor = style.WHITE_COLOR
    else:
        backColor = style.ERROR_COLOR
        foreColor = style.WHITE_COLOR

    return f"background-color: {backColor}; color: {foreColor}"


def generate_output_file_table(download_urls: list):
    urlTable = """
| Download Additional Inventories | 
| --- |
"""
    if len(download_urls) == 0:
        urlTable += "|No data|"
        return urlTable

    for url in download_urls:
        urlTable += f"|{url}|\n"
    return urlTable


@st.cache_data(show_spinner=False)
def getOutputFileTable(execution_id: str, table: str) -> pd.DataFrame:
    session = utils.get_session()
    sf_table = session.table(table)
    filtered_table = sf_table.where(col(COLUMN_EXECUTION_ID) == lit(execution_id))
    result_df = filtered_table.to_pandas()
    return result_df


@st.cache_data(show_spinner=False)
def getXlsxOutputBytes(df: DataFrame):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter", engine_kwargs={"options": {"in_memory": True}}) as writer:
        df.to_excel(writer, sheet_name="additional", index=False)
    return output


def get_sma_output_download_urls(execution_id: str):
    urls = []
    utils.get_temp_stage.clear()
    for file_name, table_name in zip(SMA_OUTPUT_FILES_NAMES, SMA_OUTPUT_FILES_TABLES, strict=False):
        table_df = getOutputFileTable(execution_id, table_name)
        output_bytes = getXlsxOutputBytes(table_df)
        url = utils.get_downloadlink_nomd(
            file_name,
            f"{file_name}-{utils.getFileNamePrefix([execution_id])}.xlsx",
            output_bytes,
        )
        urls.append(url)
    return urls


def getReportType(selectedReportTypeName):
    return f"REPORT_{selectedReportTypeName.upper()}_URL"


def color(row):
    tool_supported_color = style.getBackgroundColorProperty(style.PINK_COLOR)
    snowflake_supported_color = style.getBackgroundColorProperty(style.PINK_COLOR)
    mapping_status_color = ""
    mapping_status = row[COLUMN_STATUS.replace("_", " ")].upper()

    if mapping_status in [KEY_DIRECT, KEY_RENAME]:
        mapping_status_color = style.getBackgroundColorProperty(style.HONEY_DEW_COLOR)
    elif mapping_status in [KEY_HELPER, KEY_DIRECT_HELPER, KEY_RENAME_HELPER, KEY_TRANSFORMATION, KEY_WORKAROUND]:
        mapping_status_color = style.getBackgroundColorProperty(style.LIGTH_YELLOW_COLOR)
    elif mapping_status in [KEY_NOTSUPPORTED]:
        mapping_status_color = style.getBackgroundColorProperty(style.LIGTH_PINK_COLOR)
    elif mapping_status in [KEY_NOTDEFFINED]:
        mapping_status_color = style.getBackgroundColorProperty(style.ORANGE_COLOR)

    if row[COLUMN_SUPPORTED.replace("_", " ")] == "TRUE":
        tool_supported_color = style.getBackgroundColorProperty(style.HONEY_DEW_COLOR)
    return [snowflake_supported_color, mapping_status_color]
