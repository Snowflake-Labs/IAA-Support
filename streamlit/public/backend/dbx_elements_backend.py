import pandas

from public.backend import app_snowpark_utils as utils
from public.backend.globals import (
    ALL_KEY,
    COLUMN_AUTOMATED,
    COLUMN_CATEGORY,
    COLUMN_CELL_ID,
    COLUMN_ELEMENT,
    COLUMN_EXECUTION_ID,
    COLUMN_FILE_ID,
    COLUMN_KIND,
    COLUMN_LINE,
    COLUMN_PACKAGE_NAME,
    COLUMN_STATUS,
    COLUMN_SUPPORTED,
    COLUMN_TYPE,
    FRIENDLY_NAME_CELL_ID,
    FRIENDLY_NAME_CODE_FILE_PATH,
    FRIENDLY_NAME_PACKAGE_NAME,
    FRIENDLY_NAME_TYPE,
    TABLE_DBX_ELEMENTS_INVENTORY,
)
from public.backend.query_quality_handler import with_table_quality_handler
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import col


def get_dbx_elements_inventory_table_data():
    session = utils.get_session()
    table_data = with_table_quality_handler(
        session.table(TABLE_DBX_ELEMENTS_INVENTORY),
        TABLE_DBX_ELEMENTS_INVENTORY,
    )
    return table_data


def get_dbx_elements_inventory_table_data_by_execution_id(execution_id_list: list) -> DataFrame:
    data = get_dbx_elements_inventory_table_data().where(col(COLUMN_EXECUTION_ID).isin(execution_id_list))
    return data


def get_dbx_elements(execution_id_list: list) -> pandas.DataFrame:
    dbx_data = get_dbx_elements_inventory_table_data_by_execution_id(execution_id_list)
    dbx_data = dbx_data.select(
        col(COLUMN_ELEMENT),
        col(COLUMN_FILE_ID).alias(FRIENDLY_NAME_CODE_FILE_PATH),
        col(COLUMN_CATEGORY),
        col(COLUMN_KIND).alias(FRIENDLY_NAME_TYPE),
        col(COLUMN_LINE),
        col(COLUMN_PACKAGE_NAME).alias(FRIENDLY_NAME_PACKAGE_NAME),
        col(COLUMN_SUPPORTED),
        col(COLUMN_AUTOMATED),
        col(COLUMN_STATUS),
        col(COLUMN_CELL_ID).alias(FRIENDLY_NAME_CELL_ID),
    )

    dbx_data = dbx_data.order_by(COLUMN_ELEMENT)
    return dbx_data.toPandas()


def get_dbx_elements_filtered(
    df: pandas.DataFrame,
    element: str = None,
    category: str = None,
    kind: str = None,
    supported: str = None,
    automated: str = None,
    status: str = None,
) -> pandas.DataFrame:
    if element:
        df = df[df[COLUMN_ELEMENT].str.contains(element, case=False, na=False)]
    if category and category != ALL_KEY:
        df = df[df[COLUMN_CATEGORY] == category]
    if kind and kind != ALL_KEY:
        df = df[df[COLUMN_TYPE] == kind]
    if supported and supported != ALL_KEY:
        df = df[df[COLUMN_SUPPORTED] == supported]
    if automated and automated != ALL_KEY:
        df = df[df[COLUMN_AUTOMATED] == automated]
    if status and status != ALL_KEY:
        df = df[df[COLUMN_STATUS] == status]

    return df
