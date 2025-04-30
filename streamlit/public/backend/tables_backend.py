from public.backend import app_snowpark_utils as utils
from public.backend.globals import *
from public.backend.query_quality_handler import with_table_quality_handler
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import sum as _sum


def get_execution_info_table_data():
    session = utils.get_session()
    table_data = with_table_quality_handler(
        session.table(TABLE_EXECUTION_INFO), TABLE_EXECUTION_INFO,
    )
    return table_data


def get_execution_info_table_data_by_execution_id(execution_id_list):
    session = utils.get_session()
    table_data = with_table_quality_handler(
        session.table(TABLE_EXECUTION_INFO), TABLE_EXECUTION_INFO,
    ).where(col(COLUMN_EXECUTION_ID).isin(execution_id_list))
    return table_data


def get_input_files_inventory_table_data():
    session = utils.get_session()
    table_data = with_table_quality_handler(
        session.table(TABLE_INPUT_FILES_INVENTORY), TABLE_INPUT_FILES_INVENTORY,
    )
    return table_data


def get_input_files_inventory_table_data_by_execution_id(execution_id_list):
    session = utils.get_session()
    table_data = with_table_quality_handler(
        session.table(TABLE_INPUT_FILES_INVENTORY), TABLE_INPUT_FILES_INVENTORY,
    ).where(col(COLUMN_EXECUTION_ID).isin(execution_id_list))
    return table_data

def get_execution_timestamp_and_email():
    execution_info_table_data = get_execution_info_table_data()
    execution_timestamp_and_email = execution_info_table_data.select(COLUMN_EXECUTION_ID, COLUMN_EXECUTION_TIMESTAMP, COLUMN_CLIENT_EMAIL)
    return execution_timestamp_and_email

def get_execution_timestamp_and_email_by_execution_id(execution_id_list):
    execution_info_table_data = get_execution_info_table_data_by_execution_id(
        execution_id_list,
    )
    execution_timestamp_and_email = execution_info_table_data.select(
        COLUMN_EXECUTION_ID, COLUMN_EXECUTION_TIMESTAMP, COLUMN_CLIENT_EMAIL,
    )
    return execution_timestamp_and_email


def get_input_files_inventory_table_data_with_timestamp_and_email(execution_id_list):
    execution_info_table_data = get_execution_timestamp_and_email_by_execution_id(execution_id_list)
    input_files_inventory_table_data = (
        get_input_files_inventory_table_data_by_execution_id(execution_id_list)
    )
    input_files_inventory_join_execution_info = input_files_inventory_table_data.join(
        right=execution_info_table_data, on=COLUMN_EXECUTION_ID, how="left",
    )
    return input_files_inventory_join_execution_info


def get_lines_of_code_total_code_files_and_project_id(execution_id_list):
    input_files_inventory_table_data = (
        get_input_files_inventory_table_data_by_execution_id(execution_id_list)
    )

    code_input_files_inventory_table_data = input_files_inventory_table_data.where(
        col(COLUMN_IGNORED) == FALSE_KEY,
    ).select(
        COLUMN_EXECUTION_ID,
        COLUMN_PROJECT_ID,
        COLUMN_COUNT,
        COLUMN_LINES_OF_CODE,
        COLUMN_IGNORED,
    )

    lines_of_code_and_total_code_files = code_input_files_inventory_table_data.groupBy(
        COLUMN_EXECUTION_ID, COLUMN_PROJECT_ID,
    ).agg(
        _sum(COLUMN_LINES_OF_CODE).alias(COLUMN_TOTAL_LINES_OF_CODE),
        _sum(COLUMN_COUNT).alias(COLUMN_TOTAL_CODE_FILES),
    )

    return lines_of_code_and_total_code_files


def get_spark_usages_inventory_table_data():
    session = utils.get_session()
    table_data = with_table_quality_handler(
        session.table(TABLE_SPARK_USAGES_INVENTORY), TABLE_SPARK_USAGES_INVENTORY,
    )
    return table_data


def get_spark_usages_inventory_table_data_by_execution_id(execution_id_list):
    session = utils.get_session()
    table_data = with_table_quality_handler(
        session.table(TABLE_SPARK_USAGES_INVENTORY), TABLE_SPARK_USAGES_INVENTORY,
    ).where(col(COLUMN_EXECUTION_ID).isin(execution_id_list))
    return table_data


def get_spark_usages_inventory_table_data_with_timestamp_and_email() -> object:
    execution_info_table_data = get_execution_timestamp_and_email()
    spark_usages_inventory_table_data = get_spark_usages_inventory_table_data()
    spark_usages_inventory_join_execution_info = spark_usages_inventory_table_data.join(
        right=execution_info_table_data, on=COLUMN_EXECUTION_ID, how="left",
    )
    return spark_usages_inventory_join_execution_info

def get_spark_usages_inventory_table_data_by_execution_id_with_timestamp_and_email(
    execution_id_list: object,
) -> object:
    execution_info_table_data = get_execution_timestamp_and_email_by_execution_id(execution_id_list)
    spark_usages_inventory_table_data = (
        get_spark_usages_inventory_table_data_by_execution_id(execution_id_list)
    )
    spark_usages_inventory_join_execution_info = spark_usages_inventory_table_data.join(
        right=execution_info_table_data, on=COLUMN_EXECUTION_ID, how="left",
    )
    return spark_usages_inventory_join_execution_info


def get_import_usages_inventory_table_data_by_execution_id(execution_id_list):
    session = utils.get_session()
    table_data = (
        (
            with_table_quality_handler(
                session.table(TABLE_IMPORT_USAGES_INVENTORY),
                TABLE_IMPORT_USAGES_INVENTORY,
            ).where((col(COLUMN_EXECUTION_ID).isin(execution_id_list)))
        )
        .withColumn(COLUMN_IS_INTERNAL, col(COLUMN_ORIGIN) == USER_DEFINED_KEY)
    )
    return table_data


def get_io_files_inventory_table_data_by_execution_id(execution_id_list):
    session = utils.get_session()
    table_data = with_table_quality_handler(
        session.table(TABLE_IO_FILES_INVENTORY), TABLE_IO_FILES_INVENTORY,
    ).where(col(COLUMN_EXECUTION_ID).isin(execution_id_list))
    return table_data


def get_io_files_inventory_table_data_by_execution_id_with_timestamp_and_email(
    execution_id_list,
):
    execution_info_table_data = get_execution_timestamp_and_email_by_execution_id(execution_id_list)
    io_files_inventory_table_data = get_io_files_inventory_table_data_by_execution_id(
        execution_id_list,
    )
    io_files_inventory_join_execution_info = io_files_inventory_table_data.join(
        right=execution_info_table_data, on=COLUMN_EXECUTION_ID, how="left",
    )
    return io_files_inventory_join_execution_info


def get_third_party_usages_inventory_table_data_by_execution_id(execution_id_list):
    session = utils.get_session()
    table_data = with_table_quality_handler(
        session.table(TABLE_THIRD_PARTY_USAGES_INVENTORY),
        TABLE_THIRD_PARTY_USAGES_INVENTORY,
    ).where(col(COLUMN_EXECUTION_ID).isin(execution_id_list))
    return table_data


def get_third_party_usages_inventory_table_data_by_execution_id_with_timestamp_and_email(
    execution_id_list,
):
    execution_info_table_data = get_execution_timestamp_and_email_by_execution_id(execution_id_list)
    third_party_usages_inventory_table_data = (
        get_third_party_usages_inventory_table_data_by_execution_id(execution_id_list)
    )
    third_party_usages_inventory_join_execution_info = (
        third_party_usages_inventory_table_data.join(
            right=execution_info_table_data, on=COLUMN_EXECUTION_ID, how="left",
        )
    )
    return third_party_usages_inventory_join_execution_info

def get_report_url_table_date_by_execution_id(execution_id_list):
    session = utils.get_session()
    table_data = with_table_quality_handler(
        session.table(TABLE_REPORT_URL), TABLE_REPORT_URL,
    ).where(col(COLUMN_EXECUTION_ID).isin(execution_id_list))
    return table_data
