from public.backend import tables_backend
from public.backend.globals import *


def get_io_files_inventory_table_data_by_execution_id(execution_id_list):
    io_files_inventory_table_data = tables_backend.get_io_files_inventory_table_data_by_execution_id_with_timestamp_and_email(
        execution_id_list,
    )
    io_files_inventory_table_data_friendly_name = (
        io_files_inventory_table_data.select(
            COLUMN_EXECUTION_ID,
            COLUMN_EXECUTION_TIMESTAMP,
            COLUMN_CLIENT_EMAIL,
            COLUMN_ELEMENT,
            COLUMN_FORMAT_TYPE,
            COLUMN_MODE,
            COLUMN_PROJECT_ID,
            COLUMN_FILE_ID,
            COLUMN_LINE,
            COLUMN_COUNT,
            COLUMN_IS_LITERAL,
        )
        .withColumnRenamed(COLUMN_EXECUTION_ID, FRIENDLY_NAME_EXECUTION_ID)
        .withColumnRenamed(
            COLUMN_EXECUTION_TIMESTAMP, FRIENDLY_NAME_EXECUTION_TIMESTAMP,
        )
        .withColumnRenamed(COLUMN_CLIENT_EMAIL, FRIENDLY_NAME_CLIENT_EMAIL)
        .withColumnRenamed(COLUMN_FORMAT_TYPE, FRIENDLY_NAME_FORMAT_TYPE)
        .withColumnRenamed(COLUMN_PROJECT_ID, FRIENDLY_NAME_PROJECT_ID)
        .withColumnRenamed(COLUMN_FILE_ID, FRIENDLY_NAME_SOURCE_FILE)
        .withColumnRenamed(COLUMN_IS_LITERAL, FRIENDLY_NAME_IS_LITERAL)
    )
    return io_files_inventory_table_data_friendly_name.toPandas()
