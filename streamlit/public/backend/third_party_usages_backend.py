from public.backend import tables_backend
from public.backend.globals import *


def get_third_party_usages_inventory_table_data_by_execution_id(execution_id_list):
    third_party_usages_inventory_table_data = tables_backend.get_third_party_usages_inventory_table_data_by_execution_id_with_timestamp_and_email(
        execution_id_list,
    )
    third_party_usages_inventory_table_data_friendly_name = (
        third_party_usages_inventory_table_data.select(
            COLUMN_EXECUTION_ID,
            COLUMN_EXECUTION_TIMESTAMP,
            COLUMN_CLIENT_EMAIL,
            COLUMN_ELEMENT,
            COLUMN_PROJECT_ID,
            COLUMN_FILE_ID,
            COLUMN_COUNT,
            COLUMN_ALIAS,
            COLUMN_KIND,
            COLUMN_LINE,
            COLUMN_PACKAGE_NAME,
        )
        .withColumnRenamed(COLUMN_EXECUTION_ID, FRIENDLY_NAME_EXECUTION_ID)
        .withColumnRenamed(
            COLUMN_EXECUTION_TIMESTAMP, FRIENDLY_NAME_EXECUTION_TIMESTAMP,
        )
        .withColumnRenamed(COLUMN_CLIENT_EMAIL, FRIENDLY_NAME_CLIENT_EMAIL)
        .withColumnRenamed(COLUMN_PROJECT_ID, FRIENDLY_NAME_PROJECT_ID)
        .withColumnRenamed(COLUMN_FILE_ID, FRIENDLY_NAME_SOURCE_FILE)
    )
    return third_party_usages_inventory_table_data_friendly_name.toPandas()
