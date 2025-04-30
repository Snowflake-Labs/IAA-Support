from public.backend import tables_backend
from public.backend.globals import *
from snowflake.snowpark.functions import col, countDistinct


def get_import_usages_by_execution_id_and_by_origin(execution_id_list, origin):
    import_usages_table_data = (
        tables_backend.get_import_usages_inventory_table_data_by_execution_id(
            execution_id_list,
        )
    )
    if not origin == ALL_KEY:
        import_usages_table_data = import_usages_table_data.where(
            col(COLUMN_ORIGIN) == origin,
        )

    import_usages_by_origin = import_usages_table_data.groupBy(
        COLUMN_EXECUTION_ID,
        COLUMN_ELEMENT,
        COLUMN_ORIGIN,
        COLUMN_IS_SNOWPARK_ANACONDA_SUPPORTED,
    ).agg(countDistinct(COLUMN_FILE_ID).alias(COLUMN_FILES_COUNT))

    import_usages_by_origin_friendly_names = (
        import_usages_by_origin.select(
            COLUMN_ELEMENT,
            COLUMN_FILES_COUNT,
            COLUMN_ORIGIN,
            COLUMN_IS_SNOWPARK_ANACONDA_SUPPORTED,
        )
        .withColumnRenamed(COLUMN_ELEMENT, FRIENDLY_NAME_IMPORT)
        .withColumnRenamed(COLUMN_FILES_COUNT, FRIENDLY_NAME_STATUS_FILE_COUNT)
        .withColumnRenamed(
            COLUMN_IS_SNOWPARK_ANACONDA_SUPPORTED, FRIENDLY_NAME_SNOWFLAKE_SUPPORTED,
        )
    )
    return import_usages_by_origin_friendly_names.toPandas()
