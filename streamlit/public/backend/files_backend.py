from public.backend import tables_backend
from public.backend.globals import *
from snowflake.snowpark.functions import (
    col,
    count,
    lit,
    sql_expr,
    substring_index,
    upper,
    when,
)
from snowflake.snowpark.functions import (
    round as _round,
)
from snowflake.snowpark.functions import (
    sum as _sum,
)


def get_input_files_by_execution_id(execution_id_list):
    input_files_by_execution_id_and_dbc = get_input_files_by_execution_id_and_grouped_by_dbc_files(execution_id_list)
    input_files = input_files_by_execution_id_and_dbc.select(
        COLUMN_EXECUTION_ID,
        COLUMN_EXECUTION_TIMESTAMP,
        COLUMN_CLIENT_EMAIL,
        COLUMN_ELEMENT,
        COLUMN_TECHNOLOGY,
        col(COLUMN_LINES_OF_CODE).alias(FRIENDLY_NAME_TOTAL_CODE_LOC),
        COLUMN_BYTES,
    )
    return input_files.toPandas()


def get_input_files_by_execution_id_grouped_by_technology(execution_id_list):
    input_files_inventory = get_input_files_by_execution_id_and_grouped_by_dbc_files(
        execution_id_list,
    ).where((col(COLUMN_IGNORED) != TRUE_KEY) | (col(COLUMN_IGNORED).isNull()))

    input_files_inventory_with_new_technology = input_files_inventory.withColumn(
        COLUMN_TECHNOLOGY,
        upper(
            when(col(COLUMN_ELEMENT).like("%.git%"), lit(KEY_UNKNOWN))
            .when(
                ~substring_index(col(COLUMN_ELEMENT), "/", -1).like("%.%"),
                lit(KEY_UNKNOWN),
            )
            .otherwise(sql_expr("SPLIT_PART(ELEMENT, '.', -1)")),
        ),
    )

    input_files_inventory_grouped_by_technology = input_files_inventory_with_new_technology.group_by(
        COLUMN_TECHNOLOGY,
    ).agg(
        count(COLUMN_ELEMENT).alias(COLUMN_FILES),
        _sum(COLUMN_LINES_OF_CODE).alias(FRIENDLY_NAME_LINES_OF_CODE),
    )

    return input_files_inventory_grouped_by_technology.toPandas()


def get_input_files_by_execution_id_and_counted_by_technology(execution_id_list):
    input_files_inventory = tables_backend.get_input_files_inventory_table_data_by_execution_id(
        execution_id_list,
    )
    input_files_with_new_column_technology = input_files_inventory.withColumn(
        COLUMN_TECHNOLOGY,
        when(col(COLUMN_TECHNOLOGY).isNotNull(), col(COLUMN_TECHNOLOGY)).otherwise(
            lit(KEY_OTHER),
        ),
    )
    input_files_by_execution_id_counted_by_technology = input_files_with_new_column_technology.groupBy(
        COLUMN_TECHNOLOGY,
    ).count()
    input_files_by_execution_id_counted_by_technology_sorted = input_files_by_execution_id_counted_by_technology.sort(
        col(COLUMN_COUNT).desc(),
    )
    return input_files_by_execution_id_counted_by_technology_sorted.toPandas()


def get_count_with_sas_usages_by_execution_id(execution_id_list):
    sas_usages_inventory = tables_backend.get_spark_usages_inventory_table_data_by_execution_id(
        execution_id_list,
    )
    input_files_inventory = tables_backend.get_input_files_inventory_table_data_with_timestamp_and_email(
        execution_id_list,
    )
    input_files_with_new_column_technology = input_files_inventory.select(
        COLUMN_EXECUTION_ID,
        COLUMN_EXECUTION_TIMESTAMP,
        COLUMN_CLIENT_EMAIL,
        COLUMN_FILE_ID,
        COLUMN_LINES_OF_CODE,
        COLUMN_TECHNOLOGY,
    ).withColumn(
        COLUMN_TECHNOLOGY,
        when(col(COLUMN_TECHNOLOGY).isNotNull(), col(COLUMN_TECHNOLOGY)).otherwise(
            lit(KEY_OTHER),
        ),
    )

    sas_usages_by_file = (
        sas_usages_inventory.groupBy(COLUMN_FILE_ID)
        .agg(
            _sum(
                when(col(COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED) == TRUE_KEY, col(COLUMN_COUNT)).otherwise(0),
            ).alias(COLUMN_SUPPORTED),
            _sum(
                when(col(COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED) == FALSE_KEY, col(COLUMN_COUNT)).otherwise(0),
            ).alias(COLUMN_NOT_SUPPORTED),
        )
        .withColumn(
            COLUMN_READINESS,
            _round(
                when(
                    (col(COLUMN_SUPPORTED) + col(COLUMN_NOT_SUPPORTED)) > 0,
                    col(COLUMN_SUPPORTED) * 100 / (col(COLUMN_SUPPORTED) + col(COLUMN_NOT_SUPPORTED)),
                ).otherwise(0),
            ),
        )
    )

    sas_usages_by_file_join_input_files = sas_usages_by_file.join(
        right=input_files_with_new_column_technology,
        on=COLUMN_FILE_ID,
        how="left",
    ).sort(col(COLUMN_FILE_ID))

    sas_usages_by_file_with_loc_and_friendly_name = (
        sas_usages_by_file_join_input_files.select(
            COLUMN_EXECUTION_ID,
            COLUMN_EXECUTION_TIMESTAMP,
            COLUMN_CLIENT_EMAIL,
            COLUMN_FILE_ID,
            COLUMN_SUPPORTED,
            COLUMN_NOT_SUPPORTED,
            COLUMN_READINESS,
            COLUMN_LINES_OF_CODE,
            COLUMN_TECHNOLOGY,
        )
        .withColumnRenamed(COLUMN_LINES_OF_CODE, FRIENDLY_NAME_LINES_OF_CODE)
        .withColumnRenamed(COLUMN_FILE_ID, FRIENDLY_NAME_SOURCE_FILE)
        .withColumnRenamed(COLUMN_SUPPORTED, FRIENDLY_NAME_SAS_SUPPORTED)
        .withColumnRenamed(COLUMN_NOT_SUPPORTED, FRIENDLY_NAME_SAS_NOT_SUPPORTED)
        .withColumnRenamed(COLUMN_READINESS, FRIENDLY_NAME_SAS_READINESS)
    )

    return sas_usages_by_file_with_loc_and_friendly_name.toPandas()


def get_files_with_spark_usages_by_execution_id(execution_id_list):
    spark_usages_inventory = tables_backend.get_spark_usages_inventory_table_data_by_execution_id(
        execution_id_list,
    )
    input_files_inventory = tables_backend.get_input_files_inventory_table_data_with_timestamp_and_email(
        execution_id_list,
    )
    input_files_with_new_column_technology = input_files_inventory.select(
        COLUMN_EXECUTION_ID,
        COLUMN_EXECUTION_TIMESTAMP,
        COLUMN_CLIENT_EMAIL,
        COLUMN_FILE_ID,
        COLUMN_LINES_OF_CODE,
        COLUMN_TECHNOLOGY,
    ).withColumn(
        COLUMN_TECHNOLOGY,
        when(col(COLUMN_TECHNOLOGY).isNotNull(), col(COLUMN_TECHNOLOGY)).otherwise(
            lit(KEY_OTHER),
        ),
    )

    spark_usages_by_file = (
        spark_usages_inventory.groupBy(COLUMN_FILE_ID)
        .agg(
            _sum(
                when(col(COLUMN_SUPPORTED) == TRUE_KEY, col(COLUMN_COUNT)).otherwise(0),
            ).alias(COLUMN_SUPPORTED),
            _sum(
                when(col(COLUMN_SUPPORTED) == FALSE_KEY, col(COLUMN_COUNT)).otherwise(0),
            ).alias(COLUMN_NOT_SUPPORTED),
        )
        .withColumn(
            COLUMN_READINESS,
            _round(
                col(COLUMN_SUPPORTED) * 100 / (col(COLUMN_SUPPORTED) + col(COLUMN_NOT_SUPPORTED)),
            ),
        )
    )

    spark_usages_by_file_join_input_files = spark_usages_by_file.join(
        right=input_files_with_new_column_technology,
        on=COLUMN_FILE_ID,
        how="left",
    ).sort(col(COLUMN_FILE_ID))

    spark_usages_by_file_with_loc_and_friendly_name = (
        spark_usages_by_file_join_input_files.select(
            COLUMN_EXECUTION_ID,
            COLUMN_EXECUTION_TIMESTAMP,
            COLUMN_CLIENT_EMAIL,
            COLUMN_FILE_ID,
            COLUMN_SUPPORTED,
            COLUMN_NOT_SUPPORTED,
            COLUMN_READINESS,
            COLUMN_LINES_OF_CODE,
            COLUMN_TECHNOLOGY,
        )
        .withColumnRenamed(COLUMN_LINES_OF_CODE, FRIENDLY_NAME_LINES_OF_CODE)
        .withColumnRenamed(COLUMN_FILE_ID, FRIENDLY_NAME_SOURCE_FILE)
        .withColumnRenamed(COLUMN_NOT_SUPPORTED, FRIENDLY_NAME_NOT_SUPPORTED)
    )

    return spark_usages_by_file_with_loc_and_friendly_name.toPandas()


def get_input_files_by_execution_id_and_grouped_by_dbc_files(execution_id_list):
    input_files_inventory = tables_backend.get_input_files_inventory_table_data_with_timestamp_and_email(
        execution_id_list,
    )

    no_dbc_files = input_files_inventory.where(
        (col(COLUMN_ORIGIN_FILE_PATH).isNull()) & (col(COLUMN_TECHNOLOGY) != DBC_TECHNOLOGY_KEY),
    ).select(
        COLUMN_EXECUTION_ID,
        COLUMN_EXECUTION_TIMESTAMP,
        COLUMN_CLIENT_EMAIL,
        COLUMN_ELEMENT,
        COLUMN_TECHNOLOGY,
        COLUMN_LINES_OF_CODE,
        COLUMN_BYTES,
        COLUMN_IGNORED,
    )

    dbc_files_table_data = input_files_inventory.where(
        (col(COLUMN_ORIGIN_FILE_PATH).is_not_null()) | (col(COLUMN_TECHNOLOGY) == DBC_TECHNOLOGY_KEY),
    ).select(
        COLUMN_EXECUTION_ID,
        COLUMN_EXECUTION_TIMESTAMP,
        COLUMN_CLIENT_EMAIL,
        COLUMN_ELEMENT,
        COLUMN_TECHNOLOGY,
        COLUMN_LINES_OF_CODE,
        COLUMN_BYTES,
        COLUMN_IGNORED,
        when(col(COLUMN_ORIGIN_FILE_PATH).is_null(), col(COLUMN_ELEMENT))
        .otherwise(col(COLUMN_ORIGIN_FILE_PATH))
        .alias(COLUMN_ORIGIN_FILE_PATH),
    )

    dbc_files = (
        dbc_files_table_data.groupBy(
            COLUMN_EXECUTION_ID,
            COLUMN_EXECUTION_TIMESTAMP,
            COLUMN_CLIENT_EMAIL,
            COLUMN_ORIGIN_FILE_PATH,
        )
        .agg(
            lit(DBC_TECHNOLOGY_KEY).alias(COLUMN_TECHNOLOGY),
            _sum(COLUMN_LINES_OF_CODE).alias(COLUMN_LINES_OF_CODE),
            _sum(COLUMN_BYTES).alias(COLUMN_BYTES),
            lit(FALSE_KEY).alias(COLUMN_IGNORED),
        )
        .withColumnRenamed(col(COLUMN_ORIGIN_FILE_PATH), COLUMN_ELEMENT)
    )

    input_files = no_dbc_files.union(dbc_files)
    return input_files
