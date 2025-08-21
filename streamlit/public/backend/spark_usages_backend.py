import pandas as pd

from public.backend import tables_backend
from public.backend.globals import *
from snowflake.snowpark import Window
from snowflake.snowpark.functions import any_value, cast, col, lit, upper
from snowflake.snowpark.functions import round as _round
from snowflake.snowpark.functions import sum as _sum
from snowflake.snowpark.types import BooleanType, DateType, StringType


def get_total(df, column):
    total = df[column].sum()
    return total


def get_percentages(total, df, column):
    percentages = list()
    for i in range(len(df)):
        percentage = df[column][i] * 100 / total
        percentage = f"{percentage:.2f}" + "%"
        percentages.append(percentage)
    return percentages


def get_sas_usages_by_execution_id_grouped_by_status(execution_id_list):
    spark_usages_inventory = tables_backend.get_spark_usages_inventory_table_data_by_execution_id(
        execution_id_list,
    )

    sas_usages_group_by_status = (
        spark_usages_inventory.group_by(COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED)
        .agg(_sum(COLUMN_COUNT).alias(COLUMN_USAGES))
        .orderBy(col(COLUMN_USAGES).desc())
    )

    sas_usages_group_by_status_pandas_dataframe = (
        sas_usages_group_by_status.withColumn(
            COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED,
            upper(col(COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED).try_cast(StringType())),
        ).withColumnRenamed(COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED, COLUMN_STATUS)
    ).toPandas()
    sas_usages_group_by_status_pandas_dataframe[COLUMN_STATUS] = sas_usages_group_by_status_pandas_dataframe[
        COLUMN_STATUS
    ].map(
        {"TRUE": FRIENDLY_NAME_SAS_SUPPORTED, "FALSE": FRIENDLY_NAME_SAS_NOT_SUPPORTED},
    )
    total = get_total(sas_usages_group_by_status_pandas_dataframe, COLUMN_USAGES)
    sas_usages_group_by_status_pandas_dataframe[FRIENDLY_NAME_PERCENTAGES] = (
        sas_usages_group_by_status_pandas_dataframe[COLUMN_USAGES].mul(100).div(total).round(2)
    )
    total_percentage = get_total(sas_usages_group_by_status_pandas_dataframe, FRIENDLY_NAME_PERCENTAGES)
    total_df = pd.DataFrame(
        [["Total", total, total_percentage]],
        columns=[COLUMN_STATUS, COLUMN_USAGES, FRIENDLY_NAME_PERCENTAGES],
    )
    spark_usages_group_by_status_pandas_dataframe = pd.concat(
        [sas_usages_group_by_status_pandas_dataframe, total_df],
        ignore_index=True,
    )

    return spark_usages_group_by_status_pandas_dataframe[[COLUMN_STATUS, COLUMN_USAGES, FRIENDLY_NAME_PERCENTAGES]]


def get_spark_usages_by_execution_id_grouped_by_status(execution_id_list):
    spark_usages_inventory = tables_backend.get_spark_usages_inventory_table_data_by_execution_id(
        execution_id_list,
    )

    spark_usages_group_by_status = (
        spark_usages_inventory.group_by(COLUMN_STATUS)
        .agg(_sum(COLUMN_COUNT).alias(COLUMN_USAGES))
        .orderBy(col(COLUMN_USAGES).desc())
    )

    spark_usages_group_by_status_pandas_dataframe = spark_usages_group_by_status.to_pandas()
    total = get_total(spark_usages_group_by_status_pandas_dataframe, COLUMN_USAGES)
    percentages = get_percentages(
        total,
        spark_usages_group_by_status_pandas_dataframe,
        COLUMN_USAGES,
    )
    spark_usages_group_by_status_pandas_dataframe[FRIENDLY_NAME_PERCENTAGES] = percentages
    total_df = pd.DataFrame(
        [["Total", total, None]],
        columns=[COLUMN_STATUS, COLUMN_USAGES, FRIENDLY_NAME_PERCENTAGES],
    )
    spark_usages_group_by_status_pandas_dataframe = pd.concat(
        [spark_usages_group_by_status_pandas_dataframe, total_df],
        ignore_index=True,
    )
    spark_usages_group_by_status_pandas_dataframe[COLUMN_STATUS] = spark_usages_group_by_status_pandas_dataframe[
        COLUMN_STATUS
    ].str.replace("WorkAround", "Workaround")
    spark_usages_group_by_status_pandas_dataframe = spark_usages_group_by_status_pandas_dataframe.rename(
        columns={
            COLUMN_STATUS: FRIENDLY_NAME_STATUS_CATEGORY,
            COLUMN_USAGES: COLUMN_COUNT,
        },
    )

    return spark_usages_group_by_status_pandas_dataframe


def get_spark_usages_by_execution_id_filtered_by_spark_connect_status(execution_id_list, snowpark_connect_key):
    spark_usages_inventory = tables_backend.get_spark_usages_inventory_table_data_by_execution_id(
        execution_id_list,
    )

    spark_usages_filtered_by_snowpark_connect = spark_usages_inventory.where(
        col(COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED).try_cast(BooleanType()) == snowpark_connect_key,
    )

    return _process(spark_usages_filtered_by_snowpark_connect)


def get_spark_usages_by_execution_id_filtered_by_status(execution_id_list, status):
    spark_usages_inventory = tables_backend.get_spark_usages_inventory_table_data_by_execution_id(
        execution_id_list,
    )

    if status == "Workaround":
        status = "WorkAround"

    spark_usages_filtered_by_status = spark_usages_inventory.where(
        col(COLUMN_STATUS) == status,
    )

    return _process(spark_usages_filtered_by_status)


def _process(spark_usages_table_data):
    spark_usages_filtered_by_status_data = (
        spark_usages_table_data.select(
            COLUMN_ELEMENT,
            COLUMN_SUPPORTED,
            COLUMN_SNOWCONVERT_CORE_VERSION,
            COLUMN_STATUS,
            COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED,
        )
        .withColumn(COLUMN_SUPPORTED, upper(col(COLUMN_SUPPORTED)).try_cast(BooleanType()))
        .withColumn(
            COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED,
            upper(col(COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED)).try_cast(BooleanType()),
        )
        .withColumnRenamed(COLUMN_ELEMENT, FRIENDLY_NAME_SPARK_FULLY_QUALIFIED_NAME)
        .withColumnRenamed(COLUMN_SNOWCONVERT_CORE_VERSION, TOOL_VERSION)
        .dropDuplicates()
    )
    return spark_usages_filtered_by_status_data.toPandas()


def get_unsupported_spark_usages_inventory_by_execution_id(execution_id_list):
    spark_usages_table_data = (
        tables_backend.get_spark_usages_inventory_table_data_by_execution_id_with_timestamp_and_email(
            execution_id_list,
        )
    )
    spark_usages_unsupported = (
        spark_usages_table_data.where(col(COLUMN_SUPPORTED) == FALSE_KEY)
        .select(
            COLUMN_EXECUTION_ID,
            COLUMN_EXECUTION_TIMESTAMP,
            COLUMN_ELEMENT,
            COLUMN_PROJECT_ID,
            COLUMN_FILE_ID,
            COLUMN_COUNT,
            COLUMN_LINE,
        )
        .withColumnRenamed(COLUMN_FILE_ID, COLUMN_SOURCE_FILE)
    )
    return spark_usages_unsupported.toPandas()


def get_unsupported_spark_usages_between_dates(start_date, end_date):
    spark_usages_table_data = tables_backend.get_spark_usages_inventory_table_data_with_timestamp_and_email()
    spark_usages_unsupported = spark_usages_table_data.where(col(COLUMN_SUPPORTED) == lit(FALSE_KEY))
    spark_usages_unsupported_and_filtered = spark_usages_unsupported.where(
        cast(col(COLUMN_EXECUTION_TIMESTAMP), DateType()).between(start_date, end_date),
    )

    spark_usages_unsupported_and_filtered_selected_columns = (
        spark_usages_unsupported_and_filtered.select(
            COLUMN_EXECUTION_ID,
            COLUMN_EXECUTION_TIMESTAMP,
            COLUMN_ELEMENT,
            COLUMN_COUNT,
            COLUMN_SUPPORTED,
        )
        .withColumn(COLUMN_TOTAL, _sum(col(COLUMN_COUNT)).over(Window.partitionBy(COLUMN_EXECUTION_ID)))
        .groupBy(COLUMN_ELEMENT, COLUMN_EXECUTION_ID, COLUMN_EXECUTION_TIMESTAMP)
        .agg(_sum(COLUMN_COUNT).alias(COLUMN_COUNT), any_value(COLUMN_TOTAL).alias(COLUMN_TOTAL))
        .withColumn(COLUMN_PERCENT, _round(col(COLUMN_COUNT) * 100 / col(COLUMN_TOTAL), 2))
        .sort(col(COLUMN_PERCENT).desc())
    )

    dataframe_with_friendly_name = spark_usages_unsupported_and_filtered_selected_columns.withColumnRenamed(
        COLUMN_EXECUTION_ID,
        FRIENDLY_NAME_EXECUTION_ID,
    ).withColumnRenamed(COLUMN_EXECUTION_TIMESTAMP, FRIENDLY_NAME_EXECUTION_TIMESTAMP)

    return dataframe_with_friendly_name.toPandas()
