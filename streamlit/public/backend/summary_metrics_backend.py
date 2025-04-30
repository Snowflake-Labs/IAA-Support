import datetime

from public.backend import tables_backend
from public.backend.globals import *
from snowflake.snowpark.functions import (
    avg as _avg,
)
from snowflake.snowpark.functions import (
    col,
    count,
    datediff,
    iff,
    lit,
    max,
    when,
)
from snowflake.snowpark.functions import (
    round as _round,
)
from snowflake.snowpark.functions import (
    sum as _sum,
)
from snowflake.snowpark.functions import (
    to_char as _to_char,
)


def get_executions_count_by_tool_name(execution_id_list):
    has_execution_id = execution_selected(execution_id_list)
    execution_info_table_data = (
        tables_backend.get_execution_info_table_data_by_execution_id(execution_id_list)
        if has_execution_id
        else tables_backend.get_execution_info_table_data()
    )

    execution_info_table_data_count_by_tool_name = (
        execution_info_table_data.select(
            col(COLUMN_TOOL_NAME), col(COLUMN_EXECUTION_ID),
        )
        .distinct()
        .select(
            iff(
                col(COLUMN_TOOL_NAME).startswith(PYTHON_KEY),
                lit(PYTHON_KEY),
                lit(SCALA_KEY),
            ).alias(COLUMN_TOOL_NAME),
            col(COLUMN_EXECUTION_ID),
        )
        .groupBy(COLUMN_TOOL_NAME)
        .agg(count(COLUMN_EXECUTION_ID).alias(COLUMN_EXECUTION_ID_COUNT))
    )
    return execution_info_table_data_count_by_tool_name


def get_executions_count(execution_id_list):
    executions_by_tool_name = get_executions_count_by_tool_name(execution_id_list)
    executions_count_by_tool_name = executions_by_tool_name.select(
        _sum(COLUMN_EXECUTION_ID_COUNT).alias(COLUMN_EXECUTION_ID_COUNT),
    )
    return executions_count_by_tool_name.toPandas()[COLUMN_EXECUTION_ID_COUNT]


def get_executions_count_by_technology(execution_id_list):
    executions_by_tool_name = get_executions_count_by_tool_name(execution_id_list)
    return executions_by_tool_name.toPandas()


def execution_selected(execution_id_list):
    return execution_id_list is not None and len(execution_id_list) > 0


def get_hours_from_last_execution(execution_id_list):
    hours = 0
    has_execution_id = execution_selected(execution_id_list)
    if has_execution_id:
        execution_info_table_data = (
            tables_backend.get_execution_info_table_data_by_execution_id(
                execution_id_list,
            )
        )
        current_time = lit(datetime.datetime.utcnow())
        hours = (
            execution_info_table_data.select(
                max(COLUMN_EXECUTION_TIMESTAMP).alias(COLUMN_MAX_TIMESTAMP),
            )
            .select(
                datediff(HOUR_KEY, col(COLUMN_MAX_TIMESTAMP), current_time).alias(
                    COLUMN_HOURS_DIFFERENCE,
                ),
            )
            .to_pandas()
            .HOURS_DIFFERENCE.iloc[0]
        )

    else:
        execution_info_table_data = tables_backend.get_execution_info_table_data()
        current_time = lit(datetime.datetime.utcnow())
        hours = (
            execution_info_table_data.select(
                datediff(HOUR_KEY, max(COLUMN_EXECUTION_TIMESTAMP), current_time).alias(
                    COLUMN_HOURS_DIFFERENCE,
                ),
            )
            .to_pandas()
            .HOURS_DIFFERENCE.iloc[0]
        )

    return float(hours)


def get_average_readiness_score(execution_id_list):
    has_execution_id = execution_selected(execution_id_list)
    execution_info_table_data = (
        tables_backend.get_execution_info_table_data_by_execution_id(execution_id_list)
        if has_execution_id
        else tables_backend.get_execution_info_table_data()
    )
    execution_info_table_data_with_avg = execution_info_table_data.agg(
        _round(_avg(COLUMN_READINESS_SCORE), 2).alias(COLUMN_AVERAGE_READINESS_SCORE),
    )
    average_readiness_score = (
        execution_info_table_data_with_avg.toPandas().AVERAGE_READINESS_SCORE.iloc[0]
    )
    return float(average_readiness_score)


def get_cumulative_readiness_score(execution_id_list):
    has_execution_id = execution_selected(execution_id_list)
    spark_usages_inventory_table_data = (
        tables_backend.get_spark_usages_inventory_table_data_by_execution_id(
            execution_id_list,
        )
        if has_execution_id
        else tables_backend.get_spark_usages_inventory_table_data()
    )

    spark_usages_identifies_count = spark_usages_inventory_table_data.groupBy(
        COLUMN_EXECUTION_ID,
    ).agg(_sum(COLUMN_COUNT).alias(COLUMN_USAGES_IDENTIFIED_COUNT))

    spark_usages_supported_count = (
        spark_usages_inventory_table_data.where(col(COLUMN_SUPPORTED) == TRUE_KEY)
        .groupBy(COLUMN_EXECUTION_ID)
        .agg(_sum(COLUMN_COUNT).alias(COLUMN_USAGES_SUPPORTED_COUNT))
    )

    spark_usages_identifies = spark_usages_identifies_count.join(
        spark_usages_supported_count, COLUMN_EXECUTION_ID,
    )

    spark_usages_with_cumulative_readiness_score = spark_usages_identifies.select(
        _round(
            (col(COLUMN_USAGES_SUPPORTED_COUNT) / col(COLUMN_USAGES_IDENTIFIED_COUNT))
            * 100,
            2,
        ).alias(COLUMN_CUMULATIVE_READINESS_SCORE),
    )

    cumulative_readiness_score_series = (
        spark_usages_with_cumulative_readiness_score.toPandas().CUMULATIVE_READINESS_SCORE
    )
    cumulative_readiness_score = (
        cumulative_readiness_score_series.iloc[0]
        if len(cumulative_readiness_score_series)
        else 0
    )
    return float(cumulative_readiness_score)


def get_lines_of_code_count_by_technology(execution_id_list):
    has_execution_id = execution_selected(execution_id_list)
    input_files_inventory_table_data = (
        tables_backend.get_input_files_inventory_table_data_by_execution_id(
            execution_id_list,
        )
        if has_execution_id
        else tables_backend.get_input_files_inventory_table_data()
    )

    lines_of_code_grouped_by_technology = (
        input_files_inventory_table_data.groupBy(COLUMN_TECHNOLOGY)
        .agg(_sum(COLUMN_LINES_OF_CODE).alias(COLUMN_LINES_OF_CODE))
        .orderBy(COLUMN_LINES_OF_CODE, ascending=0)
        .limit(5)
    )
    return lines_of_code_grouped_by_technology.toPandas()


def get_files_count_by_technology(execution_id_list):
    has_execution_id = execution_selected(execution_id_list)
    input_files_inventory_table_data = (
        tables_backend.get_input_files_inventory_table_data_by_execution_id(
            execution_id_list,
        )
        if has_execution_id
        else tables_backend.get_input_files_inventory_table_data()
    )

    files_count_by_technology = (
        input_files_inventory_table_data.groupBy(COLUMN_TECHNOLOGY)
        .agg(count(COLUMN_ELEMENT).alias(COLUMN_TOTAL_CODE_FILES))
        .orderBy(COLUMN_TOTAL_CODE_FILES, ascending=0)
        .limit(5)
    )
    return files_count_by_technology.toPandas()


def get_readiness_score(execution_id_list):
    has_execution_id = execution_selected(execution_id_list)
    execution_info_table_data = (
        tables_backend.get_execution_info_table_data_by_execution_id(execution_id_list)
        if has_execution_id
        else tables_backend.get_execution_info_table_data()
    )
    average_readiness_score = (
        execution_info_table_data.agg(
            _round(_avg(COLUMN_SPARK_API_READINESS_SCORE), 2).alias(
                COLUMN_AVERAGE_READINESS_SCORE,
            ),
        )
        .toPandas()
        .AVERAGE_READINESS_SCORE.iloc[0]
    )

    return float(average_readiness_score)


def get_third_party_readiness_score(execution_id_list):
    has_execution_id = execution_selected(execution_id_list)
    execution_info_table_data = (
        tables_backend.get_execution_info_table_data_by_execution_id(execution_id_list)
        if has_execution_id
        else tables_backend.get_execution_info_table_data()
    )

    execution_info_table_data_filter = execution_info_table_data.select(
        when(col(COLUMN_THIRD_PARTY_READINESS_SCORE) == NA_KEY, 0)
        .when(col(COLUMN_THIRD_PARTY_READINESS_SCORE).is_null(), 0)
        .otherwise(col(COLUMN_THIRD_PARTY_READINESS_SCORE))
        .alias(COLUMN_THIRD_PARTY_READINESS_SCORE),
    )

    average_readiness_score = (
        execution_info_table_data_filter.agg(
            _round(_avg(COLUMN_THIRD_PARTY_READINESS_SCORE), 2).alias(
                COLUMN_AVERAGE_READINESS_SCORE,
            ),
        )
        .toPandas()
        .AVERAGE_READINESS_SCORE.iloc[0]
    )

    return float(average_readiness_score)


def get_sql_readiness_score(execution_id_list):
    has_execution_id = execution_selected(execution_id_list)
    execution_info_table_data = (
        tables_backend.get_execution_info_table_data_by_execution_id(execution_id_list)
        if has_execution_id
        else tables_backend.get_execution_info_table_data()
    )

    execution_info_table_data_filter = execution_info_table_data.select(
        when(col(COLUMN_SQL_READINESS_SCORE) == NA_KEY, 0)
        .when(col(COLUMN_SQL_READINESS_SCORE).is_null(), 0)
        .otherwise(col(COLUMN_SQL_READINESS_SCORE))
        .alias(COLUMN_SQL_READINESS_SCORE),
    )

    average_readiness_score = (
        execution_info_table_data_filter.agg(
            _round(_avg(COLUMN_SQL_READINESS_SCORE), 2).alias(
                COLUMN_AVERAGE_READINESS_SCORE,
            ),
        )
        .toPandas()
        .AVERAGE_READINESS_SCORE.iloc[0]
    )

    return float(average_readiness_score)


def get_date_last_execution(execution_id_list):
    has_execution_id = execution_selected(execution_id_list)
    execution_info_table_data = (
        tables_backend.get_execution_info_table_data_by_execution_id(execution_id_list)
        if has_execution_id
        else tables_backend.get_execution_info_table_data()
    )

    date_last_execution = (
        execution_info_table_data.agg(
            _to_char(max(COLUMN_EXECUTION_TIMESTAMP), DATE_LAST_DATE_FORMAT_KEY).alias(
                COLUMN_DATE_LAST_EXECUTION,
            ),
        )
        .toPandas()
        .DATE_LAST_EXECUTION.iloc[0]
    )

    return date_last_execution


def get_executions_selected_count(execution_id_list):
    executions_selected_count = len(execution_id_list)
    return executions_selected_count
