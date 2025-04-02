from typing import List, Optional

import pandas
import snowflake
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.functions import col, lit, when, concat, count
from snowflake.snowpark.functions import col, lower, ifnull, parse_json, replace

from public.backend import app_snowpark_utils as utils
from public.backend.app_snowpark_utils import convert_to_int
from public.backend.globals import *
from public.backend.query_quality_handler import with_table_quality_handler

double_quote = "\""
empty_string = ""

def get_artifacts_dependency_table_data_by_execution_id(execution_id_list: List, selected_files_list: Optional[List] = None) -> snowflake.snowpark.DataFrame:

    data = get_artifacts_dependency_tabla_data(execution_id_list, selected_files_list)
    data_friendly_name = data.select(
        COLUMN_FILE_ID,
        COLUMN_DEPENDENCY,
        COLUMN_TYPE,
        COLUMN_ARGUMENTS,
        COLUMN_STATUS_DETAIL,
        COLUMN_LOCATION,
    )

    return data_friendly_name

def get_correct_column_to_search(column_to_search: str) -> str:
    return ARTIFACT_COLUMN_MAPPING.get(column_to_search, column_to_search)

def get_artifacts_summary(execution_id_list:List, code_file_path_value: str, column_to_search:str, option_criteria:str, total_to_filter:str) -> pandas.DataFrame:

    dependency_data = get_artifacts_dependency_tabla_data(execution_id_list)

    if code_file_path_value:
        dependency_data = dependency_data.filter(
            lower(col(COLUMN_FILE_ID)).like(f"%{code_file_path_value.lower()}%"))

    pivot_df =  dependency_data.groupBy(COLUMN_EXECUTION_ID, COLUMN_TYPE, COLUMN_FILE_ID) \
        .agg(count('*').alias(COLUMN_COUNT_BY_TYPE)) \
        .pivot(COLUMN_TYPE, [
            COLUMN_USER_CODE_FILE,
            COLUMN_IO_FILES,
            COLUMN_THIRD_PARTY_LIBRARIES,
            COLUMN_UNKNOWN_LIBRARIES,
            COLUMN_SQL_OBJECTS]) \
        .sum(COLUMN_COUNT_BY_TYPE)

    # join total count
    totals_df = dependency_data.groupBy(COLUMN_EXECUTION_ID, COLUMN_FILE_ID).agg(count('*').alias(COLUMN_TOTAL_DEPENDENCIES))

    # get total issues
    total_issues_df = dependency_data \
        .filter(lower(col(COLUMN_STATUS_DETAIL)).isin(['notparsed', 'notsupported', 'doesnotexists', 'unknown'])) \
        .groupBy(COLUMN_EXECUTION_ID, COLUMN_FILE_ID) \
        .agg(count('*').alias(COLUMN_TOTAL_ISSUES))

    # join dfs
    joined_df = pivot_df.join(totals_df, on=[COLUMN_EXECUTION_ID, COLUMN_FILE_ID], how='left') \
        .join(total_issues_df, on=[COLUMN_EXECUTION_ID, COLUMN_FILE_ID], how='left')

    validCriteriaOption = option_criteria in [ArtifactCriteriaOptions.MORE_THAN, ArtifactCriteriaOptions.LESS_THAN, ArtifactCriteriaOptions.EQUAL_TO]
    validColumnOption = column_to_search in [
        ArtifactColumnOptions.FRIENDLY_NAME_UNKNOWN_LIBRARIES,
        ArtifactColumnOptions.FRIENDLY_NAME_USER_CODE_FILES,
        ArtifactColumnOptions.FRIENDLY_NAME_THIRD_PARTY_LIBRARIES,
        ArtifactColumnOptions.FRIENDLY_NAME_INPUT_OUTPUT_SOURCES,
        ArtifactColumnOptions.FRIENDLY_NAME_SQL_OBJECTS,
        ArtifactColumnOptions.FRIENDLY_NAME_TOTAL_DEPENDENCIES,
        ArtifactColumnOptions.FRIENDLY_NAME_TOTAL_ISSUES]

    if validCriteriaOption and validColumnOption:
        column_name = get_correct_column_to_search(column_to_search)
        total_to_filter = convert_to_int(total_to_filter)

        if option_criteria == ArtifactCriteriaOptions.MORE_THAN:
            joined_df = joined_df.filter(col(column_name) > total_to_filter)
        elif option_criteria == ArtifactCriteriaOptions.LESS_THAN:
            joined_df = joined_df.filter(col(column_name) < total_to_filter)
        elif option_criteria == ArtifactCriteriaOptions.EQUAL_TO:
            joined_df = joined_df.filter(col(column_name) == total_to_filter)
        else:
            raise ValueError(f"Unknown option_criteria: {option_criteria}")

    # sort by file
    joined_df = joined_df.order_by(COLUMN_FILE_ID)

    data_friendly_name = generate_artifacts_summary_dataframe_with_selected_column(joined_df)
    return data_friendly_name.toPandas()


def generate_artifacts_summary_dataframe_with_selected_column(artifacts_df: DataFrame, selected: bool = False) -> snowflake.snowpark.dataframe:
    if artifacts_df is None or artifacts_df.count() == 0:
        return artifacts_df

    artifacts_with_selector = artifacts_df.withColumn(FRIENDLY_NAME_SELECT, lit(selected))

    selected_executions_friendly_name = artifacts_with_selector.select(
        FRIENDLY_NAME_SELECT,
        col(COLUMN_FILE_ID).alias(FRIENDLY_NAME_CODE_FILE_PATH),
        ifnull(COLUMN_PIVOTED_UNKNOWN_LIBRARIES,lit("0")).alias(FRIENDLY_NAME_UNKNOWN_LIBRARIES),
        ifnull(COLUMN_PIVOTED_USER_CODE_FILE,lit("0")).alias(FRIENDLY_NAME_USER_CODE_FILES),
        ifnull(COLUMN_PIVOTED_THIRD_PARTY_LIBRARIES,lit("0")).alias(FRIENDLY_NAME_THIRD_PARTY_LIBRARIES),
        ifnull(COLUMN_PIVOTED_IO_FILES,lit("0")).alias(FRIENDLY_NAME_INPUT_OUTPUT_SOURCES),
        ifnull(COLUMN_PIVOTED_SQL_OBJECTS,lit("0")).alias(FRIENDLY_NAME_SQL_OBJECTS),
        ifnull(COLUMN_TOTAL_DEPENDENCIES,lit("0")).alias(FRIENDLY_NAME_TOTAL_DEPENDENCIES),
        ifnull(COLUMN_TOTAL_ISSUES,lit("0")).alias(FRIENDLY_NAME_TOTAL_ISSUES),
    )

    return selected_executions_friendly_name


def  get_artifacts_dependency_tabla_data(execution_id_list:List, selected_files_list: List = None) -> DataFrame:
    session = utils.get_session()
    artifactsTable = session.table(TABLE_ARTIFACT_DEPENDENCY_INVENTORY)
    table_data = with_table_quality_handler(artifactsTable, TABLE_ARTIFACT_DEPENDENCY_INVENTORY) \
        .where((col(COLUMN_EXECUTION_ID).isin(execution_id_list)))

    if selected_files_list is not None and len(selected_files_list) > 0:
        table_data = table_data.where(col(COLUMN_FILE_ID).isin(selected_files_list))

    return table_data


def get_artifact_table():
    session = utils.get_session()
    artifactsTable = session.table(TABLE_ARTIFACT_DEPENDENCY_INVENTORY)
    return artifactsTable


def get_user_code_file_dependency_detailed(dependency_df: SnowparkDataFrame) -> SnowparkDataFrame:
    user_code_file_dependency_df = dependency_df \
    .where(col(COLUMN_TYPE) == lit(USER_CODE_FILE_ARTIFACT_DEPENDENCY_TYPE_KEY)) \
    .sort(col(COLUMN_STATUS_DETAIL))


    user_code_file_detailed_df = user_code_file_dependency_df.select(
        parse_json(col(COLUMN_ARGUMENTS)).alias(COLUMN_ARGUMENTS), col(COLUMN_DEPENDENCY), col(COLUMN_STATUS_DETAIL)) \
        .select( col(COLUMN_DEPENDENCY),
            replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_EXTENSION_KEY], double_quote, empty_string).alias(
                COLUMN_USER_CODE_FILE_EXTENSION_FRIENDLY_NAME),
            replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_TECHNOLOGY_KEY], double_quote, empty_string).alias(
                COLUMN_USER_CODE_FILE_TECHNOLOGY_FRIENDLY_NAME),
            replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_BYTES_KEY], double_quote, empty_string).alias(
                COLUMN_USER_CODE_FILE_BYTES_FRIENDLY_NAME),
            replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_CHARACTER_LENGTH_KEY], double_quote, empty_string).alias(
                COLUMN_USER_CODE_FILE_CHARACTER_LENGTH_FRIENDLY_NAME),
            replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_LINES_OF_CODE_KEY], double_quote, empty_string).alias(
                COLUMN_USER_CODE_FILE_LINES_OF_CODE_FRIENDLY_NAME),
            when(col(COLUMN_STATUS_DETAIL) == lit(USER_CODE_FILE_PARSED_STATUS_KEY),
                 concat(lit(DEPENDENCY_ARTIFACT_GREEN_ICON + " "), col(COLUMN_STATUS_DETAIL)))
            .otherwise(concat(lit(DEPENDENCY_ARTIFACT_RED_ICON + " "), col(COLUMN_STATUS_DETAIL)))
            .alias(COLUMN_USER_CODE_FILE_STATUS_FRIENDLY_NAME))

    return user_code_file_detailed_df


def get_input_output_source_dependency_detailed(dependency_df: SnowparkDataFrame) -> SnowparkDataFrame:
    io_sources_dependency_df = dependency_df \
        .where(col(COLUMN_TYPE) == lit(IO_SOURCES_ARTIFACT_DEPENDENCY_TYPE_KEY)) \
        .sort(col(COLUMN_STATUS_DETAIL))

    io_sources_detailed_df = io_sources_dependency_df \
        .select(
        parse_json(col(COLUMN_ARGUMENTS)).alias(COLUMN_ARGUMENTS), col(COLUMN_DEPENDENCY), col(COLUMN_STATUS_DETAIL)) \
        .select(
        col(COLUMN_DEPENDENCY).alias(FRIENDLY_NAME_DEPENDENCY),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_MODE_KEY], double_quote, empty_string).alias(COLUMN_IO_SOURCES_MODE_FRIENDLY_NAME),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_FORMAT_KEY], double_quote, empty_string).alias(COLUMN_IO_SOURCES_FORMAT_FRIENDLY_NAME),
        when(col(COLUMN_STATUS_DETAIL) == lit(IO_SOURCE_EXISTS_STATUS_KEY), LABEL_EXISTS)
        .otherwise(LABEL_NOT_EXISTS)
        .alias(COLUMN_IO_SOURCES_STATUS_FRIENDLY_NAME))

    return io_sources_detailed_df


def get_third_party_libraries_detailed(dependency_df: SnowparkDataFrame) -> SnowparkDataFrame:
    third_party_libraries_dependency_df = dependency_df \
        .where(
            col(COLUMN_TYPE) == lit(THIRD_PARTY_LIBRARIES_ARTIFACT_DEPENDENCY_TYPE_KEY)) \
        .sort(col(COLUMN_STATUS_DETAIL))

    third_party_libraries_dependency_detailed_df = third_party_libraries_dependency_df.select(
        col(COLUMN_DEPENDENCY).alias(FRIENDLY_NAME_DEPENDENCY),
        when(col(COLUMN_STATUS_DETAIL) == lit(THIRD_PARTY_LIBRARIES_SUPPORTED_STATUS_KEY),
             concat(lit(DEPENDENCY_ARTIFACT_GREEN_ICON + " "), col(COLUMN_STATUS_DETAIL)))
        .otherwise(concat(lit(DEPENDENCY_ARTIFACT_RED_ICON + " "), col(COLUMN_STATUS_DETAIL)))
        .alias(COLUMN_THIRD_PARTY_LIBRARIES_STATUS_FRIENDLY_NAME))

    return third_party_libraries_dependency_detailed_df


def get_unknown_libraries_detailed(dependency_df: SnowparkDataFrame) -> SnowparkDataFrame:
    unknown_libraries_dependency_df = dependency_df \
        .where(
            col(COLUMN_TYPE) == lit(UNKNOWN_LIBRARIES_ARTIFACT_DEPENDENCY_TYPE_KEY)) \
        .sort(col(COLUMN_STATUS_DETAIL))

    unknown_libraries_dependency_detailed_df = unknown_libraries_dependency_df.select(
        col(COLUMN_DEPENDENCY).alias(FRIENDLY_NAME_DEPENDENCY),
        when(col(COLUMN_STATUS_DETAIL) == lit(UNKNOWN_LIBRARIES_SUPPORTED_STATUS_KEY),
             concat(lit(DEPENDENCY_ARTIFACT_GREEN_ICON + " "), col(COLUMN_STATUS_DETAIL)))
        .otherwise(concat(lit(DEPENDENCY_ARTIFACT_RED_ICON + " "), col(COLUMN_STATUS_DETAIL)))
        .alias(COLUMN_UNKNOWN_LIBRARIES_STATUS_FRIENDLY_NAME))

    return unknown_libraries_dependency_detailed_df


def get_sql_object_detailed(dependency_df: SnowparkDataFrame) -> SnowparkDataFrame:

    sql_object_dependency_df = dependency_df \
        .where(
            col(COLUMN_TYPE) == lit(SQL_OBJECT_ARTIFACT_DEPENDENCY_TYPE_KEY)) \
        .sort(col(COLUMN_STATUS_DETAIL))

    sql_object_dependency_detailed_df = sql_object_dependency_df \
        .select(
        parse_json(col(COLUMN_ARGUMENTS)).alias(COLUMN_ARGUMENTS), col(COLUMN_DEPENDENCY), col(COLUMN_STATUS_DETAIL)) \
        .select(
        col(COLUMN_DEPENDENCY).alias(COLUMN_SQL_LIBRARY_OBJECT_FRIENDLY_NAME),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_CATEGORY_KEY], double_quote, empty_string).alias(COLUMN_SQL_OBJECT_CATEGORY_FRIENDLY_NAME),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_DIALECT_KEY], double_quote, empty_string).alias(COLUMN_SQL_OBJECT_DIALECT_FRIENDLY_NAME),
        when(col(COLUMN_STATUS_DETAIL) == lit(SQL_OBJECT_EXISTS_STATUS_KEY), LABEL_EXISTS)
        .otherwise(LABEL_NOT_EXISTS)
        .alias(COLUMN_UNKNOWN_LIBRARIES_STATUS_FRIENDLY_NAME))

    return sql_object_dependency_detailed_df


def get_total_green_dependencies(dependency_df: SnowparkDataFrame) -> int:
    total_green_df = dependency_df \
        .where(col(COLUMN_STATUS_DETAIL).isin(ARTIFACT_DEPENDENCY_SUPPORTED_STATUS_COLLECTION)) \
        .select(count(col(COLUMN_STATUS_DETAIL)))
    return get_count_from_df(total_green_df)


def get_total_red_dependencies(dependency_df: SnowparkDataFrame) -> int:
    total_red_df = dependency_df \
        .where(col(COLUMN_STATUS_DETAIL).isin(ARTIFACT_DEPENDENCY_NOT_SUPPORTED_STATUS_COLLECTION)) \
        .select(count(col(COLUMN_STATUS_DETAIL)))
    return get_count_from_df(total_red_df)


def get_count_green_dependency_by_type(dependency_df: SnowparkDataFrame, dependency_type: str) -> int:
    total_green_df = dependency_df \
        .where(col(COLUMN_TYPE) == lit(dependency_type)) \
        .where(col(COLUMN_STATUS_DETAIL).isin(ARTIFACT_DEPENDENCY_SUPPORTED_STATUS_COLLECTION)) \
        .select(count(col(COLUMN_STATUS_DETAIL)))
    return get_count_from_df(total_green_df)


def get_count_red_dependency_by_type(dependency_df: SnowparkDataFrame, dependency_type: str) -> int:
    total_red_df = dependency_df \
        .where(col(COLUMN_TYPE) == lit(dependency_type)) \
        .where(col(COLUMN_STATUS_DETAIL).isin(ARTIFACT_DEPENDENCY_NOT_SUPPORTED_STATUS_COLLECTION)) \
        .select(count(col(COLUMN_STATUS_DETAIL)))
    return get_count_from_df(total_red_df)


def get_count_from_df(df: SnowparkDataFrame) -> int:
    df_value = df.collect()
    if df_value:
        return df_value[0][0]
    else:
        return 0