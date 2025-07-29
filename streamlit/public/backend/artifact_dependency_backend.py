import pandas

import snowflake

from public.backend import app_snowpark_utils as utils
from public.backend.app_snowpark_utils import convert_to_int
from public.backend.globals import *
from public.backend.query_quality_handler import with_table_quality_handler
from public.backend.utils import normalize_path_column
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.functions import col, concat, count, ifnull, lit, lower, parse_json, replace, when


double_quote = '"'
empty_string = ""


def get_artifacts_dependency_table_data_by_execution_id(
    execution_id_list: list,
    selected_files_list: list | None = None,
) -> snowflake.snowpark.DataFrame:
    data = get_artifacts_dependency_table_data(execution_id_list, selected_files_list)
    data_friendly_name = data.select(
        COLUMN_FILE_ID,
        COLUMN_DEPENDENCY,
        COLUMN_TYPE,
        COLUMN_ARGUMENTS,
        COLUMN_STATUS_DETAIL,
        COLUMN_LOCATION,
        COLUMN_INDIRECT_DEPENDENCIES,
        ifnull(COLUMN_TOTAL_INDIRECT_DEPENDENCIES, lit(0)).alias(COLUMN_TOTAL_INDIRECT_DEPENDENCIES),
        COLUMN_DIRECT_PARENTS,
        ifnull(COLUMN_TOTAL_DIRECT_PARENTS, lit(0)).alias(COLUMN_TOTAL_DIRECT_PARENTS),
        COLUMN_INDIRECT_PARENTS,
        ifnull(COLUMN_TOTAL_INDIRECT_PARENTS, lit(0)).alias(COLUMN_TOTAL_INDIRECT_PARENTS),
    )

    return data_friendly_name


def get_correct_column_to_search(column_to_search: str) -> str:
    return ARTIFACT_COLUMN_MAPPING.get(column_to_search, column_to_search)


def get_artifacts_summary(
    execution_id: str,
    code_file_path_value: str,
    column_to_search: str,
    option_criteria: str,
    total_to_filter: str,
    selected_islands: list = None,
) -> pandas.DataFrame:
    data = get_artifacts_summary_table_data([execution_id])

    if selected_islands and len(selected_islands) > 0:
        data = data.where(col(COLUMN_ISLAND_ID).isin(selected_islands))

    if code_file_path_value:
        data = data.filter(lower(col(COLUMN_FILE_ID)).like(f"%{code_file_path_value.lower()}%"))

    validCriteriaOption = option_criteria in [
        ArtifactCriteriaOptions.MORE_THAN,
        ArtifactCriteriaOptions.LESS_THAN,
        ArtifactCriteriaOptions.EQUAL_TO,
    ]
    validColumnOption = column_to_search in [
        ArtifactColumnOptions.FRIENDLY_NAME_UNKNOWN_LIBRARIES,
        ArtifactColumnOptions.FRIENDLY_NAME_USER_CODE_FILES,
        ArtifactColumnOptions.FRIENDLY_NAME_THIRD_PARTY_LIBRARIES,
        ArtifactColumnOptions.FRIENDLY_NAME_INPUT_OUTPUT_SOURCES,
        ArtifactColumnOptions.FRIENDLY_NAME_SQL_OBJECTS,
        ArtifactColumnOptions.FRIENDLY_NAME_TOTAL_DEPENDENCIES,
        ArtifactColumnOptions.FRIENDLY_NAME_TOTAL_ISSUES,
    ]

    if validCriteriaOption and validColumnOption:
        column_name = get_correct_column_to_search(column_to_search)
        total_to_filter = convert_to_int(total_to_filter)

        match option_criteria:
            case ArtifactCriteriaOptions.MORE_THAN:
                filter_condition = col(column_name) > total_to_filter
            case ArtifactCriteriaOptions.LESS_THAN:
                filter_condition = col(column_name) < total_to_filter
            case ArtifactCriteriaOptions.EQUAL_TO:
                filter_condition = col(column_name) == total_to_filter
            case _:
                raise ValueError(f"Unknown option_criteria: {option_criteria}")
        data = data.filter(filter_condition)

    # sort by file
    data = data.order_by(COLUMN_FILE_ID)

    if data.count() == 0:
        return pandas.DataFrame()

    data_friendly_name = generate_artifacts_summary_dataframe_with_selected_column(data)
    pandas_df = data_friendly_name.toPandas()

    if not pandas_df.empty:
        pandas_df[COLUMN_ISLAND] = pandas_df[COLUMN_ISLAND].replace(0, "")

    return pandas_df


def generate_artifacts_summary_dataframe_with_selected_column(
    artifacts_df: DataFrame,
    selected: bool = False,
) -> snowflake.snowpark.DataFrame | None:
    if artifacts_df is None or artifacts_df.count() == 0:
        return None

    artifacts_with_selector = artifacts_df.withColumn(FRIENDLY_NAME_SELECT, lit(selected))

    selected_executions_friendly_name = artifacts_with_selector.select(
        FRIENDLY_NAME_SELECT,
        col(COLUMN_FILE_ID).alias(FRIENDLY_NAME_CODE_FILE_PATH),
        col(COLUMN_TOTAL_ISSUES).alias(FRIENDLY_NAME_TOTAL_ISSUES),
        when(col(COLUMN_ISLAND_ID).isNull(), lit(0)).otherwise(col(COLUMN_ISLAND_ID)).alias(COLUMN_ISLAND),
        col(COLUMN_TOTAL_DEPENDENCIES).alias(FRIENDLY_NAME_TOTAL_DEPENDENCIES),
        col(COLUMN_TOTAL_UNKNOWN_LIBRARIES).alias(FRIENDLY_NAME_UNKNOWN_LIBRARIES),
        col(COLUMN_TOTAL_USER_CODE_FILE).alias(FRIENDLY_NAME_USER_CODE_FILES),
        col(COLUMN_TOTAL_THIRD_PARTY_LIBRARIES).alias(FRIENDLY_NAME_THIRD_PARTY_LIBRARIES),
        col(COLUMN_TOTAL_IO_SOURCES).alias(FRIENDLY_NAME_INPUT_OUTPUT_SOURCES),
        col(COLUMN_TOTAL_SQL_OBJECTS).alias(FRIENDLY_NAME_SQL_OBJECTS),
    )

    return selected_executions_friendly_name


def get_graphics_information(data: snowflake.snowpark.DataFrame) -> pandas.DataFrame:
    distinct_df = (
        data.select(COLUMN_DEPENDENCY, COLUMN_TYPE, COLUMN_STATUS_DETAIL)
        .distinct()
        .groupBy(COLUMN_TYPE, COLUMN_STATUS_DETAIL)
        .count()
        .select(
            when(
                col(COLUMN_STATUS_DETAIL) == lit(IO_SOURCE_DOES_NOT_EXISTS_STATUS_KEY),
                lit("Does Not Exists"),
            )
            .when(
                col(COLUMN_STATUS_DETAIL) == lit(USER_CODE_FILE_NOT_PARSED_STATUS_KEY),
                lit("Not Parsed"),
            )
            .when(
                col(COLUMN_STATUS_DETAIL) == lit(UNKNOWN_LIBRARIES_NOT_SUPPORTED_STATUS_KEY),
                lit("Not Supported"),
            )
            .otherwise(col(COLUMN_STATUS_DETAIL))
            .alias("STATUS"),
            col("COUNT").alias("DEPENDENCIES"),
            col(COLUMN_TYPE),
        )
    )

    return distinct_df.toPandas()


def get_user_code_files_information(execution_id_list: list) -> pandas.DataFrame:
    data = get_artifacts_dependency_table_data(execution_id_list)
    user_code_data = data.where(col(COLUMN_TYPE) == lit(USER_CODE_FILE_ARTIFACT_DEPENDENCY_TYPE_KEY))
    return user_code_data.toPandas()


def get_artifacts_dependency_table_data(execution_id_list: list, selected_files_list: list = None) -> DataFrame:
    session = utils.get_session()
    artifactsTable = session.table(TABLE_ARTIFACT_DEPENDENCY_INVENTORY)
    table_data = (
        with_table_quality_handler(artifactsTable, TABLE_ARTIFACT_DEPENDENCY_INVENTORY)
        .where(
            col(COLUMN_EXECUTION_ID).isin(execution_id_list),
        )
        .with_column(
            COLUMN_FILE_ID,
            normalize_path_column(COLUMN_FILE_ID),
        )
        .with_column(
            COLUMN_DEPENDENCY,
            normalize_path_column(COLUMN_DEPENDENCY),
        )
    )

    if selected_files_list is not None and len(selected_files_list) > 0:
        table_data = table_data.where(col(COLUMN_FILE_ID).isin(selected_files_list))

    return table_data


def get_artifact_table():
    session = utils.get_session()
    artifactsTable = session.table(TABLE_ARTIFACT_DEPENDENCY_INVENTORY)
    return artifactsTable


def get_user_code_file_dependency_detailed(dependency_df: SnowparkDataFrame) -> SnowparkDataFrame:
    user_code_file_dependency_df = dependency_df.where(
        col(COLUMN_TYPE) == lit(USER_CODE_FILE_ARTIFACT_DEPENDENCY_TYPE_KEY),
    ).sort(col(COLUMN_STATUS_DETAIL))

    user_code_file_detailed_df = user_code_file_dependency_df.select(
        parse_json(col(COLUMN_ARGUMENTS)).alias(COLUMN_ARGUMENTS),
        col(COLUMN_FILE_ID),
        col(COLUMN_DEPENDENCY),
        col(COLUMN_STATUS_DETAIL),
        col(COLUMN_INDIRECT_DEPENDENCIES),
        col(COLUMN_TOTAL_INDIRECT_DEPENDENCIES),
        col(COLUMN_DIRECT_PARENTS),
        col(COLUMN_TOTAL_DIRECT_PARENTS),
        col(COLUMN_INDIRECT_PARENTS),
        col(COLUMN_TOTAL_INDIRECT_PARENTS),
    ).select(
        col(COLUMN_FILE_ID).alias(FRIENDLY_NAME_CODE_FILE_PATH),
        col(COLUMN_DEPENDENCY),
        ifnull(col(COLUMN_INDIRECT_DEPENDENCIES), lit(empty_string)).alias(
            COLUMN_USER_CODE_FILE_INDIRECT_DEPENDENCIES_FRIENDLY_NAME,
        ),
        ifnull(col(COLUMN_TOTAL_INDIRECT_DEPENDENCIES), lit(0)).alias(
            COLUMN_USER_CODE_FILE_TOTAL_INDIRECT_DEPENDENCIES_FRIENDLY_NAME,
        ),
        ifnull(col(COLUMN_DIRECT_PARENTS), lit(empty_string)).alias(COLUMN_USER_CODE_FILE_DIRECT_PARENTS_FRIENDLY_NAME),
        ifnull(col(COLUMN_TOTAL_DIRECT_PARENTS), lit(0)).alias(
            COLUMN_USER_CODE_FILE_TOTAL_DIRECT_PARENTS_FRIENDLY_NAME,
        ),
        ifnull(col(COLUMN_INDIRECT_PARENTS), lit(empty_string)).alias(
            COLUMN_USER_CODE_FILE_INDIRECT_PARENTS_FRIENDLY_NAME,
        ),
        ifnull(col(COLUMN_TOTAL_INDIRECT_PARENTS), lit(0)).alias(
            COLUMN_USER_CODE_FILE_TOTAL_INDIRECT_PARENTS_FRIENDLY_NAME,
        ),
        when(
            col(COLUMN_STATUS_DETAIL) == lit(USER_CODE_FILE_PARSED_STATUS_KEY),
            concat(lit(DEPENDENCY_ARTIFACT_GREEN_ICON + " "), col(COLUMN_STATUS_DETAIL)),
        )
        .otherwise(concat(lit(DEPENDENCY_ARTIFACT_RED_ICON + " "), col(COLUMN_STATUS_DETAIL)))
        .alias(COLUMN_USER_CODE_FILE_STATUS_FRIENDLY_NAME),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_EXTENSION_KEY], double_quote, empty_string).alias(
            COLUMN_USER_CODE_FILE_EXTENSION_FRIENDLY_NAME,
        ),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_TECHNOLOGY_KEY], double_quote, empty_string).alias(
            COLUMN_USER_CODE_FILE_TECHNOLOGY_FRIENDLY_NAME,
        ),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_BYTES_KEY], double_quote, empty_string).alias(
            COLUMN_USER_CODE_FILE_BYTES_FRIENDLY_NAME,
        ),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_CHARACTER_LENGTH_KEY], double_quote, empty_string).alias(
            COLUMN_USER_CODE_FILE_CHARACTER_LENGTH_FRIENDLY_NAME,
        ),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_LINES_OF_CODE_KEY], double_quote, empty_string).alias(
            COLUMN_USER_CODE_FILE_LINES_OF_CODE_FRIENDLY_NAME,
        ),
    )

    return user_code_file_detailed_df


def get_input_output_source_dependency_detailed(dependency_df: SnowparkDataFrame) -> SnowparkDataFrame:
    io_sources_dependency_df = dependency_df.where(
        col(COLUMN_TYPE) == lit(IO_SOURCES_ARTIFACT_DEPENDENCY_TYPE_KEY),
    ).sort(col(COLUMN_STATUS_DETAIL))

    io_sources_detailed_df = io_sources_dependency_df.select(
        col(COLUMN_FILE_ID),
        parse_json(col(COLUMN_ARGUMENTS)).alias(COLUMN_ARGUMENTS),
        col(COLUMN_DEPENDENCY),
        col(COLUMN_STATUS_DETAIL),
    ).select(
        col(COLUMN_FILE_ID).alias(FRIENDLY_NAME_CODE_FILE_PATH),
        col(COLUMN_DEPENDENCY).alias(FRIENDLY_NAME_DEPENDENCY),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_MODE_KEY], double_quote, empty_string).alias(
            COLUMN_IO_SOURCES_MODE_FRIENDLY_NAME,
        ),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_FORMAT_KEY], double_quote, empty_string).alias(
            COLUMN_IO_SOURCES_FORMAT_FRIENDLY_NAME,
        ),
        when(col(COLUMN_STATUS_DETAIL) == lit(IO_SOURCE_EXISTS_STATUS_KEY), LABEL_EXISTS)
        .otherwise(LABEL_NOT_EXISTS)
        .alias(COLUMN_IO_SOURCES_STATUS_FRIENDLY_NAME),
    )

    return io_sources_detailed_df


def get_third_party_libraries_detailed(dependency_df: SnowparkDataFrame) -> SnowparkDataFrame:
    third_party_libraries_dependency_df = dependency_df.where(
        col(COLUMN_TYPE) == lit(THIRD_PARTY_LIBRARIES_ARTIFACT_DEPENDENCY_TYPE_KEY),
    ).sort(col(COLUMN_STATUS_DETAIL))

    third_party_libraries_dependency_detailed_df = third_party_libraries_dependency_df.select(
        col(COLUMN_FILE_ID).alias(FRIENDLY_NAME_CODE_FILE_PATH),
        col(COLUMN_DEPENDENCY).alias(FRIENDLY_NAME_DEPENDENCY),
        when(
            col(COLUMN_STATUS_DETAIL) == lit(THIRD_PARTY_LIBRARIES_SUPPORTED_STATUS_KEY),
            concat(lit(DEPENDENCY_ARTIFACT_GREEN_ICON + " "), col(COLUMN_STATUS_DETAIL)),
        )
        .otherwise(concat(lit(DEPENDENCY_ARTIFACT_RED_ICON + " "), col(COLUMN_STATUS_DETAIL)))
        .alias(COLUMN_THIRD_PARTY_LIBRARIES_STATUS_FRIENDLY_NAME),
    )

    return third_party_libraries_dependency_detailed_df


def get_unknown_libraries_detailed(dependency_df: SnowparkDataFrame) -> SnowparkDataFrame:
    unknown_libraries_dependency_df = dependency_df.where(
        col(COLUMN_TYPE) == lit(UNKNOWN_LIBRARIES_ARTIFACT_DEPENDENCY_TYPE_KEY),
    ).sort(col(COLUMN_STATUS_DETAIL))

    unknown_libraries_dependency_detailed_df = unknown_libraries_dependency_df.select(
        col(COLUMN_FILE_ID).alias(FRIENDLY_NAME_CODE_FILE_PATH),
        col(COLUMN_DEPENDENCY).alias(FRIENDLY_NAME_DEPENDENCY),
        when(
            col(COLUMN_STATUS_DETAIL) == lit(UNKNOWN_LIBRARIES_SUPPORTED_STATUS_KEY),
            concat(lit(DEPENDENCY_ARTIFACT_GREEN_ICON + " "), col(COLUMN_STATUS_DETAIL)),
        )
        .otherwise(concat(lit(DEPENDENCY_ARTIFACT_RED_ICON + " "), col(COLUMN_STATUS_DETAIL)))
        .alias(COLUMN_UNKNOWN_LIBRARIES_STATUS_FRIENDLY_NAME),
    )

    return unknown_libraries_dependency_detailed_df


def get_sql_object_detailed(dependency_df: SnowparkDataFrame) -> SnowparkDataFrame:
    sql_object_dependency_df = dependency_df.where(
        col(COLUMN_TYPE) == lit(SQL_OBJECT_ARTIFACT_DEPENDENCY_TYPE_KEY),
    ).sort(col(COLUMN_STATUS_DETAIL))

    sql_object_dependency_detailed_df = sql_object_dependency_df.select(
        col(COLUMN_FILE_ID),
        parse_json(col(COLUMN_ARGUMENTS)).alias(COLUMN_ARGUMENTS),
        col(COLUMN_DEPENDENCY),
        col(COLUMN_STATUS_DETAIL),
    ).select(
        col(COLUMN_FILE_ID).alias(FRIENDLY_NAME_CODE_FILE_PATH),
        col(COLUMN_DEPENDENCY).alias(COLUMN_SQL_LIBRARY_OBJECT_FRIENDLY_NAME),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_CATEGORY_KEY], double_quote, empty_string).alias(
            COLUMN_SQL_OBJECT_CATEGORY_FRIENDLY_NAME,
        ),
        replace(col(COLUMN_ARGUMENTS)[DEPENDENCY_ARGUMENT_DIALECT_KEY], double_quote, empty_string).alias(
            COLUMN_SQL_OBJECT_DIALECT_FRIENDLY_NAME,
        ),
        when(col(COLUMN_STATUS_DETAIL) == lit(SQL_OBJECT_EXISTS_STATUS_KEY), LABEL_EXISTS)
        .otherwise(LABEL_NOT_EXISTS)
        .alias(COLUMN_SQL_LIBRARY_STATUS_FRIENDLY_NAME),
    )

    return sql_object_dependency_detailed_df


def get_total_green_dependencies(dependency_df: SnowparkDataFrame) -> int:
    total_green_df = dependency_df.where(
        col(COLUMN_STATUS_DETAIL).isin(ARTIFACT_DEPENDENCY_SUPPORTED_STATUS_COLLECTION),
    ).select(count(col(COLUMN_STATUS_DETAIL)))
    return get_count_from_df(total_green_df)


def get_total_red_dependencies(dependency_df: SnowparkDataFrame) -> int:
    total_red_df = dependency_df.where(
        col(COLUMN_STATUS_DETAIL).isin(ARTIFACT_DEPENDENCY_NOT_SUPPORTED_STATUS_COLLECTION),
    ).select(count(col(COLUMN_STATUS_DETAIL)))
    return get_count_from_df(total_red_df)


def get_count_green_dependency_by_type(dependency_df: SnowparkDataFrame, dependency_type: str) -> int:
    total_green_df = (
        dependency_df.where(col(COLUMN_TYPE) == lit(dependency_type))
        .where(col(COLUMN_STATUS_DETAIL).isin(ARTIFACT_DEPENDENCY_SUPPORTED_STATUS_COLLECTION))
        .select(count(col(COLUMN_STATUS_DETAIL)))
    )
    return get_count_from_df(total_green_df)


def get_count_red_dependency_by_type(dependency_df: SnowparkDataFrame, dependency_type: str) -> int:
    total_red_df = (
        dependency_df.where(col(COLUMN_TYPE) == lit(dependency_type))
        .where(col(COLUMN_STATUS_DETAIL).isin(ARTIFACT_DEPENDENCY_NOT_SUPPORTED_STATUS_COLLECTION))
        .select(count(col(COLUMN_STATUS_DETAIL)))
    )
    return get_count_from_df(total_red_df)


def get_count_from_df(df: SnowparkDataFrame) -> int:
    df_value = df.collect()
    if df_value:
        return df_value[0][0]
    return 0


def get_islands_summary(islands: list, execution_id: str, selected: bool = False) -> pandas.DataFrame:
    summary_data = []
    session = utils.get_session()
    for i, island_nodes in enumerate(islands):
        data = get_artifacts_dependency_table_data([execution_id])
        dataWithFailure = data.filter(
            (col(COLUMN_FILE_ID).isin(island_nodes)) & (col(COLUMN_SUCCESS) == lit(False)),
        ).select(
            count(lit(1)),
        )

        failure_count = dataWithFailure.collect()[0][0]

        listfiles = ", ".join(island_nodes)
        summary_data.append(
            {
                COLUMN_ISLAND_ID: i + 1,
                FRIENDLY_NAME_ISLAND_NAME: f"Island {i + 1}",
                FRIENDLY_NAME_TOTAL_CODE_FILES: len(island_nodes),
                FRIENDLY_NAME_FAILURE_COUNT: failure_count,
                FRIENDLY_NAME_FILES_LIST: listfiles,
            },
        )

    df = session.create_dataframe(summary_data)

    df_with_selector = df.withColumn(FRIENDLY_NAME_SELECT, lit(selected))

    df_with_selector = df_with_selector.select(
        FRIENDLY_NAME_SELECT,
        COLUMN_ISLAND_ID,
        FRIENDLY_NAME_ISLAND_NAME,
        FRIENDLY_NAME_TOTAL_CODE_FILES,
        FRIENDLY_NAME_FILES_LIST,
    ).toPandas()
    return df_with_selector


def get_artifacts_summary_table_data(execution_id_list: list, selected_files_list: list = None) -> DataFrame:
    session = utils.get_session()
    artifactsSummaryTable = session.table(TABLE_ARTIFACT_DEPENDENCY_SUMMARY)
    table_data = (
        with_table_quality_handler(artifactsSummaryTable, TABLE_ARTIFACT_DEPENDENCY_SUMMARY)
        .where(
            col(COLUMN_EXECUTION_ID).isin(execution_id_list),
        )
        .with_column(
            COLUMN_FILE_ID,
            normalize_path_column(COLUMN_FILE_ID),
        )
    )

    if selected_files_list is not None and len(selected_files_list) > 0:
        table_data = table_data.where(col(COLUMN_FILE_ID).isin(selected_files_list))

    return table_data


def update_island_column(islands: list, execution_id: str):
    if not islands or len(islands) == 0:
        return

    not_null_df = get_artifacts_summary_table_data([execution_id]).where(col(COLUMN_ISLAND_ID).is_not_null())
    if not_null_df.count() == 0:
        session = utils.get_session()
        artifactsSummaryTable = session.table(TABLE_ARTIFACT_DEPENDENCY_SUMMARY)

        for island_number, island_files in enumerate(islands):
            artifactsSummaryTable.update(
                {
                    COLUMN_ISLAND_ID: when(
                        normalize_path_column(COLUMN_FILE_ID).isin(island_files),
                        lit(island_number + 1),
                    ).otherwise(col(COLUMN_ISLAND_ID)),
                },
                col(COLUMN_EXECUTION_ID) == execution_id,
            )
