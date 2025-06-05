from public.backend import app_snowpark_utils as utils
from public.backend import tables_backend
from public.backend.globals import *
from snowflake.snowpark.functions import col, lit, lower


def get_last_two_weeks_executions():
    last_two_weeks_executions_table_data = _get_last_two_weeks_executions_table()
    selected_executions_dataframe = generate_selected_executions_dataframe(
        last_two_weeks_executions_table_data,
    )
    return selected_executions_dataframe.toPandas()


def _get_last_two_weeks_executions_table():
    amount_of_days_for_filtering_column = lit(
        utils.getDateWithDelta(AMOUNT_OF_DAYS_FOR_FILTERING),
    )
    executions_table_data = tables_backend.get_execution_info_table_data()
    last_two_weeks_executions_table_data = executions_table_data.where(
        col(COLUMN_EXECUTION_TIMESTAMP) > amount_of_days_for_filtering_column,
    )
    return last_two_weeks_executions_table_data


def get_all_executions_by_email(email):
    email_pattern = f"%{email}%"
    executions_table_data = tables_backend.get_execution_info_table_data()
    match_email_executions_table_data = executions_table_data.where(
        lower(col(COLUMN_CLIENT_EMAIL)).like(email_pattern),
    )
    selected_executions_dataframe = generate_selected_executions_dataframe(
        match_email_executions_table_data,
    )
    return selected_executions_dataframe.toPandas()


def get_execution_by_id(execution_id):
    executions_table_data = tables_backend.get_execution_info_table_data()
    execution_id_table_data = executions_table_data.where(
        col(COLUMN_EXECUTION_ID) == execution_id,
    )
    selected_executions_dataframe = generate_selected_executions_dataframe(
        execution_id_table_data,
        True,
    )
    return selected_executions_dataframe.toPandas()


def get_execution_by_company_or_project_name_or_email(input_value):
    input_pattern = f"%{input_value}%"
    executions_table_data = tables_backend.get_execution_info_table_data()
    match_executions_table_data = executions_table_data.where(
        lower(col(COLUMN_COMPANY)).like(input_pattern)
        | lower(col(COLUMN_CLIENT_EMAIL)).like(input_pattern)
        | lower(col(COLUMN_PROJECT_NAME)).like(input_pattern),
    )
    selected_executions_dataframe = generate_selected_executions_dataframe(
        match_executions_table_data,
    )
    return selected_executions_dataframe.toPandas()


def generate_selected_executions_dataframe(executions_dataframe, selected=False):
    if executions_dataframe is None or executions_dataframe.count() == 0:
        return executions_dataframe

    execution_dataframe_with_selector = executions_dataframe.withColumn(
        FRIENDLY_NAME_SELECT,
        lit(selected),
    )
    execution_id_list = get_execution_id_column_as_array(
        executions_dataframe.select(COLUMN_EXECUTION_ID),
    )
    lines_of_codes_and_total_code_files = tables_backend.get_lines_of_code_total_code_files_and_project_id(
        execution_id_list,
    )
    selected_executions = execution_dataframe_with_selector.join(
        lines_of_codes_and_total_code_files,
        COLUMN_EXECUTION_ID,
    )
    selected_executions_friendly_name = (
        selected_executions.select(
            FRIENDLY_NAME_SELECT,
            COLUMN_EXECUTION_ID,
            COLUMN_EXECUTION_TIMESTAMP,
            COLUMN_PROJECT_NAME,
            COLUMN_PROJECT_ID,
            COLUMN_CLIENT_EMAIL,
            COLUMN_COMPANY,
            COLUMN_TOOL_NAME,
            COLUMN_TOTAL_LINES_OF_CODE,
            COLUMN_TOTAL_CODE_FILES,
            COLUMN_SPARK_API_READINESS_SCORE,
        )
        .withColumnRenamed(COLUMN_EXECUTION_ID, FRIENDLY_NAME_EXECUTION_ID)
        .withColumnRenamed(
            COLUMN_EXECUTION_TIMESTAMP,
            FRIENDLY_NAME_EXECUTION_TIMESTAMP,
        )
        .withColumnRenamed(COLUMN_PROJECT_NAME, FRIENDLY_NAME_PROJECT_NAME)
        .withColumnRenamed(COLUMN_PROJECT_ID, FRIENDLY_NAME_PROJECT_ID)
        .withColumnRenamed(COLUMN_CLIENT_EMAIL, FRIENDLY_NAME_CLIENT_EMAIL)
        .withColumnRenamed(COLUMN_TOOL_NAME, FRIENDLY_NAME_TOOL_NAME)
        .withColumnRenamed(COLUMN_TOTAL_LINES_OF_CODE, FRIENDLY_NAME_LINES_OF_CODE)
        .withColumnRenamed(COLUMN_TOTAL_CODE_FILES, FRIENDLY_NAME_TOTAL_CODE_FILES)
        .withColumnRenamed(
            COLUMN_SPARK_API_READINESS_SCORE,
            FRIENDLY_NAME_READINESS_SCORE,
        )
    )
    return selected_executions_friendly_name


def get_execution_id_column_as_array(df_with_execution_id):
    execution_id_list = [row[COLUMN_EXECUTION_ID] for row in df_with_execution_id.collect()]
    return execution_id_list


def get_file_companies_data(execution_id_list):
    executions_table_data = tables_backend.get_execution_info_table_data_by_execution_id(execution_id_list)
    executions_company = executions_table_data.select(COLUMN_COMPANY).distinct().toPandas()
    projects_data = executions_company.replace("", None).replace(" ", None)
    projects_data = projects_data[projects_data[COLUMN_COMPANY].notna()]
    if projects_data.empty == True:
        return ""
    return "_".join(projects_data[COLUMN_COMPANY])[:50]
