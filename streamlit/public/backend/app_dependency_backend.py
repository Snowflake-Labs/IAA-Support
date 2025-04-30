
import pandas
import streamlit as st

import public.backend.app_snowpark_utils as utils

from public.backend.globals import *
from public.backend.query_quality_handler import with_table_quality_handler
from snowflake.snowpark.functions import col


@st.cache_data(show_spinner=True)
def get_unique_dependencies_by_execid(execution_ids):
    session = utils.get_session()
    df = with_table_quality_handler(
        session.table(TABLE_COMPUTED_DEPENDENCIES), TABLE_COMPUTED_DEPENDENCIES) \
        .filter(col(COLUMN_TOOL_EXECUTION_ID) \
                .isin(execution_ids))
    df_dependencies = df.select(COLUMN_SOURCE_FILE, COLUMN_DEPENDENCIES,
                                COLUMN_TOOL_EXECUTION_ID).distinct().to_pandas()
    return df_dependencies

def get_java_builtins():
    session = utils.get_session()
    java_builtins_table_data = with_table_quality_handler(session.table(JAVA_BUILTINS), JAVA_BUILTINS)
    return java_builtins_table_data


def get_dependencies_without_java_builtins(execution_ids):
    session = utils.get_session()

    java_builtins_table_data = get_java_builtins()

    dependencies_table_data = with_table_quality_handler(session.table(TABLE_COMPUTED_DEPENDENCIES), TABLE_COMPUTED_DEPENDENCIES) \
                                                                .where(col(COLUMN_TOOL_EXECUTION_ID)
                                                                       .isin(execution_ids))

    dependencies_table_data_java_builtins = dependencies_table_data.join(right = java_builtins_table_data,
                                                                         on = col(COLUMN_IMPORT).startswith(col(COLUMN_NAME)),
                                                                         how = "right") \
                                                                    .drop(COLUMN_NAME)

    dependencies_without_java_builtins = dependencies_table_data.minus(dependencies_table_data_java_builtins)

    return dependencies_without_java_builtins

def get_dependencies(execution_ids):
    dependencies_data = get_dependencies_without_java_builtins(execution_ids)
    dependencies_data_with_friendly_name = dependencies_data.withColumnRenamed(COLUMN_TOOL_EXECUTION_ID, FRIENDLY_NAME_EXECUTION_ID) \
                                                            .withColumnRenamed(COLUMN_SOURCE_FILE, FRIENDLY_NAME_SOURCE_FILE)
    dependencies = dependencies_data_with_friendly_name.to_pandas()
    dependencies[COLUMN_IS_BUILTIN] = dependencies[COLUMN_ORIGIN] == "BuiltIn"
    dependencies[COLUMN_IS_BUILTIN] = dependencies[COLUMN_IS_BUILTIN].astype(str)
    dependencies[COLUMN_IS_BUILTIN] = dependencies[COLUMN_IS_BUILTIN].str.lower()

    return dependencies


def get_grouped_dependencies(df_dependencies):
    df_dependencies_filtered = df_dependencies[df_dependencies[COLUMN_DEPENDENCIES].notnull()]
    result = (
        df_dependencies_filtered.groupby(FRIENDLY_NAME_SOURCE_FILE).agg(
            DEPENDENCIES_COUNT=(COLUMN_DEPENDENCIES, "count"),
            UNKNOWN_DEPENDENCIES_COUNT=(COLUMN_ORIGIN, lambda x: (x == "Unknown").sum()),
        )
    )
    result["PERCENTAGE OF UNKNOWN"] = (
            (result["UNKNOWN_DEPENDENCIES_COUNT"] * 100) / result["DEPENDENCIES_COUNT"]
    ).round(2).apply(lambda x: "0%" if x == 0 else f"{x:.2f}%")

    result = result.reset_index().rename(columns={
        FRIENDLY_NAME_SOURCE_FILE: "SOURCE FILE",
        COLUMN_DEPENDECIES_COUNT: "DEPENDENCIES COUNT",
        "UNKNOWN_DEPENDENCIES_COUNT": "UNKNOWN DEPENDENCIES",
    })

    return result

def get_pending_execution_list(execution_ids: list[str]) -> pandas.DataFrame:
    session = utils.get_session()
    pending_execution_list = session.table(TABLE_PENDING_EXECUTION_LIST) \
                               .where(col(COLUMN_EXECUTION_ID).isin(execution_ids))

    return pending_execution_list.to_pandas()
