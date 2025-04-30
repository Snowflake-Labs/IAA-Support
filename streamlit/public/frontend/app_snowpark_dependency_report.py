
import pandas as pd
import streamlit as st

import public.backend.app_dependency_backend as backend
import public.backend.app_snowpark_utils as utils
import public.backend.app_style_values as style
import public.frontend.empty_screen as emptyScreen
import public.frontend.error_handling as errorHandling

from public.backend import telemetry
from public.backend.globals import *
from public.frontend.app_dependency_analysis import dependency_analysis_single2


def color_and_format(x):
    supported = str(x[COLUMN_SUPPORTED]).lower()
    isbuiltin = str(x[COLUMN_IS_BUILTIN]).lower()
    dependencies = str(x[COLUMN_DEPENDENCIES]).strip()
    if supported == "true" or isbuiltin == "true":
        color = style.SUCCESS_COLOR
    else:
        color = style.ERROR_COLOR
    if dependencies.startswith("[") and len(dependencies)>3:
        color = style.SUCCESS_COLOR
    return  f"{style.getBackgroundColorProperty(color)}; {style.getForegroundColorProperty(style.WHITE_COLOR)}"

def generateDependencyReport(df_dependencies):
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter",engine_kwargs={"options": {"in_memory": True}}) as writer:
        missing_builtin_list = ["json","logging","sys","math","xml","time"]
        df_dependencies[COLUMN_IS_BUILTIN] = df_dependencies.apply(lambda x: "true" if (x[COLUMN_IMPORT] in missing_builtin_list) else x[COLUMN_IS_BUILTIN], axis=1)
        df_dependencies_styled = df_dependencies.style.apply(lambda x: [color_and_format(x) for i in x], axis=1, subset=[COLUMN_IMPORT,COLUMN_DEPENDENCIES,COLUMN_SUPPORTED,COLUMN_IS_BUILTIN])
        df_dependencies_styled.to_excel(writer, sheet_name=SHEET_DEPENDENCIES,index=False)
    return output


def check_pending_list(execution_ids: list[str]):
    df_pending_list = backend.get_pending_execution_list(execution_ids)
    if df_pending_list.shape[0] > 0:
        for pending in df_pending_list.itertuples(index=False):
            st.warning(f"The dependencies for execution {pending.EXECUTION_ID} could not be calculated.\nPlease contact support at sma-support@snowflake.com.")


@errorHandling.executeFunctionWithErrorHandling
def dependency_report(execution_ids):
    if execution_ids is None or len(execution_ids) <= 0:
        emptyScreen.show()
    else:
        check_pending_list(execution_ids)
        df_dependencies = backend.get_dependencies(execution_ids)
        df_dependencies[COLUMN_DEPENDENCIES] = df_dependencies[COLUMN_DEPENDENCIES].apply(lambda x: None if x == "[]" else x)
        if df_dependencies.shape[0] > 0:
            df_dependencies[FRIENDLY_NAME_SOURCE_FILE] = df_dependencies[FRIENDLY_NAME_SOURCE_FILE]
            with (st.expander("File Dependency Data Table")):
                top = backend.get_grouped_dependencies(df_dependencies)
                if not top.empty:
                    utils.paginated_import_dependency_table(top, "dependency_table")
                else:
                    st.warning("No dependencies were found.")
                if "selected_option" not in st.session_state:
                    button_dependencies(df_dependencies, execution_ids)
            with st.expander("Dependencies Graph"):
                has_dependencies = _has_dependencies(df_dependencies)
                if not has_dependencies:
                    st.warning("No dependencies were found.")
                else:
                    st.info("""
        Choose a file from the dropdown menu below. This will show all the dependencies with that file as the root node. You will be able to view other files that are dependent on selected file.
        """)
                    dependency_graph_figure, df_dependencies_by_file = dependency_analysis_single2(execution_ids)
                    config = {
                    "staticPlot": True,
                    "modeBarButtonsToRemove": ["toImage"],
                    "displaylogo": False,
                    }
                    st.plotly_chart(dependency_graph_figure, config=config)
                    df_dependencies_by_file = utils.reset_index(df_dependencies_by_file)
                    plot_dependencies_table_by_file(df_dependencies_by_file.style.apply(_highlight_rows, axis=1))
                    utils.color_legend_import()
        else:
            st.info("Dependencies have not been calculated, please try later.")

        if "selected_option" in st.session_state:
            button_dependencies(df_dependencies, execution_ids)

def button_dependencies(df_dependencies, execution_ids):
    if st.button("Generate Dependencies Report", key="generate_dependencies"):
        missing_builtin_list = ["json", "logging", "sys", "math", "xml", "time"]
        df_dependencies[COLUMN_IS_BUILTIN] = df_dependencies.apply(
            lambda x: "true" if (x[COLUMN_IMPORT] in missing_builtin_list) else x[COLUMN_IS_BUILTIN], axis=1)
        df_dependencies_styled = df_dependencies.style.apply(lambda x: [color_and_format(x) for i in x], axis=1,
                                                             subset=[COLUMN_IMPORT, COLUMN_DEPENDENCIES,
                                                                     COLUMN_SUPPORTED, COLUMN_IS_BUILTIN])
        utils.generateExcelFile(df_dependencies_styled, "inventory", "Download dependencies",
                                f"dependencies-{utils.getFileNamePrefix(execution_ids)}.xlsx")
        event_attributes = {EXECUTIONS: execution_ids}
        telemetry.logTelemetry(CLICK_GENERATE_DEPENDENCIES_REPORT, event_attributes)

def plot_dependencies_table_by_file(df):
    st.dataframe(df, use_container_width=True)

def _highlight_rows(row):
    if row["AVAILABLE IN SNOWPARK"] == "true":
        return [style.getBackgroundColorProperty(style.LIGHT_GREEN_COLOR)]*len(row)
    if row["BUILT-IN"] == "true":
        return [style.getBackgroundColorProperty(style.LIGHT_GRAY_COLOR)]*len(row)
    if row["INTERNAL DEPENDENCIES"] != "":
        return [style.getBackgroundColorProperty(style.LIGHT_BLUE_COLOR)]*len(row)
    return [style.getBackgroundColorProperty(style.LIGHT_RED_COLOR)]*len(row)

def _has_dependencies(df_dependencies):
    return df_dependencies.dropna(subset=[COLUMN_DEPENDENCIES]).shape[0] > 0
