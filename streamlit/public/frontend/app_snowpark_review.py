import re
import urllib.parse

import pandas as pd
import plotly.express as px
import streamlit as st

import public.backend.review_executions_backend as backend
import public.frontend.empty_screen as emptyScreen
import public.frontend.error_handling as errorHandling

from public.backend import app_snowpark_utils as utils
from public.backend import files_backend, import_backend, report_url_backend, spark_usages_backend, telemetry
from public.backend.globals import *
from public.backend.utils import TextType, get_font_properties, render_text_with_style
from public.frontend.app_snowpark_dependency_report import dependency_report
from public.frontend.app_snowpark_treemap import buildTreemap


feedbackCreationResponses = []
snowpark_connect_supported = "Snowpark Connect Supported"
snowpark_connect_not_supported = "Snowpark Connect Not Supported"


def createLinkElement(element):
    query = f'project="SCT" and summary ~ "{element}" ORDER BY created DESC'
    query = urllib.parse.quote(query)
    return f"[{element.replace(']', '').replace('[', '')}](https://snowflakecomputing.atlassian.net/jira/software/c/projects/SCT/issues/?jql={query})"


@errorHandling.executeFunctionWithErrorHandling
def generateInventories(executionIds):
    inventory_file(executionIds)
    additional_inventory(executionIds)


def additional_inventory(executionIds):
    render_text_with_style("Additional Inventories", TextType.SUBTITLE)
    st.markdown(
        """The Snowpark Migration Accelerator (SMA) generates multiple inventory files every time the tool is executed. These inventory files are available to the user in the local “Reports” output folder. All of these files are also available by clicking the Generate Additional Inventory Files checkbox below. After selecting the checkbox, a link to each file will be made available. Click on the filename to download it locally.""",
    )
    if st.checkbox(
        "Generate Additional Inventory Files",
        key="generateAdditionalFiles",
    ):
        execid = st.selectbox("Execution", executionIds)
        with st.spinner("Getting SMA output download links..."):
            output_files = backend.get_sma_output_download_urls(execid)
            output_files_with_excel = backend.generate_output_file_table(output_files)
            st.markdown(output_files_with_excel)
        eventAttributes = {EXECUTIONS: executionIds}
        telemetry.logTelemetry(CLICK_GENERATE_ADDITIONAL_INVENTORY, eventAttributes)


def inventory_file(executionIds):
    render_text_with_style("Inventories", TextType.PAGE_TITLE)
    render_text_with_style("Inventory File", TextType.SUBTITLE)
    st.markdown(
        """
                The Inventory File is a list of all files found in this codebase. The number of code lines, comment lines, blank lines, and size (in bytes) is given in this spreadsheet.
                """,
    )
    if st.button(label="Generate Inventory File", key="btnGenerateInventories"):
        df_inventory = files_backend.get_input_files_by_execution_id(executionIds)
        utils.generateExcelFile(
            df_inventory,
            backend.SHEET_INVENTORY,
            "Download Inventory File",
            f"FilesInventory-{utils.getFileNamePrefix(executionIds)}.xlsx",
        )
        eventAttributes = {EXECUTIONS: executionIds}
        telemetry.logTelemetry(CLICK_GENERATE_INVENTORY, eventAttributes)


@errorHandling.executeFunctionWithErrorHandling
def assesmentReport(executionIds):
    render_text_with_style("Assessment Report", TextType.PAGE_TITLE)
    st.markdown("<br/>", unsafe_allow_html=True)
    dfAllExecutions = files_backend.get_input_files_by_execution_id_grouped_by_technology(
        executionIds,
    )
    selectedExecutionId = st.selectbox(
        "Select an Execution ID to generate the report.",
        executionIds,
        key="selectExecId",
    )
    df_filtered_executions = files_backend.get_input_files_by_execution_id_grouped_by_technology(
        [selectedExecutionId],
    )
    df_filtered_executions = utils.reset_index(df_filtered_executions)
    st.dataframe(df_filtered_executions)
    total_files = dfAllExecutions[backend.COLUMN_FILES].sum()

    df_docx = report_url_backend.get_table_report_url([selectedExecutionId])

    if df_docx is None or len(df_docx) == 0 or df_docx[0][COLUMN_RELATIVE_REPORT_PATH] is None:
        st.warning(
            "The detailed report could not be generated. Please email us at sma-support@snowflake.com.",
        )

    if len(df_docx) > 0:
        with st.columns(3)[0]:
            if df_docx[0][COLUMN_RELATIVE_REPORT_PATH] is not None:
                get_report_download_button(df_docx[0][COLUMN_RELATIVE_REPORT_PATH])
    return total_files


@errorHandling.executeFunctionWithErrorHandling
def mappings(execution_ids):
    render_text_with_style("Mappings", TextType.PAGE_TITLE)
    df = spark_usages_backend.get_spark_usages_by_execution_id_grouped_by_status(
        execution_ids,
    )
    df_snowpark_connect = spark_usages_backend.get_sas_usages_by_execution_id_grouped_by_status(execution_ids)
    total_count_value = df[df["STATUS CATEGORY"] == "Total"]["COUNT"].values[0]
    if not df.empty and total_count_value > 0:
        df = utils.reset_index(df)
        box1, box2 = st.columns(2)
        with box1:
            st.title("Snowpark API")
            st.dataframe(df)
        with box2:
            st.title("Snowpark Connect")
            st.dataframe(df_snowpark_connect.style.format({FRIENDLY_NAME_PERCENTAGES: lambda x: f"{x:.2f}%"}))
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info(
                icon="💡",
                body=f"Visit the [documentation]({DOC_URL}) to better understand the workaround comments.",
            )
            category = st.selectbox(
                "Pick a category",
                getUnsuppportedStatusList(),
                format_func=lambda x: re.sub(r"(?<!\bWor)(\w)([A-Z])", r"\1 \2", x),
            )
        if category in (snowpark_connect_supported, snowpark_connect_not_supported):
            df_filtered = spark_usages_backend.get_spark_usages_by_execution_id_filtered_by_spark_connect_status(
                execution_ids,
                category == snowpark_connect_supported,
            )
        else:
            df_filtered = spark_usages_backend.get_spark_usages_by_execution_id_filtered_by_status(
                execution_ids,
                category,
            )
        if df_filtered is not None:
            df_filtered.rename(columns={"TOOLVERSION": "TOOL VERSION"}, inplace=True)
            df_suggestions = utils.paginated(
                df_filtered,
                (backend.color, [backend.COLUMN_SUPPORTED, backend.COLUMN_STATUS], 1),
                key_prefix="review_mappings_table",
                editable=True,
                dropdown_cols=[COLUMN_SUPPORTED, COLUMN_STATUS, COLUMN_IS_SNOWPARK_CONNECT_SUPPORTED],
            )
            st.info(icon="💡", body=f"[Click here to give us feedback about mappings]({MAPPINGS_FEEDBACK_URL})")
            if df_suggestions is not None:
                feedbackCol1, feedbackCol2 = st.columns([0.663, 0.337])
                """
                with feedbackCol2:
                    st.warning("Don't forget to submit your feedback before moving to another page.")
                    
                    if st.button("Submit Feedback", key="submit_feedback_review", help= "Submitting feedback will automatically create a Jira ticket."):
                        createdJiraIds, existingJiraIds = submitMappingsFeedback(dfSuggestions, FRIENDLY_NAME_SPARK_FULLY_QUALIFIED_NAME)
                        with feedbackCol1:
                            for key in existingJiraIds.keys():
                                st.warning(f"There is already an open item for **{key}**, please add your comments [here]({existingJiraIds[key]}).")  
                            for key in createdJiraIds.keys():
                                st.info(f"✅ Item for **{key}** has been created, to add additional comments click [here](https://snowflakecomputing.atlassian.net/browse/{createdJiraIds[key]}).")
                            
                            eventAttributes = {EXECUTIONS : executionIds, JIRAIDS : createdJiraIds}
                            telemetry.logTelemetry(CLICK_SUBMIT_MAPPINGS_FEEDBACK, eventAttributes)
                """
    else:
        st.warning("No mappings found.")


def mappings_aux(df):
    return utils.paginated(
        df,
        (
            backend.color,
            [
                backend.FRIENDLY_NAME_SPARK_FULLY_QUALIFIED_NAME,
                backend.COLUMN_SUPPORTED,
                backend.TOOL_VERSION,
                backend.FRIENDLY_NAME_MAPPING_STATUS,
                backend.FRIENDLY_NAME_MAPPING_COMMENTS,
            ],
            1,
        ),
        key_prefix="mappings_table",
        editable=True,
        dropdown_cols=[
            FRIENDLY_NAME_SPARK_FULLY_QUALIFIED_NAME,
            TOOL_VERSION,
            FRIENDLY_NAME_MAPPING_STATUS,
            FRIENDLY_NAME_MAPPING_COMMENTS,
        ],
    )


@errorHandling.executeFunctionWithErrorHandling
def sparkInfo(df, execution_ids):
    st.text(" ")
    st.text(" ")
    render_text_with_style("Readiness Files Distribution", TextType.PAGE_TITLE)
    with st.columns(2)[0]:
        first_column, second_column, third_column, fourth_column = st.columns(4)
        with first_column:
            st.metric(label="With Spark", value=df.shape[0])
        with second_column:
            st.metric(
                label="Total Lines With Spark",
                value=df[[backend.FRIENDLY_NAME_LINES_OF_CODE]].sum(),
            )
        with third_column:
            avg_readiness = 0 if df.empty else df[[backend.COLUMN_READINESS]].mean().round(2)
            st.metric(label="Average Snowpark API Readiness score", value=avg_readiness)
        with fourth_column:
            avg_readiness = 0 if df.empty else df[[backend.FRIENDLY_NAME_SAS_READINESS]].mean().round(2)
            st.metric(label="Average Snowpark Connect Readiness score", value=avg_readiness)
    df = utils.reset_index(df)
    styled_df = df.style.applymap(
        lambda val: backend.getReadinessBackAndForeColorsStyle(val, is_sas_score=True),
        subset=[backend.FRIENDLY_NAME_SAS_READINESS],
    )
    styled_df = styled_df.applymap(
        lambda val: backend.getReadinessBackAndForeColorsStyle(val, is_sas_score=False),
        subset=[backend.COLUMN_READINESS],
    )
    snowpark_api_chart_column, snowpark_connect_chart_column = st.columns(2)
    with snowpark_api_chart_column:
        render_text_with_style("Snowpark API", TextType.CHART_TITLE)
        display_pie_chart(df, backend.COLUMN_READINESS)
    with snowpark_connect_chart_column:
        render_text_with_style("Snowpark Connect", TextType.CHART_TITLE)
        display_pie_chart(df, backend.FRIENDLY_NAME_SAS_READINESS)

    if st.checkbox("Show data table", key="bcxShowDataTableReadinessByFile"):
        st.dataframe(styled_df)

    if st.button(label="Generate Readiness by File", key="sparkInfo"):
        utils.generateExcelFile(
            styled_df,
            "Readiness",
            "Download Readiness by File",
            f"readiness-{utils.getFileNamePrefix(execution_ids)}.xlsx",
        )
        eventAttributes = {EXECUTIONS: execution_ids}
        telemetry.logTelemetry(CLICK_GENERATE_READINESS_FILE, eventAttributes)


def display_pie_chart(df: pd.DataFrame, readiness_key: str):
    ready_to_migrate_count = len(df[df[readiness_key] >= 80])
    migrate_with_manual_effort = len(df[df[readiness_key].between(60, 80, inclusive="left")])
    additional_info_will_be_required = len(df[df[readiness_key] < 60])

    pieChartData = pd.DataFrame(
        [
            [
                f"{ready_to_migrate_count} {backend.KEY_READY_TO_MIGRATE}",
                ready_to_migrate_count,
            ],
            [
                f"{migrate_with_manual_effort} {backend.KEY_MIGRATE_WITH_MANUAL_EFFORT}",
                migrate_with_manual_effort,
            ],
            [
                f"{additional_info_will_be_required} {backend.KEY_ADDITIONAL_INFO_WILL_BE_REQUIRED}",
                additional_info_will_be_required,
            ],
        ],
        columns=[backend.COLUMN_TITLE, backend.COLUMN_FILES_COUNT],
    )
    fig = px.pie(
        pieChartData,
        values=backend.COLUMN_FILES_COUNT,
        names=backend.COLUMN_TITLE,
        title="",
        color=backend.COLUMN_TITLE,
        color_discrete_map={
            f"{ready_to_migrate_count} {backend.KEY_READY_TO_MIGRATE}": backend.style.SUCCESS_COLOR,
            f"{migrate_with_manual_effort} {backend.KEY_MIGRATE_WITH_MANUAL_EFFORT}": backend.style.WARNING_COLOR,
            f"{additional_info_will_be_required} {backend.KEY_ADDITIONAL_INFO_WILL_BE_REQUIRED}": backend.style.ERROR_COLOR,
        },
    )
    st.plotly_chart(fig, config={"modeBarButtonsToRemove": ["toImage"], "displaylogo": False}, key=readiness_key)


@errorHandling.executeFunctionWithErrorHandling
def readiness_file(execution_ids: list[str]) -> None:
    files_with_spark_usages = files_backend.get_files_with_usages_by_execution_id(
        execution_ids,
    )
    input_files_by_execution_id_and_counted_by_technology = (
        files_backend.get_input_files_by_execution_id_and_counted_by_technology(
            execution_ids,
        )
    )

    input_files_by_execution_id_and_counted_by_technology = utils.reset_index(
        input_files_by_execution_id_and_counted_by_technology,
    )
    render_text_with_style(
        "Readiness by File",
        TextType.PAGE_TITLE,
    )

    chart_style = get_font_properties(TextType.CHART_TITLE)
    fig = px.bar(
        input_files_by_execution_id_and_counted_by_technology,
        text_auto=True,
        y=backend.COLUMN_TECHNOLOGY,
        x=backend.COLUMN_COUNT,
        title="Files Count by Technology",
    )
    fig.update_layout(
        yaxis_title="Technology",
        xaxis_title="Total files count",
        xaxis={"visible": True, "showticklabels": True},
        yaxis={"categoryorder": "total ascending"},
        title_font_size=chart_style.get("font_size"),
        title_font_family=chart_style.get("family"),
    )
    fig.update_traces(textangle=0, textfont_size=14)
    st.plotly_chart(fig, config={"modeBarButtonsToRemove": ["toImage"], "displaylogo": False})

    if st.checkbox("Show data table", key="bcxShowDataTablefilesBytech"):
        st.dataframe(input_files_by_execution_id_and_counted_by_technology)

    sparkInfo(files_with_spark_usages, execution_ids)


@errorHandling.executeFunctionWithErrorHandling
def import_library_dependency(execution_ids):
    df = import_backend.get_import_usages_by_execution_id_and_by_origin(execution_ids, ALL_KEY)
    if df.empty:
        st.warning("No imports found.")
        return
    col1, _, _ = st.columns(3)
    with col1:
        status = st.selectbox("Show", get_import_status())
    if status != ALL_KEY:
        df = df[df[COLUMN_ORIGIN] == status]
    if not df.empty:
        utils.paginated_import_dependency_table(df, "import_dependency_table")
    else:
        st.warning(f"No {status} imports found.")


@errorHandling.executeFunctionWithErrorHandling
def review(execution_ids):
    if execution_ids is None or len(execution_ids) <= 0:
        emptyScreen.show()
    else:
        with st.expander("Inventories"):
            generateInventories(execution_ids)
        with st.expander("Assessment Report"):
            total_files = assesmentReport(execution_ids)
        with st.expander("Code TreeMap"):
            if total_files is not None and total_files > 30:
                st.info(
                    f"This assessment has a total of {total_files} files. This treemap can be used to identify folders were most of the code is grouped.",
                )
                st.plotly_chart(
                    buildTreemap(execution_ids),
                    use_container_width=True,
                    config={"modeBarButtonsToRemove": ["toImage"], "displaylogo": False},
                )
        with st.expander("Mappings"):
            mappings(execution_ids)
        with st.expander("Readiness by File"):
            readiness_file(execution_ids)
        with st.expander("Import Library Dependency Data Table"):
            import_library_dependency(execution_ids)
        dependency_report(execution_ids)


def get_report_download_button(report_path):
    try:
        with utils.get_session().file.get_stream(f"@{SMA_EXECUTIONS_STAGE}/{report_path}") as file:
            st.download_button(
                label="Download Detailed Report",
                data=file,
                file_name="DetailedReport.docx",
            )
    except:
        st.warning("The detailed report could not be generated. Please email us at sma-support@snowflake.com.")
