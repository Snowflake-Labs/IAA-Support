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
from public.frontend.app_snowpark_dependency_report import dependency_report
from public.frontend.app_snowpark_treemap import buildTreemap


feedbackCreationResponses = []


def createLinkElement(element):
    query = f'project="SCT" and summary ~ "{element}" ORDER BY created DESC'
    query = urllib.parse.quote(query)
    return f'[{element.replace("]", "").replace("[", "")}](https://snowflakecomputing.atlassian.net/jira/software/c/projects/SCT/issues/?jql={query})'


@errorHandling.executeFunctionWithErrorHandling
def generateInventories(executionIds):
    inventory_file(executionIds)
    additional_inventory(executionIds)


def additional_inventory(executionIds):
    title_additional_inventory = '<strong style="font-size: 20px;">Additional Inventories</strong>'
    st.markdown(title_additional_inventory, unsafe_allow_html=True)
    st.markdown(
        """The Snowpark Migration Accelerator (SMA) generates multiple inventory files every time the tool is executed. These inventory files are available to the user in the local “Reports” output folder. All of these files are also available by clicking the Generate Additional Inventory Files checkbox below. After selecting the checkbox, a link to each file will be made available. Click on the filename to download it locally.""",
    )
    if st.checkbox(
            "Generate Additional Inventory Files", key="generateAdditionalFiles",
    ):
        execid = st.selectbox("Execution", executionIds)
        with st.spinner("Getting SMA output download links..."):
            output_files = backend.get_sma_output_download_urls(execid)
            output_files_with_excel = backend.generate_output_file_table(output_files)
            st.markdown(output_files_with_excel)
        eventAttributes = {EXECUTIONS: executionIds}
        telemetry.logTelemetry(CLICK_GENERATE_ADDITIONAL_INVENTORY, eventAttributes)


def inventory_file(executionIds):
    title_section = '<strong style="font-size: 24px;">Inventories</strong>'
    st.markdown(title_section, unsafe_allow_html=True)
    title_inventory_file = '<strong style="font-size: 20px;">Inventory File</strong>'
    st.markdown(title_inventory_file, unsafe_allow_html=True)
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
    title_section = '<strong style="font-size: 24px;">Assessment Report</strong>'
    st.markdown(title_section, unsafe_allow_html=True)
    st.markdown("<br/>", unsafe_allow_html=True)
    dfAllExecutions = (
        files_backend.get_input_files_by_execution_id_grouped_by_technology(
            executionIds,
        )
    )
    selectedExecutionId = st.selectbox(
        "Select an Execution ID to generate the report.",
        executionIds,
        key="selectExecId",
    )
    df_filtered_executions = (
        files_backend.get_input_files_by_execution_id_grouped_by_technology(
            [selectedExecutionId],
        )
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
            if (
                df_docx[0][COLUMN_RELATIVE_REPORT_PATH]
                is not None
            ):
                get_report_download_button(df_docx[0][COLUMN_RELATIVE_REPORT_PATH])
    return total_files


@errorHandling.executeFunctionWithErrorHandling
def mappings(execution_ids):
    title_section = '<strong style="font-size: 24px;">Mappings</strong>'
    st.markdown(title_section, unsafe_allow_html=True)
    st.markdown("<br/>", unsafe_allow_html=True)
    df = spark_usages_backend.get_spark_usages_by_execution_id_grouped_by_status(
        execution_ids,
    )
    total_count_value = df[df["STATUS CATEGORY"] == "Total"]["COUNT"].values[0]
    if not df.empty and total_count_value > 0:
        df = utils.reset_index(df)
        st.dataframe(df)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info(
                icon="💡",
                body=f"Visit the [documentation]({DOC_URL}) to better understand the workaround comments.",
            )
            category = st.selectbox("Pick a category", getUnsuppportedStatusList(),
                                    format_func=lambda x: re.sub(r"(?<!\bWor)(\w)([A-Z])", r"\1 \2", x))

        df_filtered = (
            spark_usages_backend.get_spark_usages_by_execution_id_filtered_by_status(
                execution_ids, category,
            )
        )
        if df_filtered is not None:
            df_filtered.rename(columns={"TOOLVERSION": "TOOL VERSION"}, inplace=True)
            df_suggestions = utils.paginated(
                df_filtered,
                (backend.color, [backend.COLUMN_SUPPORTED, backend.COLUMN_STATUS], 1),
                key_prefix="review_mappings_table",
                editable=True,
                dropdown_cols=[COLUMN_SUPPORTED, COLUMN_STATUS],
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
def sparkInfo(df, executionIds, title_font_size=20):
    st.text(" ")
    st.text(" ")
    readiness_files_title = f'<strong style="font-size: {title_font_size}px;">Readiness Files Distribution</strong>'
    st.markdown(readiness_files_title, unsafe_allow_html=True)
    with st.columns(2)[0]:
        columnLeft, columnCenter, columnRight = st.columns(3)
        with columnLeft:
            st.metric(label="With Spark", value=df.shape[0])
        with columnCenter:
            st.metric(
                label="Total Lines With Spark",
                value=df[[backend.FRIENDLY_NAME_LINES_OF_CODE]].sum(),
            )
        with columnRight:
            avg_readiness = (
                0 if df.empty else df[[backend.COLUMN_READINESS]].mean().round(2)
            )
            st.metric(label="Average Readiness", value=avg_readiness)
    df = utils.reset_index(df)
    styled_df = df.style.applymap(
        backend.getReadinessBackAndForeColorsStyle, subset=[backend.COLUMN_READINESS],
    )

    readyToMigrateCount = len(df[df[backend.COLUMN_READINESS] >= 80])
    migrateWithManualEffort = len(df[df[backend.COLUMN_READINESS].between(60, 80, inclusive="left")])
    additionalInfoWillBeRequired = len(df[df[backend.COLUMN_READINESS] < 60])

    pieChartData = pd.DataFrame(
        [
            [
                f"{readyToMigrateCount} {backend.KEY_READY_TO_MIGRATE}",
                readyToMigrateCount,
            ],
            [
                f"{migrateWithManualEffort} {backend.KEY_MIGRATE_WITH_MANUAL_EFFORT}",
                migrateWithManualEffort,
            ],
            [
                f"{additionalInfoWillBeRequired} {backend.KEY_ADDITIONAL_INFO_WILL_BE_REQUIRED}",
                additionalInfoWillBeRequired,
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
            f"{readyToMigrateCount} {backend.KEY_READY_TO_MIGRATE}": backend.style.SUCCESS_COLOR,
            f"{migrateWithManualEffort} {backend.KEY_MIGRATE_WITH_MANUAL_EFFORT}": backend.style.WARNING_COLOR,
            f"{additionalInfoWillBeRequired} {backend.KEY_ADDITIONAL_INFO_WILL_BE_REQUIRED}": backend.style.ERROR_COLOR,
        },
    )
    st.plotly_chart(fig, config={"modeBarButtonsToRemove": ["toImage"], "displaylogo": False})

    if st.checkbox("Show data table", key="bcxShowDataTableReadinessByFile"):
        st.dataframe(styled_df)

    if st.button(label="Generate Readiness by File", key="sparkInfo"):
        utils.generateExcelFile(
            styled_df,
            backend.COLUMN_READINESS,
            "Download Readiness by File",
            f"readiness-{utils.getFileNamePrefix(executionIds)}.xlsx",
        )
        eventAttributes = {EXECUTIONS: executionIds}
        telemetry.logTelemetry(CLICK_GENERATE_READINESS_FILE, eventAttributes)


@errorHandling.executeFunctionWithErrorHandling
def readinessFile(executionIds, title_font_size=20):
    files_with_spark_usages = files_backend.get_files_with_spark_usages_by_execution_id(
        executionIds,
    )
    input_files_by_execution_id_and_counted_by_technology = (
        files_backend.get_input_files_by_execution_id_and_counted_by_technology(
            executionIds,
        )
    )

    input_files_by_execution_id_and_counted_by_technology = utils.reset_index(input_files_by_execution_id_and_counted_by_technology)
    readiness_by_file_title_section = f'<strong style="font-size: {title_font_size + 4}px;">Readiness by File</strong>'
    st.markdown(readiness_by_file_title_section, unsafe_allow_html=True)

    files_count_title = f'<strong style="font-size: {title_font_size}px;">Files Count by Technology</strong>'
    st.markdown(files_count_title, unsafe_allow_html=True)

    fig = px.bar(
        input_files_by_execution_id_and_counted_by_technology,
        text_auto=True,
        y=backend.COLUMN_TECHNOLOGY,
        x=backend.COLUMN_COUNT,
    )
    fig.update_layout(
        yaxis_title="Technology",
        xaxis_title="Total files count",
        xaxis={"visible": True, "showticklabels": True},
        yaxis={"categoryorder": "total ascending"},
    )
    fig.update_traces(textangle=0, textfont_size=14)
    st.plotly_chart(fig, config={"modeBarButtonsToRemove": ["toImage"], "displaylogo": False})

    if st.checkbox("Show data table", key="bcxShowDataTablefilesBytech"):
        st.dataframe(input_files_by_execution_id_and_counted_by_technology)

    sparkInfo(files_with_spark_usages, executionIds, title_font_size=title_font_size)


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
                st.plotly_chart(buildTreemap(execution_ids), use_container_width=True, config={"modeBarButtonsToRemove": ["toImage"], "displaylogo": False})
        with st.expander("Mappings"):
            mappings(execution_ids)
        with st.expander("Readiness by File"):
            readinessFile(execution_ids, title_font_size=16)
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
        st.warning(
            "The detailed report could not be generated. Please email us at sma-support@snowflake.com.")
