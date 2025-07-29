import plotly.express as px
import streamlit as st

import public.backend.app_style_values as style
import public.backend.review_executions_backend as backend
import public.frontend.app_snowpark_review as rw
import public.frontend.search_bar as search_bar_component

from public.backend import files_backend, summary_metrics_backend
from public.backend.color_bar import *
from public.backend.globals import *
from public.frontend import empty_screen, error_handling
from public.frontend.app_artifact_dependency_review import artifact_dependency_review
from public.frontend.app_dbx_elements_review import dbx_elements_review
from public.frontend.app_snowpark_dependency_report import dependency_report
from public.frontend.app_snowpark_reader_writers import review_readers_writers
from public.frontend.app_snowpark_thirdparty import third_party_review
from public.frontend.shared_components import load_header


@error_handling.executeFunctionWithErrorHandling
def execution_metrics(execution_ids):
    with st.spinner("General metrics..."):
        st.text(" ")
        st.text(" ")
        st.subheader("Summary Metrics")
        st.text(" ")
        st.text(" ")

        last_execution_metric = summary_metrics_backend.get_date_last_execution(
            execution_ids,
        )
        execution_count_metric = summary_metrics_backend.get_executions_selected_count(
            execution_ids,
        )

        last_execution, unique_execution_count = st.columns(2, gap="large")

        with last_execution:
            st.metric("Last Execution Date", last_execution_metric)
        with unique_execution_count:
            st.metric("Selected Executions", execution_count_metric)
        st.text(" ")
        st.text(" ")
        col1, col2, col3 = st.columns(3, gap="large")

        with col1:
            st.markdown("Spark API Readiness Score")
            score = summary_metrics_backend.get_readiness_score(execution_ids)
            bar_color = get_bar_color(score)
            icon = get_icon(score)
            st.markdown(
                custom_progress_bar(score, bar_color, icon),
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown("Third Party API Readiness Score")
            score = summary_metrics_backend.get_third_party_readiness_score(
                execution_ids,
            )
            bar_color = get_bar_color(score)
            icon = get_icon(score)
            st.markdown(
                custom_progress_bar(score, bar_color, icon),
                unsafe_allow_html=True,
            )
        with col3:
            score = summary_metrics_backend.get_sql_readiness_score(execution_ids)
            st.markdown("SQL Readiness Score")
            bar_color = get_bar_color(score)
            icon = get_icon(score)
            st.markdown(
                custom_progress_bar(score, bar_color, icon),
                unsafe_allow_html=True,
            )

        df_loc_technology = summary_metrics_backend.get_lines_of_code_count_by_technology(execution_ids)
        df_files_technology = summary_metrics_backend.get_files_count_by_technology(
            execution_ids,
        )

        loc_technology, files_technology = st.columns(2)

        with loc_technology:
            fig = px.bar(
                df_loc_technology,
                x=COLUMN_LINES_OF_CODE,
                y=COLUMN_TECHNOLOGY,
                text_auto=".3s",
                title="Total Lines of Code by Technology",
                orientation="h",
                width=500,
                height=500,
            )
            fig.update_layout(
                xaxis_title="",
                yaxis_title="",
                yaxis={"categoryorder": "total ascending"},
            )
            fig.update_traces(textangle=0, textfont_size=14)
            fig.update_traces(textposition="outside")

            st.plotly_chart(
                fig,
                use_container_width=True,
                config={"modeBarButtonsToRemove": ["toImage"], "displaylogo": False},
            )

        with files_technology:
            fig = px.bar(
                df_files_technology,
                x=COLUMN_TOTAL_CODE_FILES,
                y=COLUMN_TECHNOLOGY,
                text_auto=".3s",
                title="Total Files by Technology",
                orientation="h",
                width=500,
                height=500,
            )
            fig.update_layout(
                xaxis_title="",
                yaxis_title="",
                yaxis={"categoryorder": "total ascending"},
            )
            fig.update_traces(textangle=0, textfont_size=14)
            fig.update_traces(textposition="outside")
            st.plotly_chart(
                fig,
                use_container_width=True,
                config={"modeBarButtonsToRemove": ["toImage"], "displaylogo": False},
            )


def select_execution():
    load_header()
    st.text(" ")
    st.text(" ")
    if st.session_state["sidebar_option"] == "Execution":
        st.markdown("""### Select an Execution""")
        st.text(" ")

        url = "https://github.com/Snowflake-Labs/IAA-Support/blob/main/docs/UserGuide.md"
        st.markdown(
            "Execution data will only be available for assessment data that has been loaded into this Snowflake account. Instructions for how to upload this are available on the [Interactive Assessment Application (IAA) documentation](%s)"
            % url,
        )
        st.text(" ")
        st.session_state["found_executions"] = search_bar_component.show()
    else:
        is_expanded = not bool(st.session_state.get("found_executions"))
        with st.expander("Execution(s) selected", expanded=is_expanded):
            st.session_state["found_executions"] = search_bar_component.show(False)
        st.markdown("<hr style='border:1px solid #0000001F'>", unsafe_allow_html=True)


def _select_sidebar_option(option):
    st.session_state["sidebar_option"] = option
    st.session_state.selected_option = option


def create_side_bar():
    options = [
        ("Execution", _select_sidebar_option),
        ("Inventories", _select_sidebar_option),
        ("Assessment Report", _select_sidebar_option),
        ("Code Tree Map", _select_sidebar_option),
        ("Mappings", _select_sidebar_option),
        ("Readiness by File", _select_sidebar_option),
        ("Reader Writers", _select_sidebar_option),
        ("Third Party", _select_sidebar_option),
        ("Dependencies", _select_sidebar_option),
        ("Artifacts Dependencies", _select_sidebar_option),
        ("Dbx Elements", _select_sidebar_option),
    ]
    st.sidebar.button(
        "Home",
        on_click=_home_landing_page,
        use_container_width=True,
        key="home_btn",
    )
    st.sidebar.markdown(
        "<h3 style='text-align: center;'>Explore my executions</h3>",
        unsafe_allow_html=True,
    )

    if "selected_option" not in st.session_state:
        st.session_state.selected_option = "Execution"

    for label, action in options:
        if st.session_state.selected_option == label:
            st.sidebar.markdown(
                f"<button style='background-color: #E4F5FF; border: solid 1px #0068C9; padding: 5.5px; width: 100%; border-radius: 7px; outline: none;"
                f"' onclick=\"window.location.href='#'\">{label}</button>",
                unsafe_allow_html=True,
            )
        else:
            st.sidebar.button(
                label,
                on_click=action,
                args=(label,),
                use_container_width=True,
                key=f"{label.lower()}_btn",
            )

    if "sidebar_option" not in st.session_state:
        st.session_state["sidebar_option"] = "Execution"

    st.markdown(
        f"""
            <style>
                [data-testid=stSidebar] {{
                    background-color: {style.SIDEBAR_BG_GRAY_COLOR};
                }}
            </style>
            """,
        unsafe_allow_html=True,
    )


def _home_landing_page():
    st.session_state["Page"] = "Public"
    st.session_state.selected_option = "Execution"


def _dependencies_reports(found_executions):
    title_section = '<strong style="font-size: 24px;">Dependencies</strong>'
    st.markdown(title_section, unsafe_allow_html=True)
    st.markdown("<br/>", unsafe_allow_html=True)
    with st.expander("Import Library Dependency Data Table"):
        rw.import_library_dependency(found_executions)
    dependency_report(found_executions)


def _artifact_dependencies_reports(found_executions):
    title_section = '<strong style="font-size: 24px;">Artifacts Dependencies</strong>'
    st.markdown(title_section, unsafe_allow_html=True)
    st.markdown("<br/>", unsafe_allow_html=True)
    artifact_dependency_review(found_executions)


def _dbx_elements_reports(found_executions):
    title_section = '<strong style="font-size: 24px;">Dbx Elements</strong>'
    st.markdown(title_section, unsafe_allow_html=True)
    st.markdown("<br/>", unsafe_allow_html=True)
    dbx_elements_review(found_executions)


def _handle_review_option(option, found_executions):
    if (found_executions is None or len(found_executions) <= 0) and st.session_state["sidebar_option"] != "Execution":
        empty_screen.show()
    else:
        actions = {
            "Execution": lambda: execution_metrics(found_executions),
            "Inventories": lambda: rw.generateInventories(found_executions),
            "Assessment Report": lambda: rw.assesmentReport(found_executions),
            "Code Tree Map": lambda: _code_tree_map(found_executions),
            "Mappings": lambda: rw.mappings(found_executions),
            "Readiness by File": lambda: rw.readinessFile(found_executions),
            "Reader Writers": lambda: review_readers_writers(found_executions),
            "Third Party": lambda: third_party_review(found_executions),
            "Dependencies": lambda: _dependencies_reports(found_executions),
            "Artifacts Dependencies": lambda: _artifact_dependencies_reports(found_executions),
            "Dbx Elements": lambda: _dbx_elements_reports(found_executions),
        }

        if option in actions:
            actions[option]()


def _code_tree_map(found_executions):
    executions = files_backend.get_input_files_by_execution_id_grouped_by_technology(found_executions)
    total_files = executions[backend.COLUMN_FILES].sum()
    title_section = '<strong style="font-size: 24px;">Code Tree Map</strong>'
    st.markdown(title_section, unsafe_allow_html=True)
    st.info(
        f"This assessment has a total of {total_files} files. This treemap can be used to identify folders were most of the code is grouped.",
    )
    st.plotly_chart(
        rw.buildTreemap(found_executions),
        use_container_width=True,
        config={"modeBarButtonsToRemove": ["toImage"], "displaylogo": False},
    )


def run_public_review():
    create_side_bar()
    select_execution()
    _handle_review_option(
        st.session_state["sidebar_option"],
        st.session_state["found_executions"],
    )
