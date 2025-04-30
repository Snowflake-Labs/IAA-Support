import streamlit as st

import public.frontend.empty_screen as emptyScreen
import public.frontend.error_handling as errorHandling

from public.backend import artifact_dependency_backend as artifacts
from public.backend.globals import *
from public.backend.utils import get_nbsp_str


def print_artifact_title():
    title_section = '<strong style="font-size: 24px;">Select a file</strong>'
    st.markdown(title_section, unsafe_allow_html=True)
    st.markdown("<br/>", unsafe_allow_html=True)
    st.markdown("""
                        Select a file in the table below to see its dependencies. Understanding these dependencies will help you migrate your file successfully.
                        """)


def initialize_input_element_values():
    if "search_by_file" not in st.session_state:
        st.session_state["search_by_file"] = ""
    if "criteria" not in st.session_state:
        st.session_state.criteria = CHOOSE_AN_OPTION
    if "total_issues" not in st.session_state:
        st.session_state["total_issues"] = ""
    if "filter_by_column" not in st.session_state:
        st.session_state["filter_by_column"] = CHOOSE_AN_OPTION
    if "selected_files" not in st.session_state:
        st.session_state["selected_files"] = None


def print_summary_table(execution_list, code_file_path_value, filter_by_column_option, option_criteria, total_issues) -> bool:

    df = artifacts.get_artifacts_summary(execution_list, code_file_path_value, filter_by_column_option, option_criteria, total_issues)

    if df is None or df.shape[0] <= 0:
        st.info("No dependencies detected.")
        return False
    if df.shape[0] > 1000:
        st.warning(f"{df.shape[0]} rows found. Showing first 1000.")
        files_df_to_show = df.head(1000)
    else:
        files_df_to_show = df

    edited_df = st.data_editor(
        files_df_to_show,
        column_config={
            FRIENDLY_NAME_TOOL_NAME: None,
        },
        disabled=[
            FRIENDLY_NAME_UNKNOWN_LIBRARIES,
            FRIENDLY_NAME_USER_CODE_FILES,
            FRIENDLY_NAME_THIRD_PARTY_LIBRARIES,
            FRIENDLY_NAME_INPUT_OUTPUT_SOURCES,
            FRIENDLY_NAME_SQL_OBJECTS,
            FRIENDLY_NAME_TOTAL_DEPENDENCIES,
            FRIENDLY_NAME_TOTAL_ISSUES,
        ],
        use_container_width=True,
        hide_index=True,
    )
    selected_files = list(
        files_df_to_show.loc[edited_df[edited_df[FRIENDLY_NAME_SELECT] == True].index][
            FRIENDLY_NAME_CODE_FILE_PATH
        ],
    )
    st.session_state["selected_files"] = selected_files
    return True


def print_df(dataframe, message):
    if dataframe is not None and dataframe.count() > 0:
        st.dataframe(dataframe, use_container_width=True, hide_index=True)
    else:
        st.success(message)


def print_totals(selected_file_raw_data):
    total_green = artifacts.get_total_green_dependencies(selected_file_raw_data)
    total_red = artifacts.get_total_red_dependencies(selected_file_raw_data)
    st.markdown(f"{DEPENDENCY_ARTIFACT_RED_ICON}{get_nbsp_str(2)}{total_red}{get_nbsp_str(6)}{DEPENDENCY_ARTIFACT_GREEN_ICON}{get_nbsp_str(2)}{total_green}")


def print_totals_by_type_section(raw_file_data, dependency_type):
    total_green = artifacts.get_count_green_dependency_by_type(raw_file_data, dependency_type)
    total_red = artifacts.get_count_red_dependency_by_type(raw_file_data, dependency_type)
    text = f"<b>Total Status:</b>{get_nbsp_str(2)}{DEPENDENCY_ARTIFACT_RED_ICON}{get_nbsp_str(2)}{total_red}{get_nbsp_str(6)}{DEPENDENCY_ARTIFACT_GREEN_ICON}{get_nbsp_str(2)}{total_green}{get_nbsp_str(2)}"
    div = f"<div style='text-align: right;margin-bottom:15px;'>{text}</div>"
    st.write(div, unsafe_allow_html=True)

def print_details(execution_list):

    if st.session_state["selected_files"] is not None and len(st.session_state["selected_files"]) > 0:

        raw_file_data = artifacts.get_artifacts_dependency_table_data_by_execution_id(execution_list, st.session_state["selected_files"])
        selected_file_raw_data = raw_file_data
        title_section = '<strong style="font-size: 24px;">Dependencies details</strong>'
        st.markdown(title_section, unsafe_allow_html=True)
        st.markdown("<b>Total dependencies status</b>", unsafe_allow_html=True)

        print_totals(selected_file_raw_data)

        with st.expander("Unknown Libraries"):

            st.markdown("This section shows all the third-party libraries whose origin was not determined by the SMA tool and need to be reviewed to ensure they are compatible with Snowflake.")

            unknown_libraries_df = artifacts.get_unknown_libraries_detailed(selected_file_raw_data)
            print_df(unknown_libraries_df, "This file does not have any unknown dependencies.")
            print_totals_by_type_section(raw_file_data, UNKNOWN_LIBRARIES_ARTIFACT_DEPENDENCY_TYPE_KEY)

        with st.expander("User Code Files"):

            st.markdown("This section refers to all the imports from another files in the same workload. This is useful to understand the order in which files should be migrated to Snowflake.")
            user_code_file_df = artifacts.get_user_code_file_dependency_detailed(selected_file_raw_data)
            print_df(user_code_file_df, "This file does not have any import to another file in the same workload.")
            print_totals_by_type_section(raw_file_data, USER_CODE_FILE_ARTIFACT_DEPENDENCY_TYPE_KEY)

        with st.expander("Third-Party Libraries"):

            st.markdown("This section shows any third-party libraries used in the file. This is useful to understand the libraries that need to be installed in Snowflake to run the file.")
            third_party_libraries_df = artifacts.get_third_party_libraries_detailed(selected_file_raw_data)
            print_df(third_party_libraries_df, "The file does not have any third-party libraries.")
            print_totals_by_type_section(raw_file_data, THIRD_PARTY_LIBRARIES_ARTIFACT_DEPENDENCY_TYPE_KEY)

        with st.expander("Input/Output Sources"):

            st.markdown("This section shows information about the file’s input and output operations. This is useful for understanding the data sources and destinations that the file interacts with.")
            input_output_sources_df = artifacts.get_input_output_source_dependency_detailed(selected_file_raw_data)
            print_df(input_output_sources_df, "The file does not have any input/output sources.")
            print_totals_by_type_section(raw_file_data, IO_SOURCES_ARTIFACT_DEPENDENCY_TYPE_KEY)

        with st.expander("Sql Objects"):

            st.markdown("This section shows all SQL entities such as tables, views, etc., used in the file. This is useful to understand the objects that need to be created in Snowflake to run the file.")
            sql_objects_df = artifacts.get_sql_object_detailed(selected_file_raw_data)
            print_df(sql_objects_df, "The file does not have any sql objects.")
            print_totals_by_type_section(raw_file_data, SQL_OBJECT_ARTIFACT_DEPENDENCY_TYPE_KEY)
    else:
        st.empty()


@errorHandling.executeFunctionWithErrorHandling
def artifact_dependency_review(execution_list):

    if execution_list is None or len(execution_list) <= 0:
        emptyScreen.show()
    else:
        print_artifact_title()
        initialize_input_element_values()

        with st.container():
            col1, col2, col3, col4, col5 = st.columns(5)

            with col1:
                search_by_code_file_path = st.text_input("Search by Code File Path", key="search_by_file")
                code_file_path_value = search_by_code_file_path.strip()

            with col2:
                filter_options = [
                    CHOOSE_AN_OPTION,
                    FRIENDLY_NAME_UNKNOWN_LIBRARIES,
                    FRIENDLY_NAME_USER_CODE_FILES,
                    FRIENDLY_NAME_THIRD_PARTY_LIBRARIES,
                    FRIENDLY_NAME_INPUT_OUTPUT_SOURCES,
                    FRIENDLY_NAME_SQL_OBJECTS,
                    FRIENDLY_NAME_TOTAL_DEPENDENCIES,
                    FRIENDLY_NAME_TOTAL_ISSUES,
                 ]
                filter_by_column_option = st.selectbox("Select Column", key="filter_by_column", options=filter_options)
            with col3:
                options = [CHOOSE_AN_OPTION, ArtifactCriteriaOptions.MORE_THAN, ArtifactCriteriaOptions.LESS_THAN, ArtifactCriteriaOptions.EQUAL_TO]
                option_criteria = st.selectbox("Select Criteria", key="criteria", options=  options)
            with col4:
                total_issues = st.text_input("Enter total issues value", key="total_issues")
            with col5:
                st.markdown('<div style="padding-top: 30px;"></div>', unsafe_allow_html=True)
                st.button("Clear filters", on_click=reset_filters, use_container_width=True)

        success = print_summary_table(execution_list, code_file_path_value, filter_by_column_option, option_criteria, total_issues)
        if success:
            print_details(execution_list)

def reset_filters():
    st.session_state["search_by_file"] = ""
    st.session_state["criteria"] = CHOOSE_AN_OPTION
    st.session_state["filter_by_column"] = CHOOSE_AN_OPTION
    st.session_state["total_issues"] = ""
