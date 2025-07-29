import tempfile

import altair as alt
import networkx as nx
import streamlit as st
import streamlit.components.v1 as components

from pyvis.network import Network

import public.frontend.empty_screen as emptyScreen
import public.frontend.error_handling as errorHandling
import snowflake.snowpark.dataframe

from public.backend import artifact_dependency_backend as artifacts
from public.backend.artifact_dependency_backend import (
    get_artifacts_dependency_table_data,
    get_islands_summary,
)
from public.backend.globals import *
from public.backend.utils import get_nbsp_str


def print_artifact_title() -> None:
    st.markdown("<br/>", unsafe_allow_html=True)
    st.markdown("""
                        Select a file in the table below to see its dependencies. Understanding these dependencies will help you migrate your file successfully.
                        """)


def initialize_input_element_values() -> None:
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
    if "selected_islands" not in st.session_state:
        st.session_state["selected_islands"] = None


def print_summary_table(
    execution_list: str,
    code_file_path_value: str,
    filter_by_column_option: str,
    option_criteria: str,
    total_issues: str,
    selected_islands: list = None,
) -> bool:
    df = artifacts.get_artifacts_summary(
        execution_list,
        code_file_path_value,
        filter_by_column_option,
        option_criteria,
        total_issues,
        selected_islands,
    )

    if df is None or df.shape[0] <= 0:
        st.info("No dependencies detected.")
        return False
    if df.shape[0] > 1000:
        st.warning(f"{df.shape[0]} rows found. Showing first 1000.")
        files_df_to_show = df.head(1000)
    else:
        files_df_to_show = df

    styled_df = files_df_to_show.style.applymap(
        change_color_if_has_issues,
        subset=[FRIENDLY_NAME_TOTAL_ISSUES],
    )
    edited_df = st.data_editor(
        styled_df,
        column_config={
            FRIENDLY_NAME_TOOL_NAME: None,
        },
        disabled=[
            COLUMN_ISLAND,
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
        files_df_to_show.loc[edited_df[edited_df[FRIENDLY_NAME_SELECT] == True].index][FRIENDLY_NAME_CODE_FILE_PATH],
    )
    st.session_state["selected_files"] = selected_files
    return True


def print_df(dataframe, message) -> None:
    if dataframe is not None and dataframe.count() > 0:
        st.dataframe(dataframe, use_container_width=True, hide_index=True)
    else:
        st.success(message)


def print_totals(selected_file_raw_data) -> None:
    total_green = artifacts.get_total_green_dependencies(selected_file_raw_data)
    total_red = artifacts.get_total_red_dependencies(selected_file_raw_data)
    st.markdown(
        f"{DEPENDENCY_ARTIFACT_RED_ICON}{get_nbsp_str(2)}{total_red}{get_nbsp_str(6)}{DEPENDENCY_ARTIFACT_GREEN_ICON}{get_nbsp_str(2)}{total_green}",
    )


def print_totals_by_type_section(raw_file_data, dependency_type) -> None:
    total_green = artifacts.get_count_green_dependency_by_type(raw_file_data, dependency_type)
    total_red = artifacts.get_count_red_dependency_by_type(raw_file_data, dependency_type)
    text = f"<b>Total Status:</b>{get_nbsp_str(2)}{DEPENDENCY_ARTIFACT_RED_ICON}{get_nbsp_str(2)}{total_red}{get_nbsp_str(6)}{DEPENDENCY_ARTIFACT_GREEN_ICON}{get_nbsp_str(2)}{total_green}{get_nbsp_str(2)}"
    div = f"<div style='text-align: right;margin-bottom:15px;'>{text}</div>"
    st.write(div, unsafe_allow_html=True)


def print_chart(raw_file_data: snowflake.snowpark.DataFrame) -> None:
    df = artifacts.get_graphics_information(raw_file_data)

    if not df.empty:
        st.markdown(
            "This section displays the count of distinct dependencies that the selected files rely on, categorized by type and status",
        )

        color_map = {
            "Exists": "#219EBC",
            "Does Not Exists": "#ced4da",
            "Supported": "#48cae4",
            "Not Supported": "#f28482",
            "Parsed": "#023E8A",
            "Not Parsed": "#ffb703",
        }

        data_to_plot = df.groupby([COLUMN_TYPE, COLUMN_STATUS])[COLUMN_DEPENDENCIES].sum().reset_index()

        chart = (
            alt.Chart(data_to_plot)
            .mark_bar(size=30)
            .encode(
                y=alt.Y("TYPE:N", title="Type", scale=alt.Scale(padding=6)),
                x=alt.X("sum(DEPENDENCIES):Q", title="Qty", axis=alt.Axis(format="d")),
                color=alt.Color(
                    "STATUS:N",
                    scale=alt.Scale(
                        domain=list(color_map.keys()),
                        range=list(color_map.values()),
                    ),
                    title="Status",
                ),
                order=alt.Order(
                    "sum(DEPENDENCIES)",
                    sort="descending",
                ),
            )
        )

        final_chart = (chart).configure_axis(
            labelFontSize=12,
            titleFontSize=14,
        )

        st.altair_chart(final_chart, use_container_width=True)


def print_islands_summary(islands: list, execution_id: str) -> None:
    """
    Displays a summary of detected islands in the artifact dependency graph.
    Also, the list of selected islands will be updated in a session_state property ["selected_islands"].

    Args:
        islands (list): A list of sets, where each set contains the nodes (files) on an island.
        execution_id (str): The execution ID used to fetch artifact dependency data.

    Returns:
        None.

    """
    st.markdown("#### Detected Islands")
    st.markdown("This sections shows the list of islands and their total code files.")

    summary_df = get_islands_summary(islands, execution_id)
    edited_df = st.data_editor(
        summary_df,
        column_config={
            FRIENDLY_NAME_TOOL_NAME: None,
            COLUMN_ISLAND_ID: None,
        },
        disabled=[
            FRIENDLY_NAME_ISLAND_NAME,
            FRIENDLY_NAME_TOTAL_CODE_FILES,
            FRIENDLY_NAME_FAILURE_COUNT,
        ],
        use_container_width=True,
        hide_index=True,
    )

    selected_islands = list(
        summary_df.loc[edited_df[edited_df[FRIENDLY_NAME_SELECT] == True].index][COLUMN_ISLAND_ID],
    )

    st.session_state["selected_islands"] = selected_islands


def get_graph_information(execution_id: str) -> (nx.Graph, list):
    graph = nx.Graph()
    artifacts_data = get_artifacts_dependency_table_data([execution_id])
    pandas_data = artifacts_data.toPandas()
    index = 1
    for row in pandas_data.iterrows():
        if row[1][COLUMN_TYPE] == USER_CODE_FILE_ARTIFACT_DEPENDENCY_TYPE_KEY:
            file = row[1][COLUMN_FILE_ID]
            dependency = row[1][COLUMN_DEPENDENCY]

            # add main file as a node
            if file not in graph:
                graph.add_node(file)

            # If a dependency exists, we add it and create the edge
            if dependency:
                if dependency not in graph:
                    graph.add_node(dependency)

                graph.add_edge(file, dependency, title=f"{file} imports {dependency}")

            index += 1

    # --- Find the "Islands" (connected components) ---
    islands = list(nx.connected_components(graph))
    return graph, islands


def assign_island_to_nodes(graph: nx.Graph, islands: list) -> nx.Graph:
    # island colors
    colors = ["#FF5733", "#33FF57", "#3357FF", "#F3FF33", "#FF33A8", "#33FFF3"]

    # Assign an island ID to each file, so we can paint it
    for i, island_nodes in enumerate(islands):
        island_color = colors[i % len(colors)]

        for node in island_nodes:
            graph.nodes[node]["group"] = i
            graph.nodes[node]["color"] = "#000000"
            graph.nodes[node]["size"] = 25
            graph.nodes[node]["title"] = f"Island: {i + 1} | File: {node}"

    return graph


def create_interactive_graph(graph) -> None:
    # --- Creation of Interactive Graph with Pyvis ---
    net = Network(
        height="500px",
        width="100%",
        bgcolor="white",
        font_color="black",
        cdn_resources="in_line",
    )

    # Move the graph from NetworkX to Pyvis
    net.from_nx(graph)

    net.set_options("""
               var options = {
                 "physics": { "solver": "forceAtlas2Based" }
               }
               """)

    # --- Show in Streamlit using a unique temp file ---
    try:
        with tempfile.NamedTemporaryFile(suffix=".html") as tmp_file:
            # Save the graph file in the temp root
            net.save_graph(tmp_file.name)

            # Read the content of the temp file
            with open(tmp_file.name, encoding="utf-8") as f:
                source_code = f.read()

        search_text = "var network = new vis.Network(container, data, options);"

        #    Adjust zoom
        replace_text = """
            var network = new vis.Network(container, data, options);
            network.on("stabilizationIterationsDone", function () {
              network.fit({ animation: true });
            });
            """

        source_code_fitted = source_code.replace(search_text, replace_text)

        # Show html in Streamlit
        components.html(source_code_fitted, height=500)

    except Exception as e:
        st.error(f"An error occur while printing the graph: {e}")


def print_islands(execution_id: str, graph: nx.Graph, islands: list) -> None:
    st.markdown("##### 🏝️ File Dependency Islands Viewer")
    st.markdown(
        "Each island represents a group of files that are interconnected through dependencies, but not connected to other islands.",
    )

    # --- Show detected islands ---
    if not islands:
        st.warning("No islands detected.")
    else:
        st.markdown(
            "Visualize the file islands and their connections below. Interact with the graph by dragging nodes, zooming, and exploring connections.",
        )

        graph = assign_island_to_nodes(graph, islands)
        create_interactive_graph(graph)
        print_islands_summary(islands, execution_id)


def print_details(execution_list) -> None:
    if st.session_state["selected_files"] is not None and len(st.session_state["selected_files"]) > 0:
        raw_file_data = artifacts.get_artifacts_dependency_table_data_by_execution_id(
            execution_list,
            st.session_state["selected_files"],
        )
        selected_file_raw_data = raw_file_data

        st.markdown("<b>Selected Files:</b>", unsafe_allow_html=True)
        for file in st.session_state["selected_files"]:
            st.markdown(f"<li style='margin-bottom: 3px;'>{file}</li>", unsafe_allow_html=True)

        st.markdown("#### Dependency Details")
        print_chart(raw_file_data)

        st.markdown("<b>Total dependencies status</b>", unsafe_allow_html=True)

        print_totals(selected_file_raw_data)

        with st.expander("Unknown Libraries"):
            st.markdown(
                "This section shows all the third-party libraries whose origin was not determined by the SMA tool and need to be reviewed to ensure they are compatible with Snowflake.",
            )

            unknown_libraries_df = artifacts.get_unknown_libraries_detailed(selected_file_raw_data)
            print_df(unknown_libraries_df, "This file does not have any unknown dependencies.")
            print_totals_by_type_section(raw_file_data, UNKNOWN_LIBRARIES_ARTIFACT_DEPENDENCY_TYPE_KEY)

        with st.expander("User Code Files"):
            st.markdown(
                "This section refers to all the imports from another files in the same workload. This is useful to understand the order in which files should be migrated to Snowflake.",
            )
            user_code_file_df = artifacts.get_user_code_file_dependency_detailed(selected_file_raw_data)
            print_df(user_code_file_df, "This file does not have any import to another file in the same workload.")
            print_totals_by_type_section(raw_file_data, USER_CODE_FILE_ARTIFACT_DEPENDENCY_TYPE_KEY)

        with st.expander("Third-Party Libraries"):
            st.markdown(
                "This section shows any third-party libraries used in the file. This is useful to understand the libraries that need to be installed in Snowflake to run the file.",
            )
            third_party_libraries_df = artifacts.get_third_party_libraries_detailed(selected_file_raw_data)
            print_df(third_party_libraries_df, "The file does not have any third-party libraries.")
            print_totals_by_type_section(raw_file_data, THIRD_PARTY_LIBRARIES_ARTIFACT_DEPENDENCY_TYPE_KEY)

        with st.expander("Input/Output Sources"):
            st.markdown(
                "This section shows information about the file’s input and output operations. This is useful for understanding the data sources and destinations that the file interacts with.",
            )
            input_output_sources_df = artifacts.get_input_output_source_dependency_detailed(selected_file_raw_data)
            print_df(input_output_sources_df, "The file does not have any input/output sources.")
            print_totals_by_type_section(raw_file_data, IO_SOURCES_ARTIFACT_DEPENDENCY_TYPE_KEY)

        with st.expander("Sql Objects"):
            st.markdown(
                "This section shows all SQL entities such as tables, views, etc., used in the file. This is useful to understand the objects that need to be created in Snowflake to run the file.",
            )
            sql_objects_df = artifacts.get_sql_object_detailed(selected_file_raw_data)
            print_df(sql_objects_df, "The file does not have any sql objects.")
            print_totals_by_type_section(raw_file_data, SQL_OBJECT_ARTIFACT_DEPENDENCY_TYPE_KEY)
    else:
        st.empty()


@errorHandling.executeFunctionWithErrorHandling
def artifact_dependency_review(execution_list: list) -> None:
    if execution_list is None or len(execution_list) <= 0:
        emptyScreen.show()
    elif len(execution_list) > 1:
        st.warning("Please select only one execution to review its artifact dependencies.")
    else:
        execution_id = execution_list[0]
        graph, islands = get_graph_information(execution_id)
        print_islands(execution_id, graph, islands)
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
                options = [
                    CHOOSE_AN_OPTION,
                    ArtifactCriteriaOptions.MORE_THAN,
                    ArtifactCriteriaOptions.LESS_THAN,
                    ArtifactCriteriaOptions.EQUAL_TO,
                ]
                option_criteria = st.selectbox("Select Criteria", key="criteria", options=options)
            with col4:
                total_issues = st.text_input("Enter total issues value", key="total_issues")
            with col5:
                st.markdown('<div style="padding-top: 30px;"></div>', unsafe_allow_html=True)
                st.button("Clear filters", on_click=reset_filters, use_container_width=True)

        artifacts.update_island_column(islands, execution_id)
        success = print_summary_table(
            execution_id,
            code_file_path_value,
            filter_by_column_option,
            option_criteria,
            total_issues,
            st.session_state["selected_islands"],
        )
        if success:
            print_details(execution_list)


def reset_filters() -> None:
    st.session_state["search_by_file"] = ""
    st.session_state["criteria"] = CHOOSE_AN_OPTION
    st.session_state["filter_by_column"] = CHOOSE_AN_OPTION
    st.session_state["total_issues"] = ""
    st.session_state["selected_islands"] = None


def change_color_if_has_issues(value) -> None:
    color = "#FFCDD2" if value > 0 else ""
    return f"background-color: {color}"
