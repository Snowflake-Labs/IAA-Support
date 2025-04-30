import os.path
import time

from collections.abc import Callable

import streamlit as st

from public.backend.app_snowpark_utils import IAAIcon, _get_base_path, get_decoded_asset
from public.backend.globals import *
from public.backend.globals import RELOAD_EXECUTIONS_BUTTON
from public.backend.refresh_executions import refresh_executions


def run_portal():
    col1, col2, col3, col4 = st.columns([0.1, 1, 0.5, 0.3], gap="small")
    with col1:
        IAAIcon()
    with col2:
        st.markdown("""# Interactive Assessment Application""")
    with col4:
        if not st.session_state["disable_private_version"]:
            st.markdown("<br/>", unsafe_allow_html=True)
            st.button("Switch to private version", on_click=_switch_to_public_version, key="back_private_preview_btn",  use_container_width=True)
            st.button(RELOAD_EXECUTIONS_BUTTON, on_click=refresh_executions, use_container_width=True,
                      key="refresh_manual_executions" )
    if "show_jira_message_success" in st.session_state:
        st.success(f"The {st.session_state.show_jira_message_success} has been sent successfully!")
        del st.session_state["show_jira_message_success"]
        time.sleep(0.5)
        st.rerun()

    st.markdown("""
        This portal provides information related to your SMA executions, Snowpark Parity Metadata, API metrics, and others.
        """)

    _load_interactive_options()

def _load_interactive_options():
    st.markdown("<br/>", unsafe_allow_html=True)
    st.markdown("<br/>", unsafe_allow_html=True)
    left_col, right_col = st.columns(2, gap="medium")
    with left_col:
        inner_container = st.container(border=True, height=100)
        with inner_container:
            # Pending: Open Executions page.
            st.markdown("")
            _display_option_box(os.path.join(_get_base_path(), "public", "assets", "insert_chart.svg"),
                                "Explore my executions",
                                "select_explore_executions_btn",
                                _explore_my_executions)
    with right_col:
        inner_container = st.container(border=True, height=100)
        with inner_container:
            st.markdown("")
            _display_option_box(os.path.join(_get_base_path(), "public", "assets", "search.svg"),
                                "Explore the compatibility between Spark and Snowpark",
                                "select_explore_compatibility_btn",
                                _explore_spark_compatibility)

    url = "https://github.com/Snowflake-Labs/IAA-Support/blob/main/docs/UserGuide.md"
    need_help = f"Need help? Go to the [User Guide]({url})"
    st.markdown("<br/>", unsafe_allow_html=True)
    st.markdown(need_help)

def _display_option_box(icon_path: str, text: str, button_id: str, on_click: Callable):
    graph_logo = get_decoded_asset(icon_path)
    left_inner_container_col, right_inner_container_col = st.columns([0.70, 0.15])
    with left_inner_container_col:
        st.markdown(f"""
                <img src="data:image/svg+xml;base64,
                    {graph_logo}"
                    padding-top="5%"
                    style='padding-right: 5px;'
                />
                {text}""",
                unsafe_allow_html=True)
    with right_inner_container_col:
        st.button("Select", key=button_id, on_click=on_click)

def _switch_to_public_version():
    st.session_state["Page"] = "Private"
    if "selected_option" in st.session_state:
        del st.session_state.selected_option

def _explore_my_executions():
    st.session_state["Page"] = "Review"
    st.session_state["sidebar_option"] = "Execution"

def _explore_spark_compatibility():
    st.session_state[KEY_MAPPING_TOOL_VERSION] = None
    st.session_state["Page"] = "Mappings"
    st.session_state[KEY_LOADED_MAPPINGS] = "third_party"
