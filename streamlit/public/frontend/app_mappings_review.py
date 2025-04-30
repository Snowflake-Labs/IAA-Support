
import plotly.express as px
import streamlit as st

from streamlit import session_state

import public.backend.app_snowpark_utils as utils
import public.backend.app_style_values as style

from public.backend.app_mappings_review_backend import mappings_review_backend
from public.backend.app_snowpark_utils import color_label_component
from public.backend.globals import *
from public.frontend.app_snowpark_review_executions import _home_landing_page
from public.frontend.column_painter import paint_mapping_status
from public.frontend.shared_components import load_header


def _create_side_bar():
    options = [
        ("API Module Mappings", "third_party"),
        ("Spark API Mappings", "spark"),
        ("PySpark API Mappings", "pyspark"),
        ("Pandas API Mappings", "pandas"),
    ]
    st.sidebar.button(
        "Home",
        on_click=_home_landing_page,
        use_container_width=True,
        key="home_btn_mappings",
    )
    st.sidebar.markdown(
        "<h4 style='text-align: center;'>Mapping Tables</h4>", unsafe_allow_html=True,
    )

    for label, mapping_table in options:
        key = f"{mapping_table}_btn"
        if st.session_state[KEY_LOADED_MAPPINGS] == mapping_table:
            st.sidebar.markdown(
                f"<button key = '{key}' style='background-color: #E4F5FF; border: solid 1px #0068C9; padding: 5.5px; width: 100%; border-radius: 7px; outline: none;"
                f"' onclick=\"window.location.href='#'\">{label}</button>",
                unsafe_allow_html=True,
            )
        else:
            st.sidebar.button(
                label,
                on_click=_load_mappings,
                args=(mapping_table,),
                use_container_width=True,
                key=key,
            )

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


def _load_mappings(mapping_table: str):
    st.session_state[KEY_LOADED_MAPPINGS] = mapping_table
    st.session_state[KEY_MAPPING_TOOL_VERSION] = None


def _update_tool_version(backend: mappings_review_backend):
    version = ""
    if (
        KEY_ON_CHANGE_MAPPING_TOOL_VERSION not in session_state
        or st.session_state[KEY_ON_CHANGE_MAPPING_TOOL_VERSION] is None
    ):
        version = backend.get_latest_version()
    else:
        version = st.session_state[KEY_ON_CHANGE_MAPPING_TOOL_VERSION]
    st.session_state[KEY_MAPPING_TOOL_VERSION] = version


def _trigger_last_release(backend: mappings_review_backend):
    show_last_release_mappings = st.session_state[
        KEY_ON_CHANGE_SHOW_MAPPINGS_LAST_RELEASE
    ]
    if show_last_release_mappings:
        st.session_state[KEY_MAPPING_TOOL_VERSION] = backend.get_latest_version()
    else:
        st.session_state[KEY_ON_CHANGE_MAPPING_TOOL_VERSION] = st.session_state[
            KEY_MAPPING_TOOL_VERSION
        ]


def open_reviews():
    _create_side_bar()
    load_header()
    backend = mappings_review_backend()
    if (
        KEY_MAPPING_TOOL_VERSION not in st.session_state
        or st.session_state[KEY_MAPPING_TOOL_VERSION] is None
    ):
        st.session_state[KEY_MAPPING_TOOL_VERSION] = backend.get_latest_version()
    st.markdown(
        "<h3 style='padding-top: 45px;'>Summary Metrics</h3>", unsafe_allow_html=True,
    )
    st.markdown(f"Table Version Number: {st.session_state[KEY_MAPPING_TOOL_VERSION]}")

    migration_status = backend.get_migration_status()
    if migration_status:
        fig = px.bar(
            data_frame=migration_status.to_pandas(),
            x=COLUMN_COUNT,
            y="MAPPING STATUS",
            orientation="h",
            text=[
                f"{value[COLUMN_COUNT]}"
                for value in migration_status.select(COLUMN_COUNT).collect()
            ],
        )
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, config={"modeBarButtonsToRemove": ["toImage"], "displaylogo": False})

    documentation_url = "https://docs.snowconvert.com/sma"
    st.markdown(
        """ 
        The Snowpark Migration Accelerator (SMA) team keeps a set of mapping tables based on a source's compatibility with Snowpark. Each of these mapping tables are researched by the SMA team and should reflect the current compatibility status for each of the unique elements shown below. For more information on the mapping tables, view [the mapping tables section of the SMA documentation](%s).
    """
        % documentation_url,
    )

    st.markdown(
        """
        However, your feedback is essential to keeping these tables accurate! If you notice something that is not correct or have a suggestion about something, use the built-in feedback mechanism that is built into this product. For more information on how to provide feedback, please click in the link below.
       """,
    )
    st.info(icon="💡", body= f"[Click here to give us feedback about mappings]({MAPPINGS_FEEDBACK_URL})")
    mapping_title = st.session_state[KEY_LOADED_MAPPINGS].capitalize()
    mapping_title = (
        f"{mapping_title} API Mappings"
        if mapping_title != "Third_party"
        else "API Module Mappings"
    )
    st.markdown(
        f"""<h3 style='padding-top: 15px;'>{mapping_title}</h3>""",
        unsafe_allow_html=True,
    )

    show_changes_from_last_release = st.checkbox(
        "Show changes from last release",
        key=KEY_ON_CHANGE_SHOW_MAPPINGS_LAST_RELEASE,
        on_change=_trigger_last_release,
        args=(backend,),
    )

    categories = backend.get_categories()
    categories.insert(0, "ALL")
    category = st.selectbox(
        "Category", categories, index=0, key=KEY_ON_CHANGE_SELECT_CATEGORY,
    )

    versions = backend.get_tool_versions()
    versions.insert(0, "ALL")
    version = st.selectbox(
        "Version",
        options=versions,
        disabled=show_changes_from_last_release,
        on_change=_update_tool_version,
        index=1,
        key=KEY_ON_CHANGE_MAPPING_TOOL_VERSION,
        args=(backend,),
    )

    mappings = backend.get_mappings_using_filters(
        category, version, show_changes_from_last_release,
    )

    if mappings is None or mappings.count() == 0:
        st.markdown("No data found")
    else:
        style = (paint_mapping_status, ["Mapping Status"], 1)
        utils.paginated(mappings.toPandas(), styled= style, key_prefix="mapping_tables", legend=color_legend_mapping_tables)

def split_frame(_input_df, rows):
    df = [
        _input_df.loc[i : i + rows - 1, :] for i in range(0, _input_df.shape[0], rows)
    ]
    return df


def color_legend_mapping_tables():
    col1, col2, col3, _ = st.columns([0.2, 0.15, 0.25, 0.3], gap="small")
    with col1:
        color_label_component("Snowflake Supported", style.LIGHT_GREEN_COLOR)
    with col2:
        color_label_component("Not Supported", style.LIGHT_RED_COLOR)
    with col3:
        color_label_component("Workaround", style.LIGTH_YELLOW_COLOR)
