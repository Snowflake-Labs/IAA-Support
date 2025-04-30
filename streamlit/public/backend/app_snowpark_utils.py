import base64
import io
import os.path
import pathlib
import re
import time

from collections.abc import Callable
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import streamlit as st

from natsort import natsort_keygen

import public.backend.app_style_values as style
import public.backend.snowflake_types as sftype

from public.backend import executions_backend
from public.backend.globals import *
from snowflake.snowpark import Column, Session
from snowflake.snowpark import functions as F


last_refresh = ["00:00:00"]

technologies = ["py", "Python", "python", "ipynb", "Scala", "scala"]

_tables_metadata = {
    TABLE_EXECUTION_INFO: [
        ("EXECUTION_ID", sftype.STRING),
        ("EXECUTION_TIMESTAMP", sftype.TIMESTAMP),
        ("CUSTOMER_LICENSE", sftype.STRING),
        ("TOOL_VERSION", sftype.STRING),
        ("TOOL_NAME", sftype.STRING),
        ("MARGIN_ERROR", sftype.STRING),
        ("CLIENT_EMAIL", sftype.STRING),
        ("MAPPING_VERSION", sftype.STRING),
        ("CONVERSION_SCORE", sftype.STRING),
        ("SESSION_ID", sftype.STRING),
        ("COMPANY", sftype.STRING),
        ("PROJECT_NAME", sftype.STRING),
        ("PARSING_SCORE", sftype.STRING),
        ("SPARK_API_READINESS_SCORE", sftype.STRING),
        ("PYTHON_READINESS_SCORE", sftype.STRING),
        ("SCALA_READINESS_SCORE", sftype.STRING),
        ("SQL_READINESS_SCORE", sftype.STRING),
        ("THIRD_PARTY_READINESS_SCORE", sftype.STRING),
    ],
    TABLE_INPUT_FILES_INVENTORY: [
        ("EXECUTION_ID", sftype.STRING),
        ("ELEMENT", sftype.STRING),
        ("PROJECT_ID", sftype.STRING),
        ("FILE_ID", sftype.STRING),
        ("COUNT", sftype.BIGINT),
        ("EXTENSION", sftype.STRING),
        ("BYTES", sftype.BIGINT),
        ("CHARACTER_LENGTH", sftype.BIGINT),
        ("LINES_OF_CODE", sftype.BIGINT),
        ("PARSE_RESULT", sftype.STRING),
        ("TECHNOLOGY", sftype.STRING),
        ("IGNORED", sftype.STRING),
        ("ORIGIN_FILE_PATH", sftype.STRING),
    ],
    TABLE_SPARK_USAGES_INVENTORY: [
        ("EXECUTION_ID", sftype.STRING),
        ("ELEMENT", sftype.STRING),
        ("PROJECT_ID", sftype.STRING),
        ("FILE_ID", sftype.STRING),
        ("COUNT", sftype.BIGINT),
        ("ALIAS", sftype.STRING),
        ("KIND", sftype.STRING),
        ("LINE", sftype.BIGINT),
        ("PACKAGE_NAME", sftype.STRING),
        ("SUPPORTED", sftype.STRING),
        ("AUTOMATED", sftype.STRING),
        ("STATUS", sftype.STRING),
        ("SNOWCONVERT_CORE_VERSION", sftype.STRING),
        ("SNOWPARK_VERSION", sftype.STRING),
        ("CELL_ID", sftype.BIGINT),
        ("PARAMETERS_INFO", sftype.VARIANT),
    ],
    TABLE_IMPORT_USAGES_INVENTORY: [
        ("EXECUTION_ID", sftype.STRING),
        ("ELEMENT", sftype.STRING),
        ("PROJECT_ID", sftype.STRING),
        ("FILE_ID", sftype.STRING),
        ("COUNT", sftype.BIGINT),
        ("ALIAS", sftype.STRING),
        ("KIND", sftype.STRING),
        ("LINE", sftype.BIGINT),
        ("PACKAGE_NAME", sftype.STRING),
        ("STATUS", sftype.STRING),
        ("IS_SNOWPARK_ANACONDA_SUPPORTED", sftype.STRING),
        ("ELEMENT_PACKAGE", sftype.STRING),
        ("CELL_ID", sftype.BIGINT),
        ("ORIGIN", sftype.STRING),
    ],
    TABLE_IO_FILES_INVENTORY: [
        ("EXECUTION_ID", sftype.STRING),
        ("ELEMENT", sftype.STRING),
        ("PROJECT_ID", sftype.STRING),
        ("FILE_ID", sftype.STRING),
        ("COUNT", sftype.BIGINT),
        ("IS_LITERAL", sftype.STRING),
        ("FORMAT", sftype.STRING),
        ("FORMAT_TYPE", sftype.STRING),
        ("MODE", sftype.STRING),
        ("LINE", sftype.BIGINT),
        ("OPTION_SETTINGS", sftype.STRING),
        ("CELL_ID", sftype.BIGINT),
    ],
    TABLE_THIRD_PARTY_USAGES_INVENTORY: [
        ("EXECUTION_ID", sftype.STRING),
        ("ELEMENT", sftype.STRING),
        ("PROJECT_ID", sftype.STRING),
        ("FILE_ID", sftype.STRING),
        ("COUNT", sftype.BIGINT),
        ("ALIAS", sftype.STRING),
        ("KIND", sftype.STRING),
        ("LINE", sftype.BIGINT),
        ("PACKAGE_NAME", sftype.STRING),
        ("CELL_ID", sftype.BIGINT),
        ("PARAMETERS_INFO", sftype.VARIANT),
    ],
    ## --------
    TABLE_COMPUTED_DEPENDENCIES : [
        ("TOOL_EXECUTION_ID", sftype.STRING),
        ("TOOL_EXECUTION_TIMESTAMP", sftype.TIMESTAMP),
        ("SOURCE_FILE", sftype.STRING),
        ("IMPORT", sftype.STRING),
        ("DEPENDENCIES", sftype.ARRAY_STRING),
        ("PROJECT_ID", sftype.STRING),
        ("IS_BUILTIN", sftype.BOOLEAN),
        ("ORIGIN", sftype.STRING),
        ("SUPPORTED", sftype.BOOLEAN),
    ],
    TABLE_MAPPINGS_CORE_SPARK: [
        ("CATEGORY", sftype.STRING),
        ("SPARK_FULLY_QUALIFIED_NAME", sftype.STRING),
        ("SPARK_NAME", sftype.STRING),
        ("SPARK_CLASS", sftype.STRING),
        ("SPARK_DEF", sftype.STRING),
        ("SNOWPARK_FULLY_QUALIFIED_NAME", sftype.STRING),
        ("SNOWPARK_NAME", sftype.STRING),
        ("SNOWPARK_CLASS", sftype.STRING),
        ("SNOWPARK_DEF", sftype.STRING),
        ("VERSION", sftype.STRING),
        ("TOOL_SUPPORTED", sftype.BOOLEAN),
        ("SNOWFLAKE_SUPPORTED", sftype.BOOLEAN),
        ("MAPPING_STATUS", sftype.STRING),
        ("WORKAROUND_COMMENT", sftype.STRING),
    ],
    TABLE_MAPPINGS_CORE_PYSPARK: [
        ("CATEGORY", sftype.STRING),
        ("SPARK_FULLY_QUALIFIED_NAME", sftype.STRING),
        ("SPARK_NAME", sftype.STRING),
        ("SPARK_CLASS", sftype.STRING),
        ("SPARK_DEF", sftype.STRING),
        ("SNOWPARK_FULLY_QUALIFIED_NAME", sftype.STRING),
        ("SNOWPARK_NAME", sftype.STRING),
        ("SNOWPARK_CLASS", sftype.STRING),
        ("SNOWPARK_DEF", sftype.STRING),
        ("VERSION", sftype.STRING),
        ("TOOL_SUPPORTED", sftype.BOOLEAN),
        ("SNOWFLAKE_SUPPORTED", sftype.BOOLEAN),
        ("MAPPING_STATUS", sftype.STRING),
        ("WORKAROUND_COMMENT", sftype.STRING),
    ],
    TABLE_REPORT_URL: [
        ("EXECUTION_ID", sftype.STRING),
        ("FILE_NAME", sftype.STRING),
        ("RELATIVE_REPORT_PATH", sftype.STRING),
    ],
    TABLE_THIRD_PARTY_CATEGORIES: [
        ("CATEGORY", sftype.STRING),
        ("FILTER", sftype.STRING),
    ],
    JAVA_BUILTINS: [("NAME", sftype.STRING)],
    TABLE_ARTIFACT_DEPENDENCY_INVENTORY:[
        ("EXECUTION_ID", sftype.STRING),
        ("FILE_ID", sftype.STRING),
        ("DEPENDENCY", sftype.STRING),
        ("TYPE", sftype.STRING),
        ("SUCCESS", sftype.BOOLEAN),
        ("STATUS_DETAIL", sftype.STRING),
        ("ARGUMENTS", sftype.VARIANT),
        ("LOCATION", sftype.VARIANT),
    ],
    TABLE_DBX_ELEMENTS_INVENTORY: [
        ("EXECUTION_ID", sftype.STRING),
        ("ELEMENT", sftype.STRING),
        ("PROJECT_ID", sftype.STRING),
        ("FILE_ID", sftype.STRING),
        ("COUNT", sftype.BIGINT),
        ("CATEGORY", sftype.STRING),
        ("KIND", sftype.STRING),
        ("LINE", sftype.BIGINT),
        ("PACKAGE_NAME", sftype.STRING),
        ("SUPPORTED", sftype.STRING),
        ("AUTOMATED", sftype.STRING),
        ("STATUS", sftype.STRING),
        ("SNOWCONVERT_CORE_VERSION", sftype.STRING),
        ("CELL_ID", sftype.BIGINT),
        ("TECHNOLOGY", sftype.STRING),
    ],
}


def getCurrentTimestamp():  # pragma: no cover
    return datetime.timestamp(datetime.now())


def getDateWithDelta(days_to_substract):
    date = datetime.today() - timedelta(days=days_to_substract)
    return date


@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    input_df = input_df.reset_index(drop=True)
    df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df


def add_spacing(spaces_count=1):
    while spaces_count > 0:
        st.text("")
        spaces_count -= 1


def pagination_footer(dataset, key_prefix, need_color_legend=False, legend: Callable = None):
    batch_size = 25
    current_page = 0

    if len(dataset) > 0:
        if need_color_legend and key_prefix == "import_dependency_table":
                color_legend_import()
        bottom_menu = st.columns((4, 1, 1))
        with bottom_menu[2]:
            add_spacing()
            batch_size = st.selectbox(
                "Row Count", key=f"pag_page_size_{key_prefix}", options=[25, 50, 100],
            )
            with st.spinner("Loading..."):
                time.sleep(2)
        with bottom_menu[1]:
            add_spacing()
            total_pages = (
                int(len(dataset) / batch_size)
                if int(len(dataset) / batch_size) > 0
                else 1
            )
            col1, col2 = st.columns(2)
            with col2:
                current_page = st.number_input(
                    "Page",
                    key=f"pag_curr_page_{key_prefix}",
                    min_value=1,
                    max_value=total_pages,
                    step=1,
                )
            with col1:
                add_spacing(2)
                st.markdown(f"Page **{current_page}** of **{total_pages}** ")
        with bottom_menu[0]:
            if legend is not None:
                legend()
            if need_color_legend and key_prefix == "dependency_table":
                color_legend_dependencies()

    return batch_size, current_page


def paginated(
    dataset: pd.DataFrame,
    styled=None,
    key_prefix="",
    editable: bool = False,
    dropdown_cols: list = [],
    legend: Callable = None,
):
    if len(dataset) == 0:
        st.info("No information available.")
        return None

    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio(
            "Sort Data",
            key=f"pag_sort_{key_prefix}",
            options=["Yes", "No"],
            horizontal=1,
            index=1,
        )
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox(
                "Sort By", key=f"pag_sort_field_{key_prefix}", options=dataset.columns,
            )
        with top_menu[2]:
            sort_direction = st.radio(
                "Direction",
                key=f"pag_sort_dir_{key_prefix}",
                options=["⬆️", "⬇️"],
                horizontal=True,
            )
        dataset = dataset.sort_values(
            by=sort_field,
            key=natsort_keygen(),
            ascending=sort_direction == "⬆️",
            ignore_index=True,
        )

    pagination = st.container()

    batch_size, current_page = pagination_footer(dataset, key_prefix, legend=legend)
    pages = split_frame(dataset, batch_size)
    page_to_display = pages[current_page - 1]
    if current_page == 1:
        page_to_display = reset_index(page_to_display)
    else:
        page_to_display.index = np.arange(batch_size * current_page - batch_size, batch_size * current_page)

    if styled:
        style_func, subset, axis = styled
        page_to_display = page_to_display.style.apply(style_func, axis=1, subset=subset)

    if editable:
        with st.spinner("Loading Mappings..."):
            if styled:
                page_to_display = page_to_display.data
            try:
                page_to_display.fillna(False)
                for col in dropdown_cols:
                    if col in [
                        FRIENDLY_NAME_SNOWFLAKE_SUPPORTED,
                        FRIENDLY_NAME_TOOL_SUPPORTED,
                        COLUMN_SUPPORTED,
                    ]:
                        data = getSupportedStatus()
                    if col in [FRIENDLY_NAME_MAPPING_STATUS, COLUMN_STATUS]:
                        data = getMappingStatusList()
                    page_to_display[col] = page_to_display[col].astype(
                        pd.CategoricalDtype(data),
                    )
                with pagination:
                    dfEdited = st.data_editor(
                        page_to_display,
                        use_container_width=True,
                        disabled=(
                            COLUMN_CATEGORY,
                            FRIENDLY_NAME_SPARK_FULLY_QUALIFIED_NAME,
                            FRIENDLY_NAME_SNOWPARK_FULLY_QUALIFIED_NAME,
                            FRIENDLY_NAME_EXISTING_FEEDBACK,
                        ),
                    )
                    if dfEdited.equals(page_to_display) == False:
                        dfEdited = dfEdited.compare(
                            page_to_display, result_names=(KEY_SUGGESTION, KEY_ORIGINAL),
                        )
                        dfEdited.columns = dfEdited.columns.map(",".join)
                        dfEdited = dfEdited.merge(
                            page_to_display[
                                [
                                    FRIENDLY_NAME_SPARK_FULLY_QUALIFIED_NAME,
                                    FRIENDLY_NAME_EXISTING_FEEDBACK,
                                    FRIENDLY_NAME_MAPPING_COMMENTS,
                                ]
                            ],
                            left_index=True,
                            right_index=True,
                        )
                        return dfEdited
                    return None
            except Exception as e:
                st.error(f"An unexpected error occurred loading existing feedback: {e}")
                return None
    else:
        pagination.dataframe(data=page_to_display, use_container_width=True)
        return page_to_display


def fix_path(c: Column):
    # change slash
    c = F.replace(c, "\\", "/")
    # replace leading slash if present
    c = F.iff(c.startswith("/"), F.substring(c, 2, F.length(c)), c)
    return c


def getFileNamePrefix(executionIds):
    if len(executionIds) > 0:
        companies = executions_backend.get_file_companies_data(executionIds)
        return str(companies + "_" + str(len(executionIds)))
    return ""


def looks_like_email(s):
    """
    Determines if the given string looks like an email address.

    Parameters
    ----------
    s (str): The string to check.

    Returns
    -------
    bool: True if the string looks like an email address, False otherwise.

    """
    pattern = r"^\S+@\S+\.\S+$"
    return bool(re.match(pattern, s))


def looks_like_guid(s):
    """
    Determines if the given string looks like a GUID.

    Parameters
    ----------
    s (str): The string to check.

    Returns
    -------
    bool: True if the string looks like a GUID, False otherwise.

    """
    pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    return bool(re.match(pattern, s))


@st.cache_resource(ttl=7200)
def get_session():
    try:
        from snowflake.snowpark.context import get_active_session

        return get_active_session()
    except Exception:
        pass
    return create_session()


def _get_base_path():
    base_path = f"{pathlib.Path(__file__).parent.resolve()}"
    return base_path[: base_path.find("public")]


def create_session():
    session = Session.builder.getOrCreate()
    return session

@st.cache_resource(ttl=600)
def get_temp_stage():
    session = get_session()
    session.sql(f"""drop stage IF EXISTS {TEMP_STAGE_LOCATION}""").show()
    session.sql(
        f"CREATE STAGE IF NOT EXISTS {TEMP_STAGE_LOCATION} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')",
    ).show()
    return f"@{TEMP_STAGE_LOCATION}"


def get_downloadlink(label, filename, output):
    session = get_session()
    temp_stage = get_temp_stage()
    output.seek(0)
    session.file.put_stream(
        output, f"{temp_stage}/{filename}", auto_compress=False, overwrite=True,
    )
    link = session.sql(
        f"select GET_PRESIGNED_URL({temp_stage},'{filename}')",
    ).collect()[0][0]
    st.markdown(f"[{label}]({link})")


def get_downloadlink_nomd(label, filename, output):
    session = get_session()
    temp_stage = get_temp_stage()
    output.seek(0)
    session.file.put_stream(
        output, f"{temp_stage}/{filename}", auto_compress=False, overwrite=True,
    )
    link = session.sql(
        f"select GET_PRESIGNED_URL({temp_stage},'{filename}')",
    ).collect()[0][0]
    return f"[{label}]({link})"


def load_error():
    col1, col2 = st.columns([1, 50])
    image_name = os.path.join(_get_base_path(), "public", "assets", "warning-logo.png")
    mime_type = image_name.split(".")[-1:][0].lower()
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f"data:image/{mime_type};base64,{content_b64encoded}"
    with col1:
        st.image(image_string)
    with col2:
        st.write("Pardon our dust! This section is currently under construction.")


def can_refresh(reference, reset=True):
    nowstr = datetime.now().strftime("%Y-%m-%d %H:%M:%S").split(" ")[1]
    now = datetime.strptime(nowstr, "%H:%M:%S")
    latest = datetime.strptime(reference[0], "%H:%M:%S")
    difference = now - latest
    is_long_enough = difference.total_seconds() > 120
    if is_long_enough and reset:
        last_refresh[0] = now.strftime("%Y-%m-%d %H:%M:%S").split(" ")[1]
    return is_long_enough


def generateExcelFile(dataFrame, sheetName, linkName, fileName):
    output = io.BytesIO()
    with pd.ExcelWriter(
        output, engine="xlsxwriter", engine_kwargs={"options": {"in_memory": True}},
    ) as writer:
        dataFrame.to_excel(writer, sheet_name=sheetName, index=False)
    st.info(
        """
        NOTE: If you click on the following link and it does not download the file, right-click and choose “Open in a New Tab”.
    """,
    )
    get_downloadlink(linkName, fileName, output)


def get_table_metadata(table_name):
    return _tables_metadata.get(table_name)


def get_decoded_asset(path: str):
    with open(path, "rb") as file:
        encoded_asset = base64.b64encode(file.read()).decode("utf-8")
    return encoded_asset


def color_label_component(label, color):
    st.markdown(
        f"""
            <div style="display: flex; align-items: center; padding-bottom: 20px;">
                <div style="
                width: 16px;
                height: 16px;
                background-color: {color};
                margin-right: 5px;
                border-color: #808495;
                border-width: 1.5px;
                border-style: solid;"></div>
                <span style="font-size: 14px;">{label}</span>
            </div>
            """,
        unsafe_allow_html=True,
    )


def color_legend_dependencies():
    col1, _, _ = st.columns(3)
    with col1:
        color_label_component("Unknown dependencies", style.LIGHT_RED_COLOR)


def color_legend_import():
    col1, col2, col3, col4 = st.columns([0.25, 0.15, 0.22, 1], gap="small")
    with col1:
        color_label_component(LABEL_THIRD_PARTY_LIB, style.LIGHT_GREEN_COLOR)
    with col2:
        color_label_component(LABEL_BUILTIN, style.LIGHT_BLUE_COLOR)
    with col3:
        color_label_component(LABEL_USER_DEFINED, style.LIGHT_GRAY_COLOR)
    with col4:
        color_label_component(LABEL_UNKNOWN, style.LIGHT_RED_COLOR)


def highlight_rows(row):
    status_row = row[COLUMN_ORIGIN]
    if status_row == THIRD_PARTY_LIB_KEY:
        return [style.getBackgroundColorProperty(style.LIGHT_GREEN_COLOR)] * len(row)
    if status_row == BUILTIN_KEY:
        return [style.getBackgroundColorProperty(style.LIGHT_BLUE_COLOR)] * len(row)
    if status_row == USER_DEFINED_KEY:
        return [style.getBackgroundColorProperty(style.LIGHT_GRAY_COLOR)] * len(row)
    return [style.getBackgroundColorProperty(style.LIGHT_RED_COLOR)] * len(row)


def paginated_import_dependency_table(dataset: pd.DataFrame, key_prefix):
    pagination = st.container()
    batch_size, current_page = pagination_footer(dataset, key_prefix, True)
    pages = split_frame(dataset, batch_size)
    page_to_display = pages[current_page - 1]
    if current_page == 1:
        page_to_display = reset_index(page_to_display)
    if key_prefix == "import_dependency_table":
        page_to_display = page_to_display.style.apply(highlight_rows, axis=1)
    elif key_prefix == "dependency_table":
        page_to_display = add_total_row(page_to_display)
        if current_page == 1:
            page_to_display = reset_index(page_to_display)
        page_to_display = page_to_display.style.apply(
            _highlight_dependencies_row, axis=1,
        )

    pagination.dataframe(data=page_to_display, use_container_width=True)

    return page_to_display


def _highlight_dependencies_row(row):
    percentage_of_unknown = row["PERCENTAGE OF UNKNOWN"].replace("%", "")
    if row["SOURCE FILE"] == "TOTAL":
        return [""] * len(row)
    if float(percentage_of_unknown) > 0:
        return [style.getBackgroundColorProperty(style.LIGHT_RED_COLOR)] * len(row)
    return [""] * len(row)


def add_total_row(page_to_display):
    total_row = pd.DataFrame(
        {
            "SOURCE FILE": ["TOTAL"],
            "DEPENDENCIES COUNT": [page_to_display["DEPENDENCIES COUNT"].sum()],
            "UNKNOWN DEPENDENCIES": [page_to_display["UNKNOWN DEPENDENCIES"].sum()],
            "PERCENTAGE OF UNKNOWN": [
                f"{(page_to_display['UNKNOWN DEPENDENCIES'].sum() * 100.0) / page_to_display['DEPENDENCIES COUNT'].sum():.2f}%",
            ],
        },
    )

    page_to_display = pd.concat(
        [page_to_display.reset_index(drop=True), total_row], ignore_index=True,
    )
    return page_to_display


def IAAIcon():
    image_full_path = os.path.join(_get_base_path(), "public", "assets", "IAA_Logo.svg")
    graph_logo = get_decoded_asset(image_full_path)
    st.markdown(" ")
    st.markdown(
        f"""
                   <img src="data:image/svg+xml;base64,
                       {graph_logo}"
                       padding-top="5%"
                       width="72px"
                       height="72px"
                   />""",
        unsafe_allow_html=True,
    )

def reset_index(df: pd.DataFrame) -> pd.DataFrame:
    df_indexed = df.copy()
    df_indexed.index = np.arange(1, len(df)+1)
    return df_indexed


def register_app_log(session: Session,
                     message: str,
                     application: str = Applications.IAA,
                     customer_email: str = None,
                     execution_id: str = None):
    try:
        final_message = message.replace("'", "")
        db = session.get_current_database()
        schema = session.get_current_schema()
        query = f"CALL {db}.{schema}.INSERT_APP_LOG('{application}','{final_message}','{customer_email}','{execution_id}')"

        session.sql(query).collect()
    except Exception as e:
        print(e)

def convert_to_int(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return 0
