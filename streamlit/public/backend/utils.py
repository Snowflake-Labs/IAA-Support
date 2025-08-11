from enum import Enum, auto

import streamlit as st

from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import col, lit, replace


class TextType(Enum):
    PAGE_TITLE = auto()
    SUBTITLE = auto()
    CARD_TITLE = auto()
    CHART_TITLE = auto()


FONT_PROPERTIES = {
    TextType.PAGE_TITLE: {"family": "Source Sans Pro", "font_weight": "600", "font_size": 24},
    TextType.SUBTITLE: {"family": "Source Sans Pro", "font_weight": "600", "font_size": 20},
    TextType.CARD_TITLE: {"family": "Source Sans Pro", "font_weight": "600", "font_size": 16},
    TextType.CHART_TITLE: {"family": "Source Sans Pro", "font_weight": "600", "font_size": 16},
}

DEFAULT_FONT_PROPERTIES = {"family": "Source Sans Pro", "font_weight": "600", "font_size": 16}


def get_nbsp_str(n):
    return "&nbsp;" * n


def normalize_path_column(column_name: str) -> Column:
    r"""
    Returns a Snowpark Column expression that normalizes file paths by replacing
    all forward slashes ('/') with backslashes ('\') in the specified column.

    Args:
        column_name (str): The name of the column containing file paths.

    Returns:
        Column: A Snowpark Column expression with normalized path separators.

    """
    return replace(col(column_name), lit("/"), lit("\\"))


def get_font_properties(text_type: TextType) -> dict:
    """
    Returns the font family and size based on the given text type.

    Args:
        text_type (TextType): The type of text (e.g., PAGE_TITLE, SUBTITLE).

    Returns:
        dict: A dictionary containing 'family', weight and 'size' of the font.

    """
    return FONT_PROPERTIES.get(text_type, DEFAULT_FONT_PROPERTIES)


def render_text_with_style(text: str, text_type: TextType) -> None:
    """
    Renders the given text with specific font properties based on the text type.

    Args:
        text (str): The text to be styled and rendered.
        text_type (TextType): The type of text (e.g., PAGE_TITLE, SUBTITLE, etc.)
                              that determines the font family and size.

    Returns:
        None: This function does not return a value. It renders the styled text
              directly in the Streamlit app.

    """
    font_props = get_font_properties(text_type)
    style_str = (
        f"font-family:{font_props['family']}; "
        f"font-weight:{font_props['font_weight']}; "
        f"font-size:{font_props['font_size']}px;"
    )
    html_str = f'<span style="{style_str}">{text}</span>'
    st.markdown(html_str, unsafe_allow_html=True)
