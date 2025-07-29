from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import col, lit, replace


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
