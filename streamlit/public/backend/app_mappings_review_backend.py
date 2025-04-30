from string import capwords

import streamlit as st

import public.backend.app_snowpark_utils as utils

from public.backend.globals import *
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when
from snowflake.snowpark.table import Table


class mappings_review_backend:

    _mappings: Table = None
    _session: Session = None
    _migration_status = [
        ("DirectHelper", "Direct Helper"),
        ("WorkAround", "Workaround"),
        ("NotSupported", "Not Supported"),
        ("RenameHelper", "Rename Helper"),
    ]

    _third_party_columns = [
        (COLUMN_CATEGORY, f" {COLUMN_CATEGORY.capitalize()}"),
        (COLUMN_SOURCE_LIBRARY_NAME, "Source Library/API"),
        (COLUMN_TARGET_LIBRARY_NAME, "Snowflake Library/API"),
        (COLUMN_STATUS, capwords(FRIENDLY_NAME_MAPPING_STATUS)),
    ]

    _mapping_table_name: str = None

    def __init__(self):
        mapping_table_name = st.session_state[KEY_LOADED_MAPPINGS]
        self._session = utils.get_session()
        current_schema = self._session.get_current_schema()
        if mapping_table_name == "third_party":
            self._mappings = self._session.table(
                f"{current_schema}.{TABLE_MAPPINGS_CORE_LIBRARIES}",
            )
            self._mappings = self._mappings.rename(col(COLUMN_ORIGIN), COLUMN_CATEGORY)
        else:
            if mapping_table_name == "spark" or mapping_table_name is None:
                self._mappings = self._session.table(
                    f"{current_schema}.{TABLE_MAPPINGS_CORE_SPARK}",
                )
            if mapping_table_name == "pyspark":
                self._mappings = self._session.table(
                    f"{current_schema}.{TABLE_MAPPINGS_CORE_PYSPARK }",
                )
            if mapping_table_name == "pandas":
                self._mappings = self._session.table(
                    f"{current_schema}.{TABLE_MAPPINGS_CORE_PANDAS }",
                )

            self._normalize_mapping_status()

    def _normalize_mapping_status(self):
        for old_status, new_status in self._migration_status:
            self._mappings = self._mappings.with_column(
                COLUMN_MAPPING_STATUS,
                when(col(COLUMN_MAPPING_STATUS) == old_status, new_status).otherwise(
                    col(COLUMN_MAPPING_STATUS),
                ),
            )

    def _normalize_columns_name(self, dataframe: Table, technology: str) -> Table:
        third_party = st.session_state[KEY_LOADED_MAPPINGS] == "third_party"
        known_api_columns = [
            (COLUMN_CATEGORY, f" {COLUMN_CATEGORY.capitalize()}"),
            (
                COLUMN_SPARK_FULLY_QUALIFIED_NAME,
                capwords(f"{technology} FULLY QUALIFIED NAME"),
            ),
            (
                COLUMN_SNOWPARK_FULLY_QUALIFIED_NAME,
                capwords(FRIENDLY_NAME_SNOWPARK_FULLY_QUALIFIED_NAME),
            ),
            (COLUMN_MAPPING_STATUS, capwords(FRIENDLY_NAME_MAPPING_STATUS)),
        ]
        columns_name = self._third_party_columns if third_party else known_api_columns
        for old_status, new_status in columns_name:
            dataframe = dataframe.rename(col(old_status), new_status)

        return dataframe

    def get_latest_version(self) -> str:
        return self.get_tool_versions()[0]

    def get_filter_mappings_by_column(self, dataframe: Table = None) -> Table:
        if dataframe is None:
            dataframe = self._mappings

        if st.session_state[KEY_LOADED_MAPPINGS] == "third_party":
            dataframe = dataframe.select(
                col(COLUMN_CATEGORY),
                col(COLUMN_SOURCE_LIBRARY_NAME),
                col(COLUMN_TARGET_LIBRARY_NAME),
                col(COLUMN_STATUS),
            )

        else:

            dataframe = dataframe.select(
                col(COLUMN_CATEGORY),
                col(COLUMN_SPARK_FULLY_QUALIFIED_NAME),
                col(COLUMN_SNOWPARK_FULLY_QUALIFIED_NAME),
                col(COLUMN_MAPPING_STATUS),
                )
        techonolgy = st.session_state[KEY_LOADED_MAPPINGS] if st.session_state[KEY_LOADED_MAPPINGS] != "pandas" else "SOURCE"
        return self._normalize_columns_name(dataframe, techonolgy)

    def get_migration_status(self) -> Table:
        mapping_table_name = st.session_state[KEY_LOADED_MAPPINGS]
        migration_status_df = self._mappings
        version = st.session_state[KEY_MAPPING_TOOL_VERSION]
        if version != "ALL":
            migration_status_df = migration_status_df.filter(
                col(COLUMN_VERSION) == version,
            )
        mapping_status = (
            COLUMN_STATUS
            if mapping_table_name == "third_party"
            else COLUMN_MAPPING_STATUS
        )
        migration_status_df = migration_status_df.groupBy(mapping_status).count()
        migration_status_df = migration_status_df.rename(
            col(mapping_status), FRIENDLY_NAME_MAPPING_STATUS,
        )
        return migration_status_df

    def get_mappings_by_version(self, version: str) -> Table:
        return self._mappings.where(col(COLUMN_VERSION) == version)

    def get_tool_versions(self) -> list:
        versions = (
            self._mappings.select(col(COLUMN_VERSION))
            .distinct()
            .sort(col(COLUMN_VERSION).desc())
            .collect()
        )
        versions = [row[COLUMN_VERSION] for row in versions]
        return versions

    def get_categories(self) -> list:
        categories = self._mappings.select(col(COLUMN_CATEGORY)).distinct().collect()
        categories = [row[COLUMN_CATEGORY] for row in categories]
        return categories

    def get_mappings_using_filters(
        self,
        category: str = None,
        version: str = None,
        show_changes_from_last_release: bool = None,
    ) -> Table:
        tool_version = version if version is not None else self.get_latest_version()
        filtered_df = self._mappings

        if show_changes_from_last_release:
            tool_version = self.get_latest_version()

        if tool_version != "ALL":
            filtered_df = filtered_df.filter(col(COLUMN_VERSION) == tool_version)

        if category is not None and category != "ALL":
            filtered_df = filtered_df.filter(col(COLUMN_CATEGORY) == category)

        return self.get_filter_mappings_by_column(filtered_df)
