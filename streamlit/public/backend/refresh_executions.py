import io
import zipfile

import streamlit as st

import public.backend.app_snowpark_utils as utils

from public.backend.globals import (
    COLUMN_EXECUTION_ID,
    COLUMN_EXTRACT_DATE,
    SMA_EXECUTIONS_STAGE,
    TABLE_MANUAL_UPLOADED_ZIPS,
    UPDATE_IAA_TABLES_TASK,
)
from snowflake.snowpark.functions import col, regexp_extract
from snowflake.snowpark.types import StringType, StructField, StructType


def refresh_executions():
    session = utils.get_session()
    dest_db = session.get_current_database()
    dest_schema = session.get_current_schema()
    dest_stage = SMA_EXECUTIONS_STAGE
    session.sql(f"ALTER TASK {UPDATE_IAA_TABLES_TASK} RESUME;").collect()
    zip_list = session.sql(
        rf"LIST @{dest_db}.{dest_schema}.{dest_stage} PATTERN='.*\.zip$';",
    ).select(
        regexp_extract(col('"name"'), "_([a-zA-Z0-9\\-]+)\\.", 1).alias(COLUMN_EXECUTION_ID),
        col('"last_modified"').alias(COLUMN_EXTRACT_DATE),
    )
    processed_zips = session.sql(f"SELECT * FROM {TABLE_MANUAL_UPLOADED_ZIPS}")
    folders_to_process = (
        zip_list.join(
            processed_zips,
            on=([COLUMN_EXECUTION_ID, COLUMN_EXTRACT_DATE]),
            how="leftanti",
        )
        .select(zip_list[COLUMN_EXECUTION_ID], zip_list[COLUMN_EXTRACT_DATE])
        .collect()
    )
    zip_with_errors = []
    actual_file_name = ""
    for row in folders_to_process:
        try:
            execution_id = row[COLUMN_EXECUTION_ID]
            execution_date = row[COLUMN_EXTRACT_DATE]
            file_path = f"@{dest_stage}/AssessmentFiles_{execution_id}.zip"
            actual_file_name = file_path
            upload_execution(
                session,
                file_path,
                execution_id,
                execution_date,
                dest_db,
                dest_stage,
                dest_schema,
            )
        except:
            zip_with_errors.append(actual_file_name)

    count_failed_zips = len(zip_with_errors)
    if count_failed_zips > 0:
        st.toast(
            f"{count_failed_zips} ZIP file(s) failed to upload.", icon="❗",
        )
    uploaded_zips = len(folders_to_process) - count_failed_zips
    if uploaded_zips > 0:
        st.toast(
            f"{uploaded_zips} ZIP file(s) uploaded successfully.", icon="✅",
        )


def upload_execution(
    session, file_path, execution_id, execution_date, dest_db, dest_stage, dest_schema,
):
    with session.file.get_stream(file_path) as file:
        zip_file = zipfile.ZipFile(file)
        zip_has_data = len(zip_file.filelist) > 0
        if not zip_has_data:
            return
        folder_name = execution_id
        list_files = list(
            filter(
                lambda x: (not x.filename.endswith("/"))
                and (not x.filename.startswith("__MACOSX"))
                and (not x.filename.endswith("\\")),
                zip_file.filelist,
            ),
        )
        destination_folder = f"{dest_stage}/{folder_name}/"
        files_content = [
            (
                io.BytesIO(zip_file.read(zip_info)),
                f"{destination_folder}{zip_info.filename}",
            )
            for zip_info in list_files
        ]
        for data, path in files_content:
            session.file.put_stream(data, path, auto_compress=False, overwrite=True)

        result = session.sql(
            f"CALL {dest_db}.{dest_schema}.REFRESH_MANUAL_EXECUTION()",
        ).collect()

        if result[0]["REFRESH_MANUAL_EXECUTION"] == None:
            return
        execution_info = [(execution_id, execution_date)]
        schema = StructType(
            [
                StructField(COLUMN_EXECUTION_ID, StringType()),
                StructField(COLUMN_EXTRACT_DATE, StringType()),
            ],
        )
        df_to_insert = session.createDataFrame(execution_info, schema)
        df_to_insert.write.saveAsTable("MANUAL_UPLOADED_ZIPS", mode="append")
