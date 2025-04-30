import streamlit as st

import public.backend.app_snowpark_utils as utils
import public.frontend.empty_screen as emptyScreen
import public.frontend.error_handling as errorHandling

from public.backend import io_files_backend, telemetry
from public.backend.globals import *


@errorHandling.executeFunctionWithErrorHandling
def review_readers_writers(executionIds):
    title_section = '<strong style="font-size: 24px;">Reader Writers</strong>'
    st.markdown(title_section, unsafe_allow_html=True)
    st.markdown("<br/>", unsafe_allow_html=True)
    if executionIds is None or len(executionIds) <= 0:
        emptyScreen.show()
    else:
        st.markdown(
            """
    Here, you will see any reference to a read or write from an external source listed in a table along with the line number in the file where that reference occurs. Consider this a really basic look at the Extract and Load process present in the scanned code base.

    """,
        )
        df = io_files_backend.get_io_files_inventory_table_data_by_execution_id(
            executionIds,
        )
        if df is not None and df.shape[0] > 0:
            utils.paginated(df, key_prefix="reader_writers")

            if st.button(label="Export table", key="download_reader_writer"):
                utils.generateExcelFile(
                    df,
                    "inventory",
                    "Download reader writers",
                    f"readers-writers-{utils.getFileNamePrefix(executionIds)}.xlsx",
                )
                eventAttributes = {EXECUTIONS: executionIds}
                telemetry.logTelemetry(CLICK_EXPORT_READERS_WRITERS, eventAttributes)

        else:
            st.info("No information available.")
