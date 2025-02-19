import streamlit as st
import public.backend.app_snowpark_utils as utils
import public.frontend.empty_screen as emptyScreen
import public.backend.telemetry as telemetry
from public.backend.globals import *
import public.frontend.error_handling as errorHandling
from public.backend import third_party_usages_backend


@errorHandling.executeFunctionWithErrorHandling
def third_party_review(executionIds):
    title_section = f'<strong style="font-size: 24px;">Third Party</strong>'
    st.markdown(title_section, unsafe_allow_html=True)
    st.markdown("<br/>", unsafe_allow_html=True)
    if executionIds is None or len(executionIds) <= 0:
        emptyScreen.show()
    else:
        st.markdown(
            """
        Here, you will see all the references to a third party library or function across the scanned code base.
        """
        )
        df_to_show = []
        df = third_party_usages_backend.get_third_party_usages_inventory_table_data_by_execution_id(
            executionIds
        )

        if (
            CATEGORIES_FILTER in st.session_state
            and len(st.session_state[CATEGORIES_FILTER]) > 0
        ):
            df = df[df[COLUMN_CATEGORY].isin(st.session_state[CATEGORIES_FILTER])]

        if df is None or df.shape[0] <= 0:
            st.info("No information available.")
            df_to_show = None
        elif df.shape[0] > 1000:
            st.warning(f"{df.shape[0]} rows found. Showing first 1000.")
            df_to_show = df.head(1000)
        else:
            df_to_show = df
        if df_to_show is not None:
            utils.paginated(df_to_show, key_prefix="third_party")

            if st.button("Export table", "third-party-dwn"):
                utils.generateExcelFile(
                    df,
                    third_party_usages_backend.SHEET_THIRD_PARTY,
                    "Download third-party",
                    f"third-party-{utils.getFileNamePrefix(executionIds)}.xlsx",
                )
                eventAttributes = {EXECUTIONS: executionIds}
                telemetry.logTelemetry(CLICK_EXPORT_THIRD_PARTY, eventAttributes)
