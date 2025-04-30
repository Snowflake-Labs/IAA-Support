import streamlit as st

from public.backend.app_snowpark_utils import IAAIcon
from public.backend.globals import RELOAD_EXECUTIONS_BUTTON
from public.backend.refresh_executions import refresh_executions


def load_header():
    col1, col2, col3 = st.columns([0.1, 1, 0.4], gap="small")
    with col1:
        IAAIcon()
    with col2:
        st.markdown("""# Interactive Assessment Application""", unsafe_allow_html=True)
    with col3:
        with st.columns([2, 1])[1]:
            st.markdown("<br/>", unsafe_allow_html=True)
            st.button(
                RELOAD_EXECUTIONS_BUTTON,
                on_click=refresh_executions,
                use_container_width=True,
                key="refresh_manual_executions",
            )

    st.markdown(
        """
               This portal provides information related to your SMA executions, Snowpark Parity Metadata, API metrics and others 
               """,
    )
