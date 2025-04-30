import os.path

import streamlit as st

import public.frontend.error_handling as errorHandling

from public.backend.app_snowpark_utils import _get_base_path, get_decoded_asset


@errorHandling.executeFunctionWithErrorHandling
def show(message = None):
    st.markdown("\b")
    st.markdown("\b")
    st.markdown("\b")
    st.markdown("\b")

    image_full_path = os.path.join(_get_base_path(), "public", "assets", "empty-icon.png")
    graph_logo = get_decoded_asset(image_full_path)
    no_data_component = f"""
        <div style="display: flex; align-items: center; flex-direction: column;">
            <img src="data:image/png;base64,
                       {graph_logo}"/>
            <p>{message if message is not None else "Please enter an execution in the search bar to see data."}</p>
          </div>
      """
    st.markdown(no_data_component, unsafe_allow_html=True)
