import traceback

import streamlit as st

from public.backend.globals import *


def executeFunctionWithErrorHandling (func):
    def internalErrorHandler(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as error:
            show_error_message(error, traceback.format_exc())
            return None
    return internalErrorHandler

def show_error_message (error, stackTrace, processName = None):
    processName = f"\nProcess: {processName}." if processName is not None else ""
    showStackTrace = f"\n\nStackTrace: {stackTrace}" if stackTrace is not None and IS_DEBUG_MODE == True else ""
    st.error(f"Oops, Something went wrong.\n{processName}\nIssue: {error}.\n\nIf you're seeing this message, an unexpected error has occurred. Report this issue and help us to improve this tool.{showStackTrace}")
