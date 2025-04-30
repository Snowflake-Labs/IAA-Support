from datetime import datetime

import streamlit as st

import public.backend.app_snowpark_utils as utils

from public.backend.globals import *
from snowflake.snowpark.functions import col, lit


def logEvent(session, eventName):
    eventTimestamp = datetime.now()
    UserEmail = str(st.experimental_user.email)
    dfEvent = session.create_dataframe([(eventTimestamp, eventName, UserEmail)], schema=[COLUMN_TIMESTAMP, COLUMN_NAME, COLUMN_USER])
    dfEvent.write.mode("append").save_as_table(TELEMETRY_EVENTS, column_order ="name")
    eventId = session.table(TELEMETRY_EVENTS).filter((col(COLUMN_TIMESTAMP) == lit(eventTimestamp))\
                                                                                   & (col(COLUMN_USER) == lit(UserEmail))\
                                                                                   & (col(COLUMN_NAME) == lit(eventName)))\
                                                                                   .collect()[0].ID
    return eventId


def logEventAttribute(session, eventId, attName, attValue):
    attValue = str(attValue)
    session = utils.get_session()
    attributeTimestamp = datetime.now()
    dfAttribute = session.create_dataframe([(eventId, attributeTimestamp, attName, attValue)], schema=[EVENTID, COLUMN_TIMESTAMP, COLUMN_NAME, COLUMN_VALUE])
    dfAttribute.write.mode("append").save_as_table(TELEMETRY_EVENTS_ATTRIBUTES)


def logTelemetry(eventName: str, attObject: dict = None):
    session = utils.get_session()
    eventId = logEvent(session, eventName)
    if attObject is not None:
        for name in attObject:
            logEventAttribute(session, eventId, name, attObject[name])

