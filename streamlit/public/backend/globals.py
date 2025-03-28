import os
import pathlib

# Use this variable to determine if the stack trace on an unexpected issue must be shown or not (False = don't show, True = Show)
# Value must be False for production
IS_DEBUG_MODE = False


AMOUNT_OF_DAYS_FOR_FILTERING = 15


TEMP_STAGE_LOCATION = "TEMP_STAGE"
SMA_EXECUTIONS_STAGE = "SMA_EXECUTIONS"
ASSESSMENTS_STAGE = "ASSESSMENTS_STAGE"
DATA_PYSPARK_MAPPINGS = "PYSPARK_MAPPING_CORE"
DATA_SCALASPARK_MAPPINGS = "SPARK_SCALA_MAPPING_CORE"
MAPPING_REVIEW_REQUESTS = "MAPPING_REVIEW_REQUESTS"
DEPENDENCIES_SOLVED_CACHED = "DEPENDENCIES_SOLVED_CACHED"
JAVA_BUILTINS = "JAVA_BUILTINS"
SNOW_CONVERT_TEMPORATY_FOLDER_NAME = "snowconvert_temporary_folder_name"
SUBMIT_MAPPINGS_FEEDBACK_PROC = "SUBMITMAPPINGSFEEDBACK"
GENERATE_JIRA_TICKET = "GENERATEJIRATICKET"
DBC_TECHNOLOGY_KEY = "Dbc"
FALSE_KEY = "False"
TRUE_KEY = "True"
ALL_KEY = "All"
HOUR_KEY = "hour"
PYTHON_KEY = "Python"
SCALA_KEY = "Scala"
SCALA_TECHNOLOGY_KEY = "Scala"
SCALA_DBX_TECHNOLOGY_KEY = "ScalaDbx"
SCALA_DBX_NOTEBOOK_TECHNOLOGY_KEY = "ScalaDbxNotebook"
PYTHON_TECHNOLOGY_KEY = "Python"
PYTHON_DBX_TECHNOLOGY_KEY = "PythonDbx"
PYTHON_DBX_NOTEBOOK_TECHNOLOGY_KEY = "PythonDbxNotebook"
SQL_TECHNOLOGY_KEY = "Sql"
SQL_DBX_TECHNOLOGY_KEY = "SqlDbx"
SQL_DBX_NOTEBOOK_TECHNOLOGY_KEY = "SqlDbxNotebook"
JUPYTER_NOTEBOOK_KEY = "JupyterNotebook"
HQL_TECHNOLOGY_KEY = "Hql"
HQL_DBX_TECHNOLOGY_KEY = "HqlDbx"
HQL_NOTEBOOK_TECHNOLOGY_KEY = "HqlNotebook"
NA_KEY = "N/A"
DATE_LAST_DATE_FORMAT_KEY = "yyyy-MM-dd"


UNKNOWN_KEY = "Unknown"
BUILTIN_KEY = "BuiltIn"
USER_DEFINED_KEY = "UserDefined"
THIRD_PARTY_LIB_KEY = "ThirdPartyLib"

LABEL_UNKNOWN = "Unknown"
LABEL_BUILTIN = "Builtin"
LABEL_USER_DEFINED = "User Defined"
LABEL_THIRD_PARTY_LIB = "Third Party Library"

#TASKS
UPDATE_IAA_TABLES_TASK = "UPDATE_IAA_TABLES_TASK"


# TELEMETRY EVENTS
CLICK_GENERATE_INVENTORY = "Click_GenerateInventoryFile"
CLICK_GENERATE_ADDITIONAL_INVENTORY = "Click_GenerateAdditionalInventoryFiles"
CLICK_GENERATE_READINESS_FILE = "Click_GenerateReadinessByFile"
SEE_MAPPINGS = "Show_Mappings"
SE_UNSUPPORTED = "Show_Unsupported"
CLICK_EXPORT_READERS_WRITERS = "Click_ExportReadersWriters"
CLICK_EXPORT_THIRD_PARTY = "Click_ExportThirdParty"
CLICK_GENERATE_DEPENDENCIES_REPORT = "Click_GenerateDependenciesReport"
CLICK_SHOW_REFERENCES = "Click_ShowReferences"
CLICK_SUBMIT_MAPPINGS_FEEDBACK = "Click_SubmitMappingsFeedback"
CLICK_RELOAD_EXECUTIONS = "Click_ReloadExecutions"

# TELEMETRY EVENT ATTRIBUTES
JIRAIDS = "JiraIds"
EXECUTIONS = "Executions"
FILTER = "Filter"
MAPPINGS_TABLE_TECHNOLOGY = "Technology"
SHOW_VERSION_CHANGES = "ShowChangesFromPreviousVersion"
MAPPING_CATEGORY = "MappingsCategory"
TOOL_VERSION = "ToolVersion"
START_WEEK = "StartWeek"
END_WEEK = "EndWeek"
API_REFERENCE = "APIReferences"

# BUTTON DESCRIPTIONS
RELOAD_EXECUTIONS_BUTTON = "Reload"

# KEYS
KEY_JIRA_LABELS = []
KEY_SCT_JIRA_PROJECT = "SCT"
KEY_SUGGESTION = "SUGGESTION"
KEY_TOOL_NAME = "TOOL_NAME"
KEY_ELEMENT = "ELEMENT"
KEY_SCHEMA_NAME = "Schema_Name"
KEY_DIRECT = "DIRECT"
KEY_RENAME = "RENAME"
KEY_HELPER = "HELPER"
KEY_DIRECT_HELPER = "DIRECTHELPER"
KEY_RENAME_HELPER = "RENAMEHELPER"
KEY_TRANSFORMATION = "TRANSFORMATION"
KEY_WORKAROUND = "WORKAROUND"
KEY_NOTSUPPORTED = "NOTSUPPORTED"
KEY_NOTDEFFINED = "NOTDEFINED"
KEY_UNKNOWN = "UNKNOWN"
KEY_OTHER = "OTHER"
KEY_SUGGESTION = "SUGGESTION"
KEY_ORIGINAL = "ORIGINAL"
KEY_READY_TO_MIGRATE = "File(s) Ready to migrate"
KEY_MIGRATE_WITH_MANUAL_EFFORT = "File(s) Can be migrated with low effort"
KEY_ADDITIONAL_INFO_WILL_BE_REQUIRED = "File(s) Need additional revision for migration"
KEY_MAPPINGS_SCHEMA = "MAPPINGS_SHARE"
KEY_PRODUCTION_SCHEMA = "PRODUCTION"
KEY_MAPPING_TOOL_VERSION = "selected_mapping_tool_version"
KEY_ON_CHANGE_MAPPING_TOOL_VERSION = "on_change_mapping_tool_version"
KEY_ON_CHANGE_SELECT_CATEGORY = "category_select_box"
KEY_ON_CHANGE_SHOW_MAPPINGS_LAST_RELEASE = "show_mappings_last_release"
KEY_LOADED_MAPPINGS = "loaded_mappings"
KEY_SUPPORTED = "SUPPORTED"


# TABLES/VIEWS NAMES
TELEMETRY_EVENTS_ATTRIBUTES = "TELEMETRY_EVENTS_ATTRIBUTES"
TELEMETRY_EVENTS = "TELEMETRY_EVENTS"
TABLE_REPORT_URL = "REPORT_URL"
TABLE_INVENTORY = "INVENTORY"
TABLE_INFORMATION_SCHEMA = "INFORMATION_SCHEMA"
TABLE_PACKAGES = "PACKAGES"
TABLE_THIRD_PARTY_CATEGORIES = "THIRD_PARTY_CATEGORIES"
TABLE_LIBRARIES_MAPPING_CORE = "LIBRARIES_MAPPING_CORE"
TABLE_MANUAL_UPLOADED_ZIPS = "MANUAL_UPLOADED_ZIPS"
TABLE_MAPPINGS_CORE_EWI_CATALOG = "MAPPINGS_CORE_EWI_CATALOG"
TABLE_MAPPINGS_CORE_LIBRARIES = "MAPPINGS_CORE_LIBRARIES"
TABLE_MAPPINGS_CORE_PANDAS = "MAPPINGS_CORE_PANDAS"
TABLE_MAPPINGS_CORE_PYSPARK = "MAPPINGS_CORE_PYSPARK"
TABLE_MAPPINGS_CORE_SPARK = "MAPPINGS_CORE_SPARK"
TABLE_MAPPINGS_CORE_SQL_ELEMENTS = "MAPPINGS_CORE_SQL_ELEMENTS"
TABLE_MAPPINGS_CORE_SQL_FUNCTIONS = "MAPPINGS_CORE_SQL_FUNCTIONS"


# STREAM TABLES
TABLE_EXECUTION_INFO = "EXECUTION_INFO"
TABLE_INPUT_FILES_INVENTORY = "INPUT_FILES_INVENTORY"
TABLE_IMPORT_USAGES_INVENTORY = "IMPORT_USAGES_INVENTORY"
TABLE_IO_FILES_INVENTORY = "IO_FILES_INVENTORY"
TABLE_ISSUES_INVENTORY = "ISSUES_INVENTORY"
TABLE_JOINS_INVENTORY = "JOINS_INVENTORY"
TABLE_NOTEBOOK_CELLS_INVENTORY = "NOTEBOOK_CELLS_INVENTORY"
TABLE_NOTEBOOK_SIZE_INVENTORY = "NOTEBOOK_SIZE_INVENTORY"
TABLE_PACKAGES_INVENTORY = "PACKAGES_INVENTORY"
TABLE_PACKAGE_VERSIONS_INVENTORY = "PACKAGE_VERSIONS_INVENTORY"
TABLE_PANDAS_USAGES_INVENTORY = "PANDAS_USAGES_INVENTORY"
TABLE_SPARK_USAGES_INVENTORY = "SPARK_USAGES_INVENTORY"
TABLE_SQL_ELEMENTS_INVENTORY = "SQL_ELEMENTS_INVENTORY"
TABLE_SQL_EMBEDDED_USAGES_INVENTORY = "SQL_EMBEDDED_USAGES_INVENTORY"
TABLE_SQL_FUNCTIONS_INVENTORY = "SQL_FUNCTIONS_INVENTORY"
TABLE_THIRD_PARTY_USAGES_INVENTORY = "THIRD_PARTY_USAGES_INVENTORY"

#COMPUTED FROM STREAMS
TABLE_COMPUTED_DEPENDENCIES = "COMPUTED_DEPENDENCIES"
TABLE_PENDING_EXECUTION_LIST = "PENDING_EXECUTION_LIST"

# COLUMNS NAMES
COLUMN_IS_SNOWPARK_ANACONDA_SUPPORTED = "IS_SNOWPARK_ANACONDA_SUPPORTED"

EVENTID = "EVENTID"
COLUMN_USER = "USER"
COLUMN_TIMESTAMP = "TIMESTAMP"
COLUMN_SESSION_ID = "SESSION_ID"
COLUMN_ELEMENT = "ELEMENT"
COLUMN_FORMAT = "FORMAT"
COLUMN_FORMATTYPE = "FORMATTYPE"
COLUMN_FORMAT_TYPE = "FORMAT_TYPE"
COLUMN_MODE = "MODE"
COLUMN_FILTER = "FILTER"
COLUMN_SUPPORTED = "SUPPORTED"
COLUMN_FILEID = "FILEID"
COLUMN_FILE_ID = "FILE_ID"
COLUMN_LINE = "LINE"
COLUMN_COUNT = "COUNT"
COLUMN_ISLITERAL = "ISLITERAL"
COLUMN_IS_LITERAL = "IS_LITERAL"
COLUMN_PROJECTID = "PROJECTID"
COLUMN_PROJECT_ID = "PROJECT_ID"
COLUMN_EXECUTION_ID = "EXECUTION_ID"
COLUMN_EXTRACT_DATE = "EXTRACT_DATE"
COLUMN_TOOL_EXECUTION_ID = "TOOL_EXECUTION_ID"
COLUMN_EXECUTION_ID_COUNT = "EXECUTION_ID_COUNT"
COLUMN_TECHNOLOGY = "TECHNOLOGY"
COLUMN_IGNORED = "IGNORED"
COLUMN_ORIGIN_FILE_PATH = "ORIGIN_FILE_PATH"
COLUMN_BYTES = "BYTES"
COLUMN_NOT_SUPPORTED = "NOT_SUPPORTED"
COLUMN_SOURCE_FILE = "SOURCE_FILE"
COLUMN_USAGES = "USAGES"
COLUMN_TOOL_EXECUTION_TIMESTAMP = "TOOL_EXECUTION_TIMESTAMP"
COLUMN_EXECUTION_TIMESTAMP = "EXECUTION_TIMESTAMP"
COLUMN_CLIENT_EMAIL = "CLIENT_EMAIL"
COLUMN_EMAIL = "EMAIL"
COLUMN_READINESS = "READINESS"
COLUMN_FILES = "FILES"
COLUMN_CODE_LOC = "CODE_LOC"
COLUMN_TOTAL_CODE_LOC = "TOTAL_CODE_LOC"
COLUMN_COMMENT_LOC = "COMMENT_LOC"
COLUMN_TOTAL_COMMENT_LOC = "TOTAL_COMMENT_LOC"
COLUMN_BLANK_LOC = "BLANK_LOC"
COLUMN_TOTAL_BLANK_LOC = "TOTAL_BLANK_LOC"
COLUMN_MARGIN_ERROR = "MARGIN_ERROR"
COLUMN_MAPPING_VERSION = "MAPPING_VERSION"
COLUMN_CONVERSION_SCORE = "CONVERSION_SCORE"
COLUMN_EXECUTION_ID_ALL_TOGETHER = "EXECUTIONID"
COLUMN_DETAILED_DOCX = "DETAILED_DOCX"
COLUMN_LANGUAGE = "LANGUAGE"
COLUMN_PACKAGE_NAME = "PACKAGE_NAME"
COLUMN_DEPENDENCIES = "DEPENDENCIES"
COLUMN_ORIGIN = "ORIGIN"
COLUMN_IS_BUILTIN = "IS_BUILTIN"
COLUMN_IS_INTERNAL = "IS_INTERNAL"
COLUMN_IMPORT = "IMPORT"
COLUMN_VALUE = "VALUE"
COLUMN_TOTAL = "TOTAL"
COLUMN_TOOL_NAME = "TOOL_NAME"
COLUMN_ISSUE = "ISSUE"
COLUMN_ALIAS = "ALIAS"
COLUMN_KIND = "KIND"
COLUMN_PACKAGENAME = "PACKAGENAME"
COLUMN_PERCENT = "PERCENT"
COLUMN_TOOL_VERSION = "TOOL_VERSION"
COLUMN_CATEGORY = "CATEGORY"
COLUMN_VERSION_AVAILABLE = "VERSION_AVAILABLE"
COLUMN_SIMILAR_PACKAGES = "SIMILAR_PACKAGES"
COLUMN_VERSION = "VERSION"
COLUMN_VERSIONS = "VERSIONS"
COLUMN_NAME = "NAME"
COLUMN_READINESS_SCORE = "SPARK_API_READINESS_SCORE"
COLUMN_TOTAL_CODE_FILES = "TOTAL_CODE_FILES"
COLUMN_LINES_OF_CODE = "LINES_OF_CODE"
COLUMN_TOTAL_LINES_OF_CODE = "TOTAL_LINES_OF_CODE"
COLUMN_DEPENDECIES_COUNT = "DEPENDENCIES_COUNT"
COLUMN_SUPPORTED_REFS = "SUPPORTED_REFS"
COLUMN_TOTAL_REFS = "TOTAL_REFS"
COLUMN_COMPANY = "COMPANY"
COLUMN_SPARK_FULLY_QUALIFIED_NAME = "SPARK_FULLY_QUALIFIED_NAME"
COLUMN_SPARK_NAME = "SPARK_NAME"
COLUMN_SPARK_CLASS = "SPARK_CLASS"
COLUMN_SPARK_DEF = "SPARK_DEF"
COLUMN_SNOWPARK_FULLY_QUALIFIED_NAME = "SNOWPARK_FULLY_QUALIFIED_NAME"
COLUMN_SNOWPARK_NAME = "SNOWPARK_NAME"
COLUMN_SNOWPARK_CLASS = "SNOWPARK_CLASS"
COLUMN_SNOWPARK_DEF = "SNOWPARK_DEF"
COLUMN_TOOL_SUPPORTED = "TOOL_SUPPORTED"
COLUMN_SNOWFLAKE_SUPPORTED = "SNOWFLAKE_SUPPORTED"
COLUMN_MAPPING_STATUS = "MAPPING_STATUS"
COLUMN_STATUS = "STATUS"
COLUMN_WORKAROUND_COMMENT = "WORKAROUND_COMMENT"
COLUMN_PROJECT_NAME = "PROJECT_NAME"
COLUMN_SNOWCONVERTCOREVERSION = "SNOWCONVERTCOREVERSION"
COLUMN_SNOWCONVERT_CORE_VERSION = "SNOWCONVERT_CORE_VERSION"
COLUMN_FILES_COUNT = "FilesCount"
COLUMN_TITLE = "Title"
COLUMN_ORIGIN = "ORIGIN"
COLUMN_SOURCE_LIBRARY_NAME = "SOURCE_LIBRARY_NAME"
COLUMN_TARGET_LIBRARY_NAME = "TARGET_LIBRARY_NAME"
COLUMN_SPARK_API_READINESS_SCORE = "SPARK_API_READINESS_SCORE"
COLUMN_HOURS_DIFFERENCE = "HOURS_DIFFERENCE"
COLUMN_MAX_TIMESTAMP = "MAX_TIMESTAMP"
COLUMN_AVERAGE_READINESS_SCORE = "AVERAGE_READINESS_SCORE"
COLUMN_USAGES_SUPPORTED_COUNT = "USAGES_SUPPORTED_COUNT"
COLUMN_USAGES_IDENTIFIED_COUNT = "USAGES_IDENTIFIED_COUNT"
COLUMN_CUMULATIVE_READINESS_SCORE = "CUMULATIVE_READINESS_SCORE"
COLUMN_THIRD_PARTY_READINESS_SCORE = "THIRD_PARTY_READINESS_SCORE"
COLUMN_SQL_READINESS_SCORE = "SQL_READINESS_SCORE"
COLUMN_DATE_LAST_EXECUTION = "DATE_LAST_EXECUTION"
COLUMN_RELATIVE_REPORT_PATH = "RELATIVE_REPORT_PATH"

# CONFIG
SCHEMA = "schema"
CONFIG_CLIENT_SESSION_KEEP_ALIVE = "client_session_keep_alive"

# SHEET NAMES
SHEET_INVENTORY = "inventory"
SHEET_THIRD_PARTY = "THIRD_PARTY"
SHEET_DEPENDENCIES = "DEPENDENCIES"
SHEET_UNSUPPORTED = "UNSUPPORTED"

# REPORT TYPES
REPORT_TYPE_SUMMARY = "Summary"
REPORT_TYPE_DETAILED = "Detailed"

# USER FRIENDLY COLUMN NAMES
FRIENDLY_NAME_EXECUTION_ID = "EXECUTION ID"
FRIENDLY_NAME_EXECUTION_TIMESTAMP = "EXECUTION TIMESTAMP"
FRIENDLY_NAME_TOOL_NAME = "TOOL NAME"
FRIENDLY_NAME_READINESS_SCORE = "SPARK API READINESS SCORE"
FRIENDLY_NAME_CLIENT_EMAIL = "CLIENT EMAIL"
FRIENDLY_NAME_TOTAL_CODE_FILES = "TOTAL CODE FILES"
FRIENDLY_NAME_LINES_OF_CODE = "LINES OF CODE"
FRIENDLY_NAME_SELECT = "SELECT"
FRIENDLY_NAME_MAPPING_VERSION = "MAPPING VERSION"
FRIENDLY_NAME_CONVERSION_SCORE = "CONVERSION SCORE"
FRIENDLY_NAME_PROJECT_ID = "SRC FOLDER"
FRIENDLY_NAME_TOTAL_CODE_LOC = "TOTAL CODE LOC"
FRIENDLY_NAME_TOTAL_COMMENT_LOC = "TOTAL COMMENT LOC"
FRIENDLY_NAME_TOTAL_BLANK_LOC = "TOTAL BLANK LOC"
FRIENDLY_NAME_NOT_SUPPORTED = "NOT SUPPORTED"
FRIENDLY_NAME_CODE_LOC = "CODE LOC"
FRIENDLY_NAME_SOURCE_FILE = "SOURCE FILE"
FRIENDLY_NAME_API = "API"
FRIENDLY_NAME_FORMAT_TYPE = "FORMAT TYPE"
FRIENDLY_NAME_FILE = "FILE"
FRIENDLY_NAME_IS_LITERAL = "IS LITERAL"
FRIENDLY_NAME_EXTRACT_DATE = "EXTRACT DATE"
FRIENDLY_NAME_DEPENDENCIES_COUNT = "DEPENDENCIES COUNT"
FRIENDLY_NAME_TOTAL = "TOTAL"
FRIENDLY_NAME_DIRECT = "DIRECT"
FRIENDLY_NAME_RENAME = "RENAME"
FRIENDLY_NAME_HELPER = "HELPER"
FRIENDLY_NAME_TRANSFORMATION = "TRANSFORMATION"
FRIENDLY_NAME_WORKAROUND = "WORKAROUND"
FRIENDLY_NAME_OTHER = "OTHER"
FRIENDLY_NAME_PROJECT_NAME = "PROJECT NAME"
FRIENDLY_NAME_SPARK_FULLY_QUALIFIED_NAME = "SPARK FULLY QUALIFIED NAME"
FRIENDLY_NAME_SNOWPARK_FULLY_QUALIFIED_NAME = "SNOWPARK FULLY QUALIFIED NAME"
FRIENDLY_NAME_MAPPING_STATUS = "MAPPING STATUS"
FRIENDLY_NAME_SNOWFLAKE_SUPPORTED = "SNOWFLAKE SUPPORTED"
FRIENDLY_NAME_TOOL_SUPPORTED = "TOOL SUPPORTED"
FRIENDLY_NAME_TOOL_VERSION = "TOOL VERSION"
FRIENDLY_NAME_MAPPING_COMMENTS = "MAPPING COMMENT"
FRIENDLY_NAME_WORKAROUND_COMMENT = "WORKAROUND COMMENT"
FRIENDLY_NAME_EXISTING_FEEDBACK = "EXISTING FEEDBACK"
FRIENDLY_NAME_PERCENTAGES = "PERCENTAGE OF TOTAL REFERENCES"
FRIENDLY_NAME_STATUS_CATEGORY = "STATUS CATEGORY"
FRIENDLY_NAME_STATUS_FILE_COUNT = "FILE COUNT"
FRIENDLY_NAME_IMPORT = "IMPORT NAME"
FRIENDLY_NAME_COMMENT = "COMMENT"


# Session States

CATEGORIES_FILTER = "categories_filter"


def getUnsuppportedStatusList():
    return [
        "Direct",
        "Rename",
        "Helper",
        "Transformation",
        "Workaround",
        "NotSupported",
        "NotDefined",
    ]


def getMappingStatusList():
    return [
        "Direct",
        "Rename",
        "DirectHelper",
        "Helper",
        "RenameHelper",
        "Transformation",
        "WorkAround",
        "NotSupported",
    ]


def getHelperMappingStatusList():
    return ["DirectHelper", "Helper", "RenameHelper"]


def getSupportedStatus():
    return [True, False]


def get_import_status():
    return [ALL_KEY, THIRD_PARTY_LIB_KEY, BUILTIN_KEY, USER_DEFINED_KEY, UNKNOWN_KEY]


SMA_OUTPUT_FILES_NAMES = [
    "ImportUsagesInventory",
    "InputFilesInventory",
    "IOFilesInventory",
    "Issues",
    "JoinsInventory",
    "NotebookCellInventory",
    "NotebookSizeInventory",
    "SparkUsagesInventory",
    "SqlElementsInventory",
    "SqlFunctionsInventory",
    "ThirdPartyUsagesInventory",
]

SMA_OUTPUT_FILES_TABLES = [
    TABLE_IMPORT_USAGES_INVENTORY,
    TABLE_INPUT_FILES_INVENTORY,
    TABLE_IO_FILES_INVENTORY,
    TABLE_ISSUES_INVENTORY,
    TABLE_JOINS_INVENTORY,
    TABLE_NOTEBOOK_CELLS_INVENTORY,
    TABLE_NOTEBOOK_SIZE_INVENTORY,
    TABLE_SPARK_USAGES_INVENTORY,
    TABLE_SQL_ELEMENTS_INVENTORY,
    TABLE_SQL_FUNCTIONS_INVENTORY,
    TABLE_THIRD_PARTY_USAGES_INVENTORY,
]

DOC_URL = "https://docs.snowconvert.com/sma/issue-analysis/workarounds"

MAPPINGS_FEEDBACK_URL = "https://github.com/Snowflake-Labs/IAA-Support/issues/new?assignees=&labels=Issue&projects=&template=iaa-issue.md&title=%5BMake+sure+the+title+matches+the+mapping+name%5D"

class Applications:
    IAA = "IAA"
    WE = "WE"