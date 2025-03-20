--- Suggested DDL ---

CREATE SCHEMA IF NOT EXISTS "SNOW_DB"."SNOW_SCHEMA"
WITH MANAGED ACCESS;

CREATE OR REPLACE FILE FORMAT "SNOW_DB"."SNOW_SCHEMA"."DOCX_FORMAT" 
TYPE = 'CUSTOM';

COMMENT ON FILE FORMAT "SNOW_DB"."SNOW_SCHEMA"."DOCX_FORMAT" IS '';

CREATE OR REPLACE FILE FORMAT "SNOW_DB"."SNOW_SCHEMA"."JSON" 
TYPE = 'JSON'
TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24-MI-SS';

COMMENT ON FILE FORMAT "SNOW_DB"."SNOW_SCHEMA"."JSON" IS '';

CREATE STAGE IF NOT EXISTS "SNOW_DB"."SNOW_SCHEMA"."APP_STAGE"

DIRECTORY = ( ENABLE = TRUE );

CREATE STAGE IF NOT EXISTS "SNOW_DB"."SNOW_SCHEMA"."SMA_EXECUTIONS"

DIRECTORY = ( ENABLE = TRUE );

CREATE STAGE IF NOT EXISTS "SNOW_DB"."SNOW_SCHEMA"."SMA_MAPPINGS"

DIRECTORY = ( ENABLE = TRUE );

CREATE SEQUENCE IF NOT EXISTS "SNOW_DB"."SNOW_SCHEMA"."APP_LOG_SEQUENCE"
START = 1
INCREMENT = 1;

CREATE SEQUENCE IF NOT EXISTS "SNOW_DB"."SNOW_SCHEMA"."AUTOINCREMENT_SEQUENCE"
START = 1
INCREMENT = 1;

CREATE OR REPLACE FUNCTION "SNOW_DB"."SNOW_SCHEMA"."GET_VERSION" (
      "TOOL_VERSION" VARCHAR(255)
)
RETURNS VARCHAR(255)
LANGUAGE SQL
AS 'concat(split_part(tool_version,''.'',1),
  ''.'',
  lpad(split_part(tool_version,''.'',2),2,''0''),
  ''.'', 
  lpad(split_part(tool_version,''.'',3),3,''0''))';

COMMENT ON FUNCTION "SNOW_DB"."SNOW_SCHEMA"."GET_VERSION"(VARCHAR) IS '';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."GET_IMPORTS_MAPPINGS" (
)
RETURNS TABLE (
      "PACKAGE_NAME" VARCHAR(16777216)
    , "VERSIONS" VARCHAR(16777216)
    , "STATUS" VARCHAR(16777216)
    , "SUPPORTED" BOOLEAN
)
LANGUAGE python
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS 'import snowflake.snowpark as snowpark
import os
import sys
import sysconfig
from pydoc import ModuleScanner
from snowflake.snowpark.functions import  col, lit, iff, lower, listagg

        
def main(session: snowpark.Session): 
      


    def load_standard_library_modules():
        std_lib_dir = sysconfig.get_paths()["stdlib"]
        modules = []
        for f in os.listdir(std_lib_dir):
          if f.endswith(".py") and not f.startswith("_"):
            modules.append(f[:-3])

        return modules

    def load_builtin_modules():
        modules = []
        for x in sys.builtin_module_names:
          modules.append(x[1:])

        return modules
          
    def load_additional_modules():
        modules = []
        def callback(path, modname, desc, modules=modules):
          if modname and modname[-9:] == ''.__init__'':
            modname = modname[:-9] + '' (package)''
          if modname.find(''.'') < 0:
            modules.append(modname)
        def onerror(modname):
          callback(None, modname, None)


        ModuleScanner().run(callback, onerror=onerror)
        return modules

    
    standard_library_modules = load_standard_library_modules()
    builtins_modules = load_builtin_modules()
    additional_modules = load_additional_modules()
    all_modules = set(standard_library_modules + builtins_modules + additional_modules)
    dfSupported = session.table(["snowpark_analysis","information_schema","packages"]).where(col("language") == lit("python")).select("PACKAGE_NAME", "VERSION")
    dfSupported = dfSupported.group_by(col("PACKAGE_NAME")).agg(listagg(col("VERSION"), ",", is_distinct= True).alias("VERSIONS"))
    dfSupported = dfSupported.withColumn("STATUS", lit("Supported")).withColumn("SUPPORTED", lit(True))      
    dfBase = session.create_dataframe(list(all_modules), schema = ["PACKAGE_NAME"])
    dfBase = dfBase.withColumn("PACKAGE_NAME", lower(col("PACKAGE_NAME"))) \\
                        .withColumn("VERSIONS", lit(None))\\
                        .withColumn("STATUS", lit("Base"))\\
                        .withColumn("SUPPORTED", lit(True))

    dfAdditionalBase = dfBase.filter(dfBase["PACKAGE_NAME"].isin(dfSupported.select("PACKAGE_NAME"))).select("PACKAGE_NAME")
    dfBase = dfBase.where(~col("PACKAGE_NAME").isin(dfAdditionalBase))
    dfSupported = dfSupported.select("PACKAGE_NAME","VERSIONS", iff(col("PACKAGE_NAME").isin(dfAdditionalBase), lit("Base"), col("STATUS")).alias("STATUS"), "SUPPORTED" )
    df = dfSupported.union(dfBase)
    df.show()

    return df';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."GET_IMPORTS_MAPPINGS"() IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_APP_LOG" (
      "APPLICATION" VARCHAR(16777216)
    , "MESSAGE" VARCHAR(16777216)
    , "CUSTOMER_EMAIL" VARCHAR(16777216)
    , "EXECUTION_ID" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS 'BEGIN
  INSERT INTO APP_LOG
    (APPLICATION, MESSAGE, CUSTOMER_EMAIL, EXECUTION_ID)
  VALUES
    (:APPLICATION, :MESSAGE, :CUSTOMER_EMAIL, :EXECUTION_ID);

RETURN ''APP_LOG FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_APP_LOG"(VARCHAR,VARCHAR,VARCHAR,VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_COMPUTED_DEPENDENCIES" (
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS 'DECLARE
    TABLE_NAME varchar DEFAULT concat(''STREAM_DATA_COMPUTED_DEPENDENCIES_'' , DATE_PART(epoch_second, CURRENT_TIMESTAMP()));

BEGIN
    CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:TABLE_NAME) (
    EXECUTION_ID VARCHAR(16777216)
    );

    INSERT INTO IDENTIFIER(:TABLE_NAME) SELECT EXECUTION_ID FROM EXECUTION_INFO_STREAM;

  CALL PRE_CALC_DEPENDENCY_ANALYSIS();  
  RETURN ''INSERT_COMPUTED_DEPENDENCIES FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_COMPUTED_DEPENDENCIES"() IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_JAVA_BUILTINS" (
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS 'BEGIN
  TRUNCATE TABLE IF EXISTS JAVA_BUILTINS;

  INSERT INTO JAVA_BUILTINS
  VALUES
  (''com.sun.jarsigner''),
  (''com.sun.java.accessibility.util''),
  (''com.sun.javadoc''),
  (''com.sun.jdi''),
  (''com.sun.jdi.connect''),
  (''com.sun.jdi.connect.spi''),
  (''com.sun.jdi.event''),
  (''com.sun.jdi.request''),
  (''com.sun.management''),
  (''com.sun.net.httpserver''),
  (''com.sun.net.httpserver.spi''),
  (''com.sun.nio.sctp''),
  (''com.sun.security.auth''),
  (''com.sun.security.auth.callback''),
  (''com.sun.security.auth.login''),
  (''com.sun.security.auth.module''),
  (''com.sun.security.jgss''),
  (''com.sun.source.doctree''),
  (''com.sun.source.tree''),
  (''com.sun.source.util''),
  (''com.sun.tools.attach''),
  (''com.sun.tools.attach.spi''),
  (''com.sun.tools.javac''),
  (''com.sun.tools.javadoc''),
  (''com.sun.tools.jconsole''),
  (''java.applet''),
  (''java.awt''),
  (''java.awt.color''),
  (''java.awt.datatransfer''),
  (''java.awt.desktop''),
  (''java.awt.dnd''),
  (''java.awt.event''),
  (''java.awt.font''),
  (''java.awt.geom''),
  (''java.awt.im''),
  (''java.awt.im.spi''),
  (''java.awt.image''),
  (''java.awt.image.renderable''),
  (''java.awt.print''),
  (''java.beans''),
  (''java.beans.beancontext''),
  (''java.io''),
  (''java.lang''),
  (''java.lang.annotation''),
  (''java.lang.instrument''),
  (''java.lang.invoke''),
  (''java.lang.management''),
  (''java.lang.module''),
  (''java.lang.ref''),
  (''java.lang.reflect''),
  (''java.math''),
  (''java.net''),
  (''java.net.http''),
  (''java.net.spi''),
  (''java.nio''),
  (''java.nio.channels''),
  (''java.nio.channels.spi''),
  (''java.nio.charset''),
  (''java.nio.charset.spi''),
  (''java.nio.file''),
  (''java.nio.file.attribute''),
  (''java.nio.file.spi''),
  (''java.rmi''),
  (''java.rmi.activation''),
  (''java.rmi.dgc''),
  (''java.rmi.registry''),
  (''java.rmi.server''),
  (''java.security''),
  (''java.security.acl''),
  (''java.security.cert''),
  (''java.security.interfaces''),
  (''java.security.spec''),
  (''java.sql''),
  (''java.text''),
  (''java.text.spi''),
  (''java.time''),
  (''java.time.chrono''),
  (''java.time.format''),
  (''java.time.temporal''),
  (''java.time.zone''),
  (''java.util''),
  (''java.util.concurrent''),
  (''java.util.concurrent.atomic''),
  (''java.util.concurrent.locks''),
  (''java.util.function''),
  (''java.util.jar''),
  (''java.util.logging''),
  (''java.util.prefs''),
  (''java.util.regex''),
  (''java.util.spi''),
  (''java.util.stream''),
  (''java.util.zip''),
  (''javax.accessibility''),
  (''javax.annotation.processing''),
  (''javax.crypto''),
  (''javax.crypto.interfaces''),
  (''javax.crypto.spec''),
  (''javax.imageio''),
  (''javax.imageio.event''),
  (''javax.imageio.metadata''),
  (''javax.imageio.plugins.bmp''),
  (''javax.imageio.plugins.jpeg''),
  (''javax.imageio.plugins.tiff''),
  (''javax.imageio.spi''),
  (''javax.imageio.stream''),
  (''javax.lang.model''),
  (''javax.lang.model.element''),
  (''javax.lang.model.type''),
  (''javax.lang.model.util''),
  (''javax.management''),
  (''javax.management.loading''),
  (''javax.management.modelmbean''),
  (''javax.management.monitor''),
  (''javax.management.openmbean''),
  (''javax.management.relation''),
  (''javax.management.remote''),
  (''javax.management.remote.rmi''),
  (''javax.management.timer''),
  (''javax.naming''),
  (''javax.naming.directory''),
  (''javax.naming.event''),
  (''javax.naming.ldap''),
  (''javax.naming.spi''),
  (''javax.net''),
  (''javax.net.ssl''),
  (''javax.print''),
  (''javax.print.attribute''),
  (''javax.print.attribute.standard''),
  (''javax.print.event''),
  (''javax.rmi.ssl''),
  (''javax.script''),
  (''javax.security.auth''),
  (''javax.security.auth.callback''),
  (''javax.security.auth.kerberos''),
  (''javax.security.auth.login''),
  (''javax.security.auth.spi''),
  (''javax.security.auth.x500''),
  (''javax.security.cert''),
  (''javax.security.sasl''),
  (''javax.smartcardio''),
  (''javax.sound.midi''),
  (''javax.sound.midi.spi''),
  (''javax.sound.sampled''),
  (''javax.sound.sampled.spi''),
  (''javax.sql''),
  (''javax.sql.rowset''),
  (''javax.sql.rowset.serial''),
  (''javax.sql.rowset.spi''),
  (''javax.swing''),
  (''javax.swing.border''),
  (''javax.swing.colorchooser''),
  (''javax.swing.event''),
  (''javax.swing.filechooser''),
  (''javax.swing.plaf''),
  (''javax.swing.plaf.basic''),
  (''javax.swing.plaf.metal''),
  (''javax.swing.plaf.multi''),
  (''javax.swing.plaf.nimbus''),
  (''javax.swing.plaf.synth''),
  (''javax.swing.table''),
  (''javax.swing.text''),
  (''javax.swing.text.html''),
  (''javax.swing.text.html.parser''),
  (''javax.swing.text.rtf''),
  (''javax.swing.tree''),
  (''javax.swing.undo''),
  (''javax.tools''),
  (''javax.transaction.xa''),
  (''javax.xml''),
  (''javax.xml.catalog''),
  (''javax.xml.crypto''),
  (''javax.xml.crypto.dom''),
  (''javax.xml.crypto.dsig''),
  (''javax.xml.crypto.dsig.dom''),
  (''javax.xml.crypto.dsig.keyinfo''),
  (''javax.xml.crypto.dsig.spec''),
  (''javax.xml.datatype''),
  (''javax.xml.namespace''),
  (''javax.xml.parsers''),
  (''javax.xml.stream''),
  (''javax.xml.stream.events''),
  (''javax.xml.stream.util''),
  (''javax.xml.transform''),
  (''javax.xml.transform.dom''),
  (''javax.xml.transform.sax''),
  (''javax.xml.transform.stax''),
  (''javax.xml.transform.stream''),
  (''javax.xml.validation''),
  (''javax.xml.xpath''),
  (''jdk.dynalink''),
  (''jdk.dynalink.beans''),
  (''jdk.dynalink.linker''),
  (''jdk.dynalink.linker.support''),
  (''jdk.dynalink.support''),
  (''jdk.javadoc.doclet''),
  (''jdk.jfr''),
  (''jdk.jfr.consumer''),
  (''jdk.jshell''),
  (''jdk.jshell.execution''),
  (''jdk.jshell.spi''),
  (''jdk.jshell.tool''),
  (''jdk.management.jfr''),
  (''jdk.nashorn.api.scripting''),
  (''jdk.nashorn.api.tree''),
  (''jdk.net''),
  (''jdk.nio''),
  (''jdk.security.jarsigner''),
  (''netscape.javascript''),
  (''org.ietf.jgss''),
  (''org.w3c.dom''),
  (''org.w3c.dom.bootstrap''),
  (''org.w3c.dom.css''),
  (''org.w3c.dom.events''),
  (''org.w3c.dom.html''),
  (''org.w3c.dom.ls''),
  (''org.w3c.dom.ranges''),
  (''org.w3c.dom.stylesheets''),
  (''org.w3c.dom.traversal''),
  (''org.w3c.dom.views''),
  (''org.w3c.dom.xpath''),
  (''org.xml.sax''),
  (''org.xml.sax.ext''),
  (''org.xml.sax.helpers'');

END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_JAVA_BUILTINS"() IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_EWI_CATALOG" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
EWI_FOLDER := ''EWI'';
EWI_INVENTORY := ''EwiCatalog.json'';

BEGIN

-- INSERT INTO EWI

INSERT INTO MAPPINGS_CORE_EWI_CATALOG(
    VERSION,
    EWI_CODE,
    ELEMENT,
    CATEGORY,
    DEPRECATED_VERSION,
    SHORT_DESCRIPTION,
    GENERAL_DESCRIPTION,
    LONG_DESCRIPTION)

    SELECT
    GET_VERSION(VERSION),
    JSON_VALUES:EWI_CODE,
    JSON_VALUES:ELEMENT,
    JSON_VALUES:CATEGORY,
    JSON_VALUES:DEPRECATED_VERSION,
    JSON_VALUES:SHORT_DESCRIPTION,
    JSON_VALUES:GENERAL_DESCRIPTION,
    JSON_VALUES:LONG_DESCRIPTION
    FROM (
    SELECT VERSION, FILE_CONTENT_JSON.VALUE AS JSON_VALUES
    FROM (
    SELECT VERSION, FILE_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME = :EWI_INVENTORY AND FOLDER = :EWI_FOLDER
    ), TABLE(FLATTEN(FILE_CONTENT)) FILE_CONTENT_JSON
    );

RETURN ''INSERT_MAPPINGS_EWI_CATALOG FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_EWI_CATALOG"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_LIBRARY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
LIBRARY_FOLDER := ''Library'';
LIBRARY_INVENTORY := ''ConversionStatusLibraries.json'';

BEGIN

-- INSERT INTO LIBRARY

INSERT INTO MAPPINGS_CORE_LIBRARIES(
    VERSION,
    SOURCE_LIBRARY_NAME,
    LIBRARY_PREFIX,
    ORIGIN,
    SUPPORTED,
    STATUS,
    TARGET_LIBRARY_NAME)

    SELECT
    GET_VERSION(VERSION),
    JSON_VALUES:SOURCE_LIBRARY_NAME,
    JSON_VALUES:LIBRARY_PREFIX,
    JSON_VALUES:ORIGIN,
    JSON_VALUES:SUPPORTED,
    JSON_VALUES:STATUS,
    JSON_VALUES:TARGET_LIBRARY_NAME
    FROM (
    SELECT VERSION, FILE_CONTENT_JSON.VALUE AS JSON_VALUES
    FROM (
    SELECT VERSION, FILE_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME = :LIBRARY_INVENTORY AND FOLDER = :LIBRARY_FOLDER
    ), TABLE(FLATTEN(FILE_CONTENT)) FILE_CONTENT_JSON
    );

RETURN ''INSERT_MAPPINGS_LIBRARY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_LIBRARY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_PANDAS" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
PANDAS_FOLDER := ''Pandas'';
PANDAS_INVENTORY := ''ConversionStatusPyPandas.json'';

BEGIN

-- INSERT INTO MAPPINGS_PANDAS

INSERT INTO MAPPINGS_CORE_PANDAS(
    VERSION,
    CATEGORY,
    SPARK_FULLY_QUALIFIED_NAME,
    SPARK_NAME,
    SPARK_CLASS,
    SPARK_DEF,
    SNOWPARK_FULLY_QUALIFIED_NAME,
    SNOWPARK_NAME,
    SNOWPARK_CLASS,
    SNOWPARK_DEF,
    TOOL_SUPPORTED,
    SNOWFLAKE_SUPPORTED,
    MAPPING_STATUS,
    WORKAROUND_COMMENT,
    EWICode)

    SELECT
    GET_VERSION(VERSION),
    JSON_VALUES:CATEGORY,
    JSON_VALUES:SPARK_FULLY_QUALIFIED_NAME,
    JSON_VALUES:SPARK_NAME,
    JSON_VALUES:SPARK_CLASS,
    JSON_VALUES:SPARK_DEF,
    JSON_VALUES:SNOWPARK_FULLY_QUALIFIED_NAME,
    JSON_VALUES:SNOWPARK_NAME,
    JSON_VALUES:SNOWPARK_CLASS,
    JSON_VALUES:SNOWPARK_DEF,
    JSON_VALUES:TOOL_SUPPORTED,
    JSON_VALUES:SNOWFLAKE_SUPPORTED,
    JSON_VALUES:MAPPING_STATUS,
    JSON_VALUES:WORKAROUND_COMMENT,
    JSON_VALUES:EWICode
    FROM (
    SELECT VERSION, FILE_CONTENT_JSON.VALUE AS JSON_VALUES
    FROM (
    SELECT VERSION, FILE_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME = :PANDAS_INVENTORY AND FOLDER = :PANDAS_FOLDER
    ), TABLE(FLATTEN(FILE_CONTENT)) FILE_CONTENT_JSON
    );
RETURN ''INSERT_MAPPINGS_PANDAS FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_PANDAS"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_PYSPARK" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE

PYTHON_FOLDER := ''Python'';
CONVERSION_INVENTORY := ''ConversionStatusPySpark.json'';

BEGIN

-- INSERT INTO PYSPARK CONVERSION STATUS

INSERT INTO MAPPINGS_CORE_PYSPARK(
    VERSION,
    CATEGORY,
    SPARK_FULLY_QUALIFIED_NAME,
    SPARK_NAME,
    SPARK_CLASS,
    SPARK_DEF,
    SNOWPARK_FULLY_QUALIFIED_NAME,
    SNOWPARK_NAME,
    SNOWPARK_CLASS,
    SNOWPARK_DEF,
    TOOL_SUPPORTED,
    SNOWFLAKE_SUPPORTED,
    MAPPING_STATUS,
    WORKAROUND_COMMENT,
    EWICode)

    SELECT
    GET_VERSION(VERSION),
    JSON_VALUES:CATEGORY,
    JSON_VALUES:SPARK_FULLY_QUALIFIED_NAME,
    JSON_VALUES:SPARK_NAME,
    JSON_VALUES:SPARK_CLASS,
    JSON_VALUES:SPARK_DEF,
    JSON_VALUES:SNOWPARK_FULLY_QUALIFIED_NAME,
    JSON_VALUES:SNOWPARK_NAME,
    JSON_VALUES:SNOWPARK_CLASS,
    JSON_VALUES:SNOWPARK_DEF,
    JSON_VALUES:TOOL_SUPPORTED,
    JSON_VALUES:SNOWFLAKE_SUPPORTED,
    JSON_VALUES:MAPPING_STATUS,
    JSON_VALUES:WORKAROUND_COMMENT,
    JSON_VALUES:EWICode
    FROM (
    SELECT VERSION, FILE_CONTENT_JSON.VALUE AS JSON_VALUES
    FROM (
    SELECT VERSION, FILE_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME = :CONVERSION_INVENTORY AND FOLDER = :PYTHON_FOLDER
    ), TABLE(FLATTEN(FILE_CONTENT)) FILE_CONTENT_JSON
    );

RETURN ''INSERT_MAPPINGS_PYSPARK FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_PYSPARK"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_DATAFRAMES_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
   BACKSLASH_TOKEN := ''\\\\'';
   SLASH_TOKEN := ''/'';
   DATAFRAMES_INVENTORY_NAME := ''DataFramesInventory%.json'';

BEGIN

    INSERT INTO DATAFRAMES_INVENTORY(
    EXECUTION_ID,
    FULLNAME,
    NAME,
    FILE_ID,
    CELL_ID,
    LINE,
    "COLUMN",
    ASSIGNMENT_NUMBER,
    RELEVANT_FUNCTION,
    RELATED_DATAFRAMES,
    ENTRY_POINTS
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:FullName,      
    INVENTORY_CONTENT:Name,      
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:CellId,
    INVENTORY_CONTENT:Line,
    INVENTORY_CONTENT:Column,
    INVENTORY_CONTENT:AssignmentNumber,
    INVENTORY_CONTENT:RelevantFunction,
    INVENTORY_CONTENT:RelatedDataFrames,
    INVENTORY_CONTENT:EntryPoints
    FROM (
      SELECT 
          EXECUTION_ID,
          INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
      FROM (
        SELECT 
          EXECUTION_ID, INVENTORY_CONTENT
        FROM IDENTIFIER(:TABLE_NAME)
        WHERE FILE_NAME ILIKE :DATAFRAMES_INVENTORY_NAME
      ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_DATAFRAMES_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_DATAFRAMES_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_DATA_TO_IAA_TABLES" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
BEGIN
  BEGIN TRANSACTION;
    CALL MERGE_EXECUTION_INFO(:table_name);
    CALL MERGE_INPUT_FILE_INVENTORY(:table_name);
    CALL MERGE_IMPORT_USAGES_INVENTORY(:table_name);
    CALL MERGE_IO_FILES_INVENTORY(:table_name);
    CALL MERGE_ISSUES_INVENTORY(:table_name);
    CALL MERGE_JOINS_INVENTORY(:table_name);
    CALL MERGE_NOTEBOOK_CELLS_INVENTORY(:table_name);
    CALL MERGE_NOTEBOOK_SIZE_INVENTORY(:table_name);
    CALL MERGE_PACKAGE_INVENTORY(:table_name);
    CALL MERGE_PACKAGE_VERSIONS_INVENTORY(:table_name);
    CALL MERGE_PANDAS_USAGES_INVENTORY(:table_name);
    CALL MERGE_SPARK_USAGES_INVENTORY(:table_name);
    CALL MERGE_SQL_ELEMENTS_INVENTORY(:table_name);
    CALL MERGE_SQL_EMBEDDED_USAGES_INVENTORY(:table_name);
    CALL MERGE_SQL_FUNCTIONS_INVENTORY(:table_name);
    CALL MERGE_THIRD_PARTY_USAGES_INVENTORY(:table_name);
    CALL MERGE_ARTIFACTS_DEPENDENCY_INVENTORY(:table_name);
    CALL MERGE_DATAFRAMES_INVENTORY(:table_name);
  COMMIT;
  EXCEPTION
    WHEN STATEMENT_ERROR THEN
      RETURN OBJECT_CONSTRUCT(''Error type'', ''STATEMENT_ERROR'',
                          ''SQLCODE'', SQLCODE,
                          ''SQLERRM'', SQLERRM,
                          ''SQLSTATE'', SQLSTATE);

  RETURN ''MERGE_DATA_TO_IAA_TABLES FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_DATA_TO_IAA_TABLES"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_EXECUTION_INFO" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
EXECUTION_INFO_INVENTORY_NAME := ''ExecutionInfo%.json'';

BEGIN

    INSERT INTO EXECUTION_INFO(
    EXECUTION_ID,
    EXECUTION_TIMESTAMP,
    CUSTOMER_LICENSE,
    TOOL_VERSION,
    TOOL_NAME,
    MARGIN_ERROR,
    CLIENT_EMAIL,
    MAPPING_VERSION,
    CONVERSION_SCORE,
    SESSION_ID,
    COMPANY,
    PROJECT_NAME,
    PARSING_SCORE,
    SPARK_API_READINESS_SCORE,
    PYTHON_READINESS_SCORE,
    SCALA_READINESS_SCORE,
    SQL_READINESS_SCORE,
    THIRD_PARTY_READINESS_SCORE
    )
    SELECT
    EXECUTION_ID,
    TO_TIMESTAMP_NTZ(EXTRACT_DATE),
    INVENTORY_CONTENT:CUSTOMER_LICENSE,
    GET_VERSION(INVENTORY_CONTENT:TOOL_VERSION),
    INVENTORY_CONTENT:TOOL_NAME,
    INVENTORY_CONTENT:MARGIN_ERROR,
    INVENTORY_CONTENT:CLIENT_EMAIL,
    GET_VERSION(INVENTORY_CONTENT:MAPPING_VERSION),
    INVENTORY_CONTENT:CONVERSION_SCORE,
    INVENTORY_CONTENT:SESSION_ID,
    INVENTORY_CONTENT:COMPANY,
    INVENTORY_CONTENT:PROJECT_NAME,
    INVENTORY_CONTENT:PARSING_SCORE,
    INVENTORY_CONTENT:READINESS_SCORE,
    INVENTORY_CONTENT:PYTHON_READINESS_SCORE,
    INVENTORY_CONTENT:SCALA_READINESS_SCORE,
    INVENTORY_CONTENT:SQL_READINESS_SCORE,
    INVENTORY_CONTENT:THIRD_PARTY_READINESS_SCORE
    FROM (
    SELECT EXECUTION_ID, EXTRACT_DATE, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, EXTRACT_DATE, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :EXECUTION_INFO_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_EXECUTION_INFO FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_EXECUTION_INFO"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_IMPORT_USAGES_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
   BACKSLASH_TOKEN := ''\\\\'';
   SLASH_TOKEN := ''/'';
   IMPORT_USAGES_INVENTORY_NAME := ''ImportUsagesInventory%.json'';

BEGIN

    INSERT INTO IMPORT_USAGES_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    ALIAS,
    KIND,
    LINE,
    PACKAGE_NAME,
    STATUS,
    IS_SNOWPARK_ANACONDA_SUPPORTED,
    SNOWPARK_CORE_VERSION,
    SNOWPARK_VERSION,
    ELEMENT_PACKAGE,
    CELL_ID,
    ORIGIN
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:Alias,
    INVENTORY_CONTENT:Kind,
    CASE
    WHEN INVENTORY_CONTENT:Line = '''' THEN NULL
    ELSE INVENTORY_CONTENT:Line
    END,
    INVENTORY_CONTENT:PackageName,
    INVENTORY_CONTENT:Status,
    INVENTORY_CONTENT:IsSnowparkAnacondaSupported,
    GET_VERSION(INVENTORY_CONTENT:SnowConvertCoreVersion),
    GET_VERSION(INVENTORY_CONTENT:SnowparkVersion),
    INVENTORY_CONTENT:ElementPackage,
    CASE
    WHEN INVENTORY_CONTENT:CellId = '''' THEN NULL
    ELSE INVENTORY_CONTENT:CellId
    END,
    INVENTORY_CONTENT:Origin
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :IMPORT_USAGES_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_IMPORT_USAGES_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_IMPORT_USAGES_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_INPUT_FILE_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    INPUT_FILES_INVENTORY_NAME := ''InputFilesInventory%.json'';

BEGIN

   INSERT INTO INPUT_FILES_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    EXTENSION,
    BYTES,
    CHARACTER_LENGTH,
    LINES_OF_CODE,
    PARSE_RESULT,
    TECHNOLOGY,
    IGNORED,
    ORIGIN_FILE_PATH
    )
    SELECT
    EXECUTION_ID,
    REPLACE(INVENTORY_CONTENT:Element, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:Extension,
    INVENTORY_CONTENT:Bytes,
    INVENTORY_CONTENT:CharacterLength,
    INVENTORY_CONTENT:LinesOfCode,
    INVENTORY_CONTENT:ParseResult,
    INVENTORY_CONTENT:Technology,
    INVENTORY_CONTENT:Ignored,
    CASE
    WHEN INVENTORY_CONTENT:OriginFilePath = '''' THEN NULL
    ELSE REPLACE(INVENTORY_CONTENT:OriginFilePath, :BACKSLASH_TOKEN, :SLASH_TOKEN)
    END,
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :INPUT_FILES_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_INPUT_FILES_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_INPUT_FILE_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_IO_FILES_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    IO_FILES_INVENTORY_NAME := ''IOFilesInventory%.json'';

BEGIN

    INSERT INTO IO_FILES_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    IS_LITERAL,
    FORMAT,
    FORMAT_TYPE,
    MODE,
    SUPPORTED,
    LINE,
    OPTION_SETTINGS,
    CELL_ID
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:IsLiteral,
    INVENTORY_CONTENT:Format,
    INVENTORY_CONTENT:FormatType,
    INVENTORY_CONTENT:Mode,
    INVENTORY_CONTENT:Supported,
    CASE
    WHEN INVENTORY_CONTENT:Line = '''' THEN NULL
    ELSE INVENTORY_CONTENT:Line
    END,
    INVENTORY_CONTENT:OptionSettings,
    CASE
    WHEN INVENTORY_CONTENT:CellId = '''' THEN NULL
    ELSE INVENTORY_CONTENT:CellId
    END
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :IO_FILES_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_IO_FILES_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_IO_FILES_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_ISSUES_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    ISSUES_ARGUMENTS_SEPARATOR_TOKEN := ''�'';
    ISSUES_INVENTORY_NAME := ''notifications%.json'';

BEGIN

    INSERT INTO ISSUES_INVENTORY(
    EXECUTION_ID,
    CODE,
    DESCRIPTION,
    FILE_ID,
    PROJECT_ID,
    LINE,
    "COLUMN"
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Code,
    SPLIT(INVENTORY_CONTENT:NotificationArgs, :ISSUES_ARGUMENTS_SEPARATOR_TOKEN)[0],
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:ProjectId,
    CASE
    WHEN INVENTORY_CONTENT:Line = '''' THEN NULL
    ELSE INVENTORY_CONTENT:Line
    END,
    CASE
    WHEN INVENTORY_CONTENT:Column = '''' THEN NULL
    ELSE INVENTORY_CONTENT:Column
    END
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :ISSUES_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_ISSUES_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_ISSUES_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_SPARK" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE

SCALA_FOLDER := ''Scala'';
CONVERSION_INVENTORY := ''ConversionStatusSclSpark.json'';

BEGIN

-- INSERT INTO SPARK CONVERSION STATUS

INSERT INTO MAPPINGS_CORE_SPARK(
VERSION,
CATEGORY,
SPARK_FULLY_QUALIFIED_NAME,
SPARK_NAME,
SPARK_CLASS,
SPARK_DEF,
SNOWPARK_FULLY_QUALIFIED_NAME,
SNOWPARK_NAME,
SNOWPARK_CLASS,
SNOWPARK_DEF,
TOOL_SUPPORTED,
SNOWFLAKE_SUPPORTED,
MAPPING_STATUS,
WORKAROUND_COMMENT,
EWICode)

SELECT
GET_VERSION(VERSION),
JSON_VALUES:CATEGORY,
JSON_VALUES:SPARK_FULLY_QUALIFIED_NAME,
JSON_VALUES:SPARK_NAME,
JSON_VALUES:SPARK_CLASS,
JSON_VALUES:SPARK_DEF,
JSON_VALUES:SNOWPARK_FULLY_QUALIFIED_NAME,
JSON_VALUES:SNOWPARK_NAME,
JSON_VALUES:SNOWPARK_CLASS,
JSON_VALUES:SNOWPARK_DEF,
JSON_VALUES:TOOL_SUPPORTED,
JSON_VALUES:SNOWFLAKE_SUPPORTED,
JSON_VALUES:MAPPING_STATUS,
JSON_VALUES:WORKAROUND_COMMENT,
JSON_VALUES:EWICode
FROM (
SELECT VERSION, FILE_CONTENT_JSON.VALUE AS JSON_VALUES
FROM (
SELECT VERSION, FILE_CONTENT
FROM IDENTIFIER(:TABLE_NAME)
WHERE FILE_NAME = :CONVERSION_INVENTORY AND FOLDER = :SCALA_FOLDER
), TABLE(FLATTEN(FILE_CONTENT)) FILE_CONTENT_JSON
);

RETURN ''INSERT_MAPPINGS_SCALA FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_SPARK"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_SQL_ELEMENTS" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE

SQL_FOLDER := ''Sql'';
SQL_ELEMENTS_INVENTORY := ''SqlElementsInfo.json'';

BEGIN

-- INSERT INTO SQL ELEMENTS

INSERT INTO MAPPINGS_CORE_SQL_ELEMENTS(
    VERSION,
    ELEMENT,
    SOURCE_FULL_NAME,
    CATEGORY,
    FLAVOR,
    EWI,
    CONVERSION_STATUS,
    TARGET_ELEMENT,
    DEFAULT_MAPPING)

    SELECT
    GET_VERSION(VERSION),
    JSON_VALUES:ELEMENT,
    JSON_VALUES:SOURCE_FULL_NAME,
    JSON_VALUES:CATEGORY,
    JSON_VALUES:FLAVOR,
    JSON_VALUES:EWI,
    JSON_VALUES:CONVERSION_STATUS,
    JSON_VALUES:TARGET_ELEMENT,
    JSON_VALUES:DEFAULT_MAPPING
    FROM (
    SELECT VERSION, FILE_CONTENT_JSON.VALUE AS JSON_VALUES
    FROM (
    SELECT VERSION, FILE_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME = :SQL_ELEMENTS_INVENTORY AND FOLDER = :SQL_FOLDER
    ), TABLE(FLATTEN(FILE_CONTENT)) FILE_CONTENT_JSON
    );

RETURN ''INSERT_MAPPINGS_SQL_ELEMENTS FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_SQL_ELEMENTS"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_SQL_FUNCTIONS" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE

SQL_FOLDER := ''Sql'';
SQL_FUNCTIONS_INVENTORY := ''SqlFunctionsInfo.json'';

BEGIN

-- INSERT INTO SQL FUNCTIONS

INSERT INTO MAPPINGS_CORE_SQL_FUNCTIONS(
    VERSION,
    ELEMENT,
    FLAVOR,
    CATEGORY,
    MIGRATION_STATUS)

    SELECT
    GET_VERSION(VERSION),
    JSON_VALUES:Element,
    JSON_VALUES:Flavor,
    JSON_VALUES:Category,
    JSON_VALUES:MigrationStatus
    FROM (
    SELECT VERSION, FILE_CONTENT_JSON.VALUE AS JSON_VALUES
    FROM (
    SELECT VERSION, FILE_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME = :SQL_FUNCTIONS_INVENTORY AND FOLDER = :SQL_FOLDER
    ), TABLE(FLATTEN(FILE_CONTENT)) FILE_CONTENT_JSON
    );

RETURN ''INSERT_MAPPINGS_SQL_FUNCTIONS FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_MAPPINGS_SQL_FUNCTIONS"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_PIPE_DATA_TO_IAA_TABLES" (
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS 'DECLARE
    TABLE_NAME varchar DEFAULT concat(''STREAM_DATA_'' , DATE_PART(epoch_second, CURRENT_TIMESTAMP()));

BEGIN
    CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:TABLE_NAME) (
    EXECUTION_ID VARCHAR(16777216),
    EXTRACT_DATE TIMESTAMP_NTZ(9),
    FILE_NAME VARCHAR(16777216),
    INVENTORY_CONTENT VARIANT
    );

    INSERT INTO IDENTIFIER(:TABLE_NAME) SELECT EXECUTION_ID, EXTRACT_DATE, FILE_NAME, INVENTORY_CONTENT FROM ASSESSMENTS_FILES_STREAM as src 
    WHERE NOT EXISTS (SELECT EXECUTION_ID FROM EXECUTION_INFO WHERE EXECUTION_ID = src.EXECUTION_ID);

  CALL MERGE_DATA_TO_IAA_TABLES(:TABLE_NAME);  
  RETURN ''INSERT_PIPE_DATA_TO_IAA_TABLES FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_PIPE_DATA_TO_IAA_TABLES"() IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_THIRD_PARTY_CATEGORIES" (
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS 'BEGIN
  TRUNCATE TABLE IF EXISTS THIRD_PARTY_CATEGORIES;

  INSERT INTO THIRD_PARTY_CATEGORIES(CATEGORY, FILTER)
  VALUES
  (''dbutils'', ''dbutils.*'');
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."INSERT_THIRD_PARTY_CATEGORIES"() IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_ARTIFACTS_DEPENDENCY_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
   BACKSLASH_TOKEN := ''\\\\'';
   SLASH_TOKEN := ''/'';
   ARTIFACT_DEPENDENCY_INVENTORY_NAME := ''ArtifactDependencyInventory%.json'';

BEGIN

    INSERT INTO ARTIFACTS_DEPENDENCY_INVENTORY(
    EXECUTION_ID,
    FILE_ID,
    DEPENDENCY,
    TYPE,
    ARGUMENTS,      
    STATUS,
    LOCATION
    )
    SELECT
    EXECUTION_ID,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:DEPENDENCY,      
    INVENTORY_CONTENT:TYPE,
    INVENTORY_CONTENT:ARGUMENTS,
    INVENTORY_CONTENT:STATUS,
    INVENTORY_CONTENT:LOCATION
    FROM (
      SELECT 
          EXECUTION_ID,
          INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
      FROM (
        SELECT 
          EXECUTION_ID, INVENTORY_CONTENT
        FROM IDENTIFIER(:TABLE_NAME)
        WHERE FILE_NAME ILIKE :ARTIFACT_DEPENDENCY_INVENTORY_NAME
      ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_ARTIFACTS_DEPENDENCY_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_ARTIFACTS_DEPENDENCY_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_JOINS_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    JOINS_INVENTORY_NAME := ''JoinsInventory%.json'';

BEGIN

   INSERT INTO JOINS_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    IS_SELF_JOIN,
    HAS_LEFT_ALIAS,
    HAS_RIGHT_ALIAS,
    LINE,
    KIND,
    CELL_ID
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:IsSelfJoin,
    INVENTORY_CONTENT:HasLeftAlias,
    INVENTORY_CONTENT:HasRightAlias,
    CASE
    WHEN INVENTORY_CONTENT:Line = '''' THEN NULL
    ELSE INVENTORY_CONTENT:Line
    END,
    INVENTORY_CONTENT:Kind,
    CASE
    WHEN INVENTORY_CONTENT:CellId = '''' THEN NULL
    ELSE INVENTORY_CONTENT:CellId
    END,
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :JOINS_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_JOINS_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_JOINS_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_NOTEBOOK_CELLS_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    NOTEBOOK_CELLS_INVENTORY_NAME := ''NotebookCellsInventory%.json'';

BEGIN

    INSERT INTO NOTEBOOK_CELLS_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    ARGUMENTS,
    CELL_ID,
    LOC,
    SIZE,
    SUPPORTED_STATUS,
    PARSING_RESULT
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:Arguments,
    INVENTORY_CONTENT:CellID,
    INVENTORY_CONTENT:LOC,
    INVENTORY_CONTENT:Size,
    INVENTORY_CONTENT:SupportedStatus,
    INVENTORY_CONTENT:ParsingResult
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :NOTEBOOK_CELLS_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_NOTEBOOK_CELLS_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_NOTEBOOK_CELLS_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_NOTEBOOK_SIZE_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    NOTEBOOK_SIZE_INVENTORY_NAME := ''NotebookSizeInventory%.json'';

BEGIN

    INSERT INTO NOTEBOOK_SIZE_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    PYTHON_LOC,
    SCALA_LOC,
    SQL_LOC,
    LINE
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:PythonLOC,
    INVENTORY_CONTENT:ScalaLOC,
    INVENTORY_CONTENT:SqlLOC,
    CASE
    WHEN INVENTORY_CONTENT:Line = '''' THEN NULL
    ELSE INVENTORY_CONTENT:Line
    END,
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :NOTEBOOK_SIZE_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_NOTEBOOK_SIZE_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_NOTEBOOK_SIZE_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_PACKAGE_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    PACKAGES_INVENTORY_NAME := ''PackagesInventory%.json'';

BEGIN

    INSERT INTO PACKAGES_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :PACKAGES_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_PACKAGES_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_PACKAGE_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_PACKAGE_VERSIONS_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    PACKAGE_VERSIONS_NAME := ''PackageVersions%.json'';

BEGIN

    INSERT INTO PACKAGE_VERSIONS_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    VERSION
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    GET_VERSION(INVENTORY_CONTENT:Version)
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :PACKAGE_VERSIONS_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );


RETURN ''MERGE_PACKAGE_VERSIONS_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_PACKAGE_VERSIONS_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_PANDAS_USAGES_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    PANDAS_USAGES_INVENTORY_NAME := ''PandasUsagesInventory%.json'';

BEGIN

    INSERT INTO PANDAS_USAGES_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    ALIAS,
    KIND,
    LINE,
    PACKAGE_NAME,
    SUPPORTED,
    AUTOMATED,
    STATUS,
    SNOWCONVERT_CORE_VERSION,
    PANDAS_VERSION,
    CELL_ID,
    PARAMETERS_INFO
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:Alias,
    INVENTORY_CONTENT:Kind,
    CASE
    WHEN INVENTORY_CONTENT:Line = '''' THEN NULL
    ELSE INVENTORY_CONTENT:Line
    END,
    INVENTORY_CONTENT:PackageName,
    INVENTORY_CONTENT:Supported,
    INVENTORY_CONTENT:Automated,
    INVENTORY_CONTENT:Status,
    GET_VERSION(INVENTORY_CONTENT:SnowConvertCoreVersion),
    GET_VERSION(INVENTORY_CONTENT:PandasVersion),
    CASE
    WHEN INVENTORY_CONTENT:CellId = '''' THEN NULL
    ELSE INVENTORY_CONTENT:CellId
    END,
    INVENTORY_CONTENT:ParametersInfo,
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :PANDAS_USAGES_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_PANDAS_USAGES_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_PANDAS_USAGES_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_SPARK_USAGES_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    SPARK_USAGES_INVENTORY_NAME := ''SparkUsagesInventory%.json'';

BEGIN

    INSERT INTO SPARK_USAGES_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    ALIAS,
    KIND,
    LINE,
    PACKAGE_NAME,
    SUPPORTED,
    AUTOMATED,
    STATUS,
    SNOWCONVERT_CORE_VERSION,
    SNOWPARK_VERSION,
    CELL_ID,
    PARAMETERS_INFO
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:Alias,
    INVENTORY_CONTENT:Kind,
    CASE
    WHEN INVENTORY_CONTENT:Line = '''' THEN NULL
    ELSE INVENTORY_CONTENT:Line
    END,
    INVENTORY_CONTENT:PackageName,
    INVENTORY_CONTENT:Supported,
    INVENTORY_CONTENT:Automated,
    INVENTORY_CONTENT:Status,
    GET_VERSION(INVENTORY_CONTENT:SnowConvertCoreVersion),
    GET_VERSION(INVENTORY_CONTENT:SnowparkVersion),
    CASE
    WHEN INVENTORY_CONTENT:CellId = '''' THEN NULL
    ELSE INVENTORY_CONTENT:CellId
    END,
    INVENTORY_CONTENT:ParametersInfo,
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :SPARK_USAGES_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_SPARK_USAGES_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_SPARK_USAGES_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_SQL_ELEMENTS_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    SQL_ELEMENTS_INVENTORY_NAME := ''SqlElementsInventory%.json'';

BEGIN

    INSERT INTO SQL_ELEMENTS_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    NOTEBOOK_CELL_ID,
    LINE,
    "COLUMN",
    SQL_FLAVOR,
    ROOT_FULLNAME,
    ROOT_LINE,
    ROOT_COLUMN,
    TOP_LEVEL_FULLNAME,
    TOP_LEVEL_LINE,
    TOP_LEVEL_COLUMN,
    CONVERSION_STATUS,
    CATEGORY,
    EWI,
    OBJECT_REFERENCE
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:NotebookCellId,
    INVENTORY_CONTENT:Line,
    INVENTORY_CONTENT:Column,
    INVENTORY_CONTENT:SqlFlavor,
    INVENTORY_CONTENT:RootFullName,
    INVENTORY_CONTENT:RootLine,
    INVENTORY_CONTENT:RootColumn,
    INVENTORY_CONTENT:TopLevelFullName,
    INVENTORY_CONTENT:TopLevelLine,
    INVENTORY_CONTENT:TopLevelColumn,
    INVENTORY_CONTENT:ConversionStatus,
    INVENTORY_CONTENT:Category,
    INVENTORY_CONTENT:EWI,
    INVENTORY_CONTENT:ObjectReference
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :SQL_ELEMENTS_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_SQL_ELEMENTS_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_SQL_ELEMENTS_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_SQL_EMBEDDED_USAGES_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    SQL_EMBEDDED_USAGES_INVENTORY_NAME := ''SqlEmbeddedUsageInventory%.json'';

BEGIN

    INSERT INTO SQL_EMBEDDED_USAGES_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    LIBRARY_NAME,
    HAS_LITERAL,
    HAS_VARIABLE,
    HAS_FUNCTION,
    PARSING_STATUS,
    HAS_INTERPOLATION,
    CELL_ID,
    LINE,
    "COLUMN"
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:FileILibraryNamed,
    INVENTORY_CONTENT:HasLiteral,
    INVENTORY_CONTENT:HasVariable,
    INVENTORY_CONTENT:HasFunction,
    INVENTORY_CONTENT:ParsingStatus,
    INVENTORY_CONTENT:HasInterpolation,
    CASE
    WHEN INVENTORY_CONTENT:CellId = '''' THEN NULL
    ELSE INVENTORY_CONTENT:CellId
    END,
    INVENTORY_CONTENT:Line,
    INVENTORY_CONTENT:Column
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :SQL_EMBEDDED_USAGES_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_SQL_EMBEDDED_USAGES_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_SQL_EMBEDDED_USAGES_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_SQL_FUNCTIONS_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    SQL_FUNCTIONS_INVENTORY_NAME := ''SqlFunctionsInventory%.json'';

BEGIN

    INSERT INTO SQL_FUNCTIONS_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    CATEGORY,
    CELL_ID,
    LINE,
    "COLUMN"
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:Category,
    CASE
    WHEN INVENTORY_CONTENT:CellId = '''' THEN NULL
    ELSE INVENTORY_CONTENT:CellId
    END,
    INVENTORY_CONTENT:Line,
    INVENTORY_CONTENT:Column
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :SQL_FUNCTIONS_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_SQL_FUNCTIONS_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_SQL_FUNCTIONS_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_THIRD_PARTY_USAGES_INVENTORY" (
      "TABLE_NAME" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS '
DECLARE
    BACKSLASH_TOKEN := ''\\\\'';
    SLASH_TOKEN := ''/'';
    THIRD_PARTY_USAGES_INVENTORY_NAME := ''ThirdPartyUsagesInventory%.json'';

BEGIN

   INSERT INTO THIRD_PARTY_USAGES_INVENTORY(
    EXECUTION_ID,
    ELEMENT,
    PROJECT_ID,
    FILE_ID,
    COUNT,
    ALIAS,
    KIND,
    LINE,
    PACKAGE_NAME,
    CELL_ID,
    PARAMETERS_INFO
    )
    SELECT
    EXECUTION_ID,
    INVENTORY_CONTENT:Element,
    INVENTORY_CONTENT:ProjectId,
    REPLACE(INVENTORY_CONTENT:FileId, :BACKSLASH_TOKEN, :SLASH_TOKEN),
    INVENTORY_CONTENT:Count,
    INVENTORY_CONTENT:Alias,
    INVENTORY_CONTENT:Kind,
    INVENTORY_CONTENT:Line,
    INVENTORY_CONTENT:PackageName,
    CASE
    WHEN INVENTORY_CONTENT:CellId = '''' THEN NULL
    ELSE INVENTORY_CONTENT:CellId
    END,
    INVENTORY_CONTENT:ParametersInfo,
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT_JSON.VALUE AS INVENTORY_CONTENT
    FROM (
    SELECT EXECUTION_ID, INVENTORY_CONTENT
    FROM IDENTIFIER(:TABLE_NAME)
    WHERE FILE_NAME ILIKE :THIRD_PARTY_USAGES_INVENTORY_NAME
    ), TABLE(FLATTEN(INVENTORY_CONTENT)) INVENTORY_CONTENT_JSON
    );

RETURN ''MERGE_THIRD_PARTY_USAGES_INVENTORY FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."MERGE_THIRD_PARTY_USAGES_INVENTORY"(VARCHAR) IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."PRE_CALC_DEPENDENCY_ANALYSIS" (
)
RETURNS VARCHAR(16777216)
LANGUAGE python
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'modin==0.28.1')
HANDLER = 'main'
AS 'import re
import time
import traceback
import sys
import sysconfig
import os
import pandas as pd
import numpy as np
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, replace, iff, substring, lit, length
from datetime import datetime, timedelta
from snowflake.snowpark.types import StructType, StructField, StringType, ArrayType, BooleanType, TimestampType
from pydoc import ModuleScanner


def main(session: snowpark.Session):
    da = DependencyAnalysis(session)
    pd.session = session
    return da.pre_calc_dependency_analysis()


def add_log(log:[], message:str):
    print(message)
    log.append(message)
    return log


def register_pending_execution(self, execution_id: str, message: str):
    df = self.session.create_dataframe(
            data=[[
                execution_id,
                message,
                datetime.now()
            ]],
            schema=StructType([
                    StructField("EXECUTION_ID", StringType()),
                    StructField("MESSAGE", StringType()),
                    StructField("TIMESTAMP", TimestampType())]))
    df.write.mode(''append'').save_as_table(table_name=f"{self.db}.{self.schema}.PENDING_EXECUTION_LIST")


def clean_pending_list(df_pending_list, execution, session=snowpark.Session):
    try:
        if df_pending_list.filter(col("TOOL_EXECUTION_ID")== execution.TOOL_EXECUTION_ID).count() > 0:
            t = session.table("pending_execution_list")
            result = t.delete(t["EXECUTION_ID"] == execution.TOOL_EXECUTION_ID)
            return result
    except Exception as ex:
         return


def print_log(msn):
    now = datetime.now()
    ts = now.strftime("%m/%d/%Y, %H:%M:%S")
    print(f''{ts} - {msn}'')


class DependencyAnalysis:

    max_imports_to_process = 30000

    def __init__(self, session=snowpark.Session):
        self.session = session
        self.schema = self.session.connection.schema
        self.db = self.session.connection.database
        if len(self.session.sql(f"SHOW SCHEMAS LIKE ''{self.schema}''").collect()) == 0:
            raise Exception(f''INVALID SCHEMA {self.schema}'')
        self.schema_name = ".".join([self.db, self.schema])
        self.builtin_modules = DependencyAnalysis._get_builtin_modules()

    @staticmethod
    def _load_standard_library_modules():
        std_lib_dir = sysconfig.get_paths()["stdlib"]
        modules = []
        for f in os.listdir(std_lib_dir):
            if f.endswith(".py") and not f.startswith("_"):
                modules.append(f[:-3])

        return modules

    @staticmethod
    def _load_builtin_modules():
        modules = []
        for x in sys.builtin_module_names:
            modules.append(x[1:])

        return modules

    @staticmethod
    def _load_additional_modules():
        modules = []

        def callback(path, modname, desc, modules=modules):
            if modname and modname[-9:] == ''.__init__'':
                modname = modname[:-9] + '' (package)''
            if modname.find(''.'') < 0:
                modules.append(modname)

        def onerror(modname):
            callback(None, modname, None)

        ModuleScanner().run(callback, onerror=onerror)
        return modules

    @staticmethod
    def _get_builtin_modules():
        standard_library_modules = DependencyAnalysis._load_standard_library_modules()
        builtins_modules = DependencyAnalysis._load_builtin_modules()
        additional_modules = DependencyAnalysis._load_additional_modules()
        all_modules = standard_library_modules + builtins_modules + additional_modules

        return set(all_modules)

    @staticmethod
    def _get_schema_template():
        schema = StructType([
            StructField("TOOL_EXECUTION_ID", StringType()),
            StructField("EXECUTION_TIMESTAMP", TimestampType()),
            StructField("SOURCE_FILE", StringType()),
            StructField("IMPORT", StringType()),
            StructField("DEPENDENCIES", ArrayType()),
            StructField("PROJECT_ID", StringType()),
            StructField("IS_BUILTIN", BooleanType()),
            StructField("ORIGIN", StringType()),
            StructField("SUPPORTED", BooleanType())
        ])

        return schema

    @staticmethod
    def _normalize_usages_file_paths(df):
        normalized_df = df.withColumn("FILE_ID", replace("FILE_ID", "\\\\", "/"))
        normalized_df = normalized_df.withColumn("FILE_ID", iff(col("FILE_ID").startswith("/"),
                                                                substring(col("FILE_ID"), 2, length(col("FILE_ID"))),
                                                                col("FILE_ID")))

        return normalized_df

    @staticmethod
    def _normalize_files_inventory_paths(df):
        normalized_df = df \\
            .withColumn("REVIEWED_SOURCE_FILE", replace("REVIEWED_SOURCE_FILE", "\\\\", "/")) \\
            .withColumn("REVIEWED_SOURCE_FILE", iff(col("REVIEWED_SOURCE_FILE").startswith("/"),
                                                    substring(col("REVIEWED_SOURCE_FILE"), 2,
                                                              length(col("REVIEWED_SOURCE_FILE"))),
                                                    col("REVIEWED_SOURCE_FILE"))) \\
            .withColumn("FILE_ID", replace("FILE_ID", "\\\\", "/")) \\
            .withColumn("FILE_ID", iff(col("FILE_ID").startswith("/"),
                                       substring(col("FILE_ID"), 2, length(col("FILE_ID"))),
                                       col("FILE_ID")))

        return normalized_df

    def dependency_analysis(self, df_all_import_usages, current_execution_id, df_all_executions_info, timeout_sec=30):
        limit = (datetime.now() + timedelta(0, timeout_sec))

        df_current_import_usages = df_all_import_usages.where((col("EXECUTION_ID") == lit(current_execution_id)))
        print(f"Current imports: {df_current_import_usages.count()}")

        if df_current_import_usages.count() >= DependencyAnalysis.max_imports_to_process:
            raise Exception(f"Current imports ({df_current_import_usages.count()}) exceeds limit ({DependencyAnalysis.max_imports_to_process}).")

        df_current_import_usages = DependencyAnalysis._normalize_usages_file_paths(df_current_import_usages)
        df_current_import_usages = df_current_import_usages.join(df_all_executions_info, how=''inner'', on=[''EXECUTION_ID''],rsuffix="executions_list")
        df_current_import_usages = df_current_import_usages.select(
            col(''EXECUTION_ID'').alias(''TOOL_EXECUTION_ID''),
            col(''EXECUTION_TIMESTAMP'').alias(''TOOL_EXECUTION_TIMESTAMP''),
            col("FILE_ID").alias(''FILE_ID''),
            col("ELEMENT").alias(''IMPORT''),
            ''LINE'',
            ''PROJECT_ID'',
            ''ORIGIN'',
            col("IS_SNOWPARK_ANACONDA_SUPPORTED").alias(''SUPPORTED'')
        )

        all_files = self.session.table([self.schema_name, "INPUT_FILES_INVENTORY"]) \\
            .where((col("EXECUTION_ID") == lit(current_execution_id)) & (col("TECHNOLOGY") != lit(''Other''))) \\
            .select("FILE_ID", col("FILE_ID").alias("REVIEWED_SOURCE_FILE"))

        all_files = DependencyAnalysis._normalize_files_inventory_paths(all_files)

        all_files = all_files.select(
            "FILE_ID",
            "REVIEWED_SOURCE_FILE",
            replace(replace(col("REVIEWED_SOURCE_FILE"), ".py", ""), "/", ".").alias("MODULE_NAME")) \\
            .sort(length("MODULE_NAME"), ascending=False)
        all_files = all_files.to_pandas()
        current_imports = pd.DataFrame(df_current_import_usages.to_pandas())

        def _is_builtin(found_import, import_element, builtin_modules):
            if found_import is not None or len(found_import) > 0:
                first_path = import_element.split(".")[0]
                if first_path in builtin_modules:
                    return True
                else:
                    return False
            return False

        def _review_alignment(candidates, original_import_element):
            if candidates is not None and len(candidates) == 2:
                pattern = candidates[0]
                candidate_file = candidates[1]
                import_element_suffix = re.sub(pattern, "", original_import_element)
                pattern = pattern.replace("\\.", ".")
                candidate_file_suffix = re.sub(f".*{pattern}", "", candidate_file)
                if import_element_suffix == "" and candidate_file_suffix == "/__init__.py":
                    return candidate_file
                else:
                    candidate_file_suffix_to_import = candidate_file_suffix.replace(".py", "").replace("/", ".")
                    if import_element_suffix.startswith(candidate_file_suffix_to_import):
                        return candidate_file

        def _find_candidates(import_pattern, files_inventory, import_element_pattern):
            candidates = files_inventory.apply(
                lambda x: (import_pattern, x[''FILE_ID''],) if import_element_pattern.search(x[''MODULE_NAME'']) else None,
                axis=1)
            return candidates.dropna().to_numpy()

        def _find_import(import_element, files_inventory):
            def shorten(imp):
                l = imp.split("\\\\.")
                l.pop()
                return ".".join(l)

            original_import_element = import_element
            try:
                if original_import_element == ".":
                    return []
                import_element = r''\\b'' + import_element.replace(''.'', ''\\\\.'').replace('')'', '''').replace(''"'', '''') + r''\\b''
                looking = True
                while looking and len(import_element) > 0:
                    import_element_pattern = re.compile(import_element)
                    candidates = _find_candidates(import_element, files_inventory, import_element_pattern)
                    if len(candidates) == 0:
                        import_element = shorten(import_element)
                    else:
                        looking = False

                if len(candidates) == 1 and candidates[0][1] is not None:
                    return [candidates[0][1]]
                elif len(candidates) > 1:
                    vectorized_func = np.vectorize(_review_alignment)
                    return vectorized_func(candidates, original_import_element).tolist()
                return []

            except Exception as ex:
                raise Exception(original_import_element, ex)

        def process_imports(row, files_inventory, builtins):
            print_log(f"processing fileId: {row.FILE_ID} - import :{row.IMPORT}")
            found_imports = _find_import(row.IMPORT, files_inventory)
            found_imports = [x for x in found_imports if x is not None ]
            return (row.TOOL_EXECUTION_ID,
                    row.TOOL_EXECUTION_TIMESTAMP,
                    row.FILE_ID,
                    row.IMPORT,
                    found_imports,
                    row.PROJECT_ID,
                    _is_builtin(found_imports, row.IMPORT, builtins),
                    row.ORIGIN,
                    row.SUPPORTED,)

        builtins = self.builtin_modules
        processed_imports = current_imports.apply(process_imports, axis=1, args=(all_files, builtins,))
        df = self.session.createDataFrame(data=processed_imports.tolist(), schema=DependencyAnalysis._get_schema_template())
        df.write.mode(''append'').save_as_table(table_name=f"{self.db}.{self.schema}.COMPUTED_DEPENDENCIES")

    def pre_calc_dependency_analysis(self, executions_id=None):

        dependency_calculated = (self.session.table([self.schema_name, "COMPUTED_DEPENDENCIES"])
                                 .select("TOOL_EXECUTION_ID").distinct())

        df_all_import_usages = self.session.table([self.schema_name, "IMPORT_USAGES_INVENTORY"]).distinct()

        df_pending_list = (self.session.table([self.schema_name, "PENDING_EXECUTION_LIST"])
                           .select(col(''EXECUTION_ID'').alias(''TOOL_EXECUTION_ID'')).distinct())

        df_all_executions_info = self.session.table([self.schema_name, ''EXECUTION_INFO'']).distinct()

        df_all_import_usages = df_all_import_usages.join(df_all_executions_info, how=''inner'', on=[''EXECUTION_ID''])

        executions_to_process = (df_all_import_usages.select(
            col(''EXECUTION_ID'').alias(''TOOL_EXECUTION_ID''),
            col(''EXECUTION_TIMESTAMP''))
                                 .distinct()
                                 .where(~col("TOOL_EXECUTION_ID").isin(dependency_calculated))
                                 .where(~col("TOOL_EXECUTION_ID").isin(df_pending_list))
                                 .sort(col("EXECUTION_TIMESTAMP").desc(), "TOOL_EXECUTION_ID"))

        if executions_id is not None:
            executions_to_process = executions_to_process.filter(executions_to_process[''TOOL_EXECUTION_ID''].isin(executions_id))
        executions_to_process = executions_to_process.collect()

        log = []
        log = add_log(log, f''Found {dependency_calculated.count()} dependencies already calculated'')
        log = add_log(log, f"{len(executions_to_process)} still to go")

        for execution in executions_to_process:
            start_time = time.time()
            print(f"Start time: {start_time}")
            try:
                print(f"Processing execution {execution.TOOL_EXECUTION_ID}")
                has_exception = False
                self.dependency_analysis(df_all_import_usages, execution.TOOL_EXECUTION_ID, df_all_executions_info)

            except Exception as ex:
                has_exception = True
                log = add_log(log, str(ex))
                register_pending_execution(self, execution.TOOL_EXECUTION_ID, str(ex))

            end_time = time.time()
            total_time = end_time - start_time
            if has_exception:
                log = add_log(log, f"Found an Exception with Execution: {execution.TOOL_EXECUTION_ID}")
                log = add_log(log, traceback.format_exc())
            else:
                clean_pending_list(df_pending_list, execution, self.session)

                log = add_log(log, f"{execution.TOOL_EXECUTION_ID} Time taken for dependency_analysis: {total_time:.2f} seconds.")
                log = add_log(log, ''\\n'')
        res = self.session.table([self.schema_name, "COMPUTED_DEPENDENCIES"])
        return ''\\n''.join(log)';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."PRE_CALC_DEPENDENCY_ANALYSIS"() IS '';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."REFRESH_MANUAL_EXECUTION" (
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS 'BEGIN
COPY INTO "ASSESSMENTS_FILES_RAW"  (
      "EXECUTION_ID"
    , "EXTRACT_DATE"
    , "FILE_NAME"
    , "INVENTORY_CONTENT"
) FROM (
    SELECT
          SPLIT_PART(metadata$filename, ''/'', 1)
        , metadata$FILE_LAST_MODIFIED
        , SPLIT_PART(metadata$filename, ''/'', 2)
        , $1
    FROM @"SMA_EXECUTIONS"
    (PATTERN => ''.*\\.json$'')
)
FILE_FORMAT = "JSON";

COPY INTO "REPORT_URL"  (
      "EXECUTION_ID"
    , "FILE_NAME"
    , "RELATIVE_REPORT_PATH"
) FROM (
    SELECT
          SPLIT_PART(metadata$filename, ''/'', 1)
        , SPLIT_PART(metadata$filename, ''/'', 2)
        , metadata$filename

    FROM @"SMA_EXECUTIONS"
    (PATTERN => ''.*\\.docx$'')
)
FILE_FORMAT = "DOCX_FORMAT";

RETURN ''Upload successfully'';

END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."REFRESH_MANUAL_EXECUTION"() IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."REFRESH_MANUAL_MAPPINGS" (
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS 'BEGIN
COPY INTO "ALL_MAPPINGS_CORE_RAW"  (
      "VERSION"
    , "FOLDER"
    , "FILE_NAME"
    , "FILE_CONTENT"
) FROM (
    SELECT
          SPLIT_PART(metadata$filename, ''/'', 1)
        , SPLIT_PART(metadata$filename, ''/'', 2)
        , SPLIT_PART(metadata$filename, ''/'', 3)
        , $1
    FROM @"SMA_MAPPINGS"
    (PATTERN => ''.*\\.json$'')
)
FILE_FORMAT = "JSON";

RETURN ''Upload successfully'';

END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."REFRESH_MANUAL_MAPPINGS"() IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."REFRESH_MAPPINGS_CORE" (
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
AS 'DECLARE 

TABLE_NAME varchar DEFAULT concat(''STREAM_DATA_'' , DATE_PART(epoch_second, CURRENT_TIMESTAMP()));

BEGIN
CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:TABLE_NAME) (
    VERSION STRING,
    FOLDER STRING,
    FILE_NAME STRING,
    FILE_CONTENT VARIANT
    );
INSERT INTO IDENTIFIER(:TABLE_NAME) SELECT VERSION, FOLDER, FILE_NAME, FILE_CONTENT FROM ALL_MAPPINGS_CORE_STREAM;

CALL INSERT_MAPPINGS_EWI_CATALOG(:TABLE_NAME);
CALL INSERT_MAPPINGS_LIBRARY(:TABLE_NAME);
CALL INSERT_MAPPINGS_PANDAS(:TABLE_NAME);
CALL INSERT_MAPPINGS_PYSPARK(:TABLE_NAME);
CALL INSERT_MAPPINGS_SPARK(:TABLE_NAME);
CALL INSERT_MAPPINGS_SQL_ELEMENTS(:TABLE_NAME);
CALL INSERT_MAPPINGS_SQL_FUNCTIONS(:TABLE_NAME);

RETURN ''INSERT_MAPPINGS_CORE_TABLES FINISHED'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."REFRESH_MAPPINGS_CORE"() IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."START_IAA_MECHANISM" (
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS 'BEGIN
  ALTER TASK INSERT_COMPUTED_DEPENDENCIES RESUME;
  ALTER TASK UPDATE_IAA_TABLES_TASK RESUME;
  ALTER TASK REFRESH_MAPPINGS_CORE_TASK RESUME;
  CALL REFRESH_MANUAL_MAPPINGS();
  
  RETURN ''Initialization completed'';
END;';

COMMENT ON PROCEDURE "SNOW_DB"."SNOW_SCHEMA"."START_IAA_MECHANISM"() IS '';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."ALL_MAPPINGS_CORE_RAW"
(
      "VERSION" VARCHAR(16777216)
    , "FOLDER" VARCHAR(16777216)
    , "FILE_NAME" VARCHAR(16777216)
    , "FILE_CONTENT" VARIANT
)
CHANGE_TRACKING = TRUE
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."APP_LOG"
(
      "ID" NUMBER(10,0) DEFAULT SNOW_DB.SNOW_SCHEMA.APP_LOG_SEQUENCE.NEXTVAL NOT NULL
    , "APPLICATION" VARCHAR(16777216)
    , "MESSAGE" VARCHAR(16777216)
    , "CUSTOMER_EMAIL" VARCHAR(16777216)
    , "EXECUTION_ID" VARCHAR(16777216)
    , "TIMESTAMP" TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_NTZ(3)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."ARTIFACTS_DEPENDENCY_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "DEPENDENCY" VARCHAR(16777216)
    , "TYPE" VARCHAR(16777216)
    , "ARGUMENTS" VARIANT
    , "STATUS" VARCHAR(16777216)
    , "LOCATION" VARIANT
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."ASSESSMENTS_FILES_RAW"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "EXTRACT_DATE" TIMESTAMP_NTZ(9)
    , "FILE_NAME" VARCHAR(16777216)
    , "INVENTORY_CONTENT" VARIANT
)
CHANGE_TRACKING = TRUE
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."COMPUTED_DEPENDENCIES"
(
      "TOOL_EXECUTION_ID" VARCHAR(16777216)
    , "TOOL_EXECUTION_TIMESTAMP" TIMESTAMP_NTZ(9)
    , "SOURCE_FILE" VARCHAR(16777216)
    , "IMPORT" VARCHAR(16777216)
    , "DEPENDENCIES" ARRAY
    , "PROJECT_ID" VARCHAR(16777216)
    , "IS_BUILTIN" BOOLEAN
    , "ORIGIN" VARCHAR(16777216)
    , "SUPPORTED" BOOLEAN
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."DATAFRAMES_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "FULLNAME" VARCHAR(16777216)
    , "NAME" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "CELL_ID" VARCHAR(16777216)
    , "LINE" NUMBER(38,0)
    , "COLUMN" NUMBER(38,0)
    , "ASSIGNMENT_NUMBER" NUMBER(38,0)
    , "RELEVANT_FUNCTION" VARCHAR(16777216)
    , "RELATED_DATAFRAMES" VARCHAR(16777216)
    , "ENTRY_POINTS" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."EXECUTION_INFO"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "EXECUTION_TIMESTAMP" TIMESTAMP_NTZ(9)
    , "CUSTOMER_LICENSE" VARCHAR(16777216)
    , "TOOL_VERSION" VARCHAR(16777216)
    , "TOOL_NAME" VARCHAR(16777216)
    , "MARGIN_ERROR" VARCHAR(16777216)
    , "CLIENT_EMAIL" VARCHAR(16777216)
    , "MAPPING_VERSION" VARCHAR(16777216)
    , "CONVERSION_SCORE" VARCHAR(16777216)
    , "SESSION_ID" VARCHAR(16777216)
    , "COMPANY" VARCHAR(16777216)
    , "PROJECT_NAME" VARCHAR(16777216)
    , "PARSING_SCORE" VARCHAR(16777216)
    , "SPARK_API_READINESS_SCORE" VARCHAR(16777216)
    , "PYTHON_READINESS_SCORE" VARCHAR(16777216)
    , "SCALA_READINESS_SCORE" VARCHAR(16777216)
    , "SQL_READINESS_SCORE" VARCHAR(16777216)
    , "THIRD_PARTY_READINESS_SCORE" VARCHAR(16777216)
)
CHANGE_TRACKING = TRUE
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."IMPORT_USAGES_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "ALIAS" VARCHAR(16777216)
    , "KIND" VARCHAR(16777216)
    , "LINE" NUMBER(38,0)
    , "PACKAGE_NAME" VARCHAR(16777216)
    , "STATUS" VARCHAR(16777216)
    , "IS_SNOWPARK_ANACONDA_SUPPORTED" VARCHAR(16777216)
    , "SNOWPARK_CORE_VERSION" VARCHAR(16777216)
    , "SNOWPARK_VERSION" VARCHAR(16777216)
    , "ELEMENT_PACKAGE" VARCHAR(16777216)
    , "CELL_ID" NUMBER(38,0)
    , "ORIGIN" VARCHAR(16777216)
    , "FULL_NAME" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."INPUT_FILES_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "EXTENSION" VARCHAR(16777216)
    , "BYTES" NUMBER(38,0)
    , "CHARACTER_LENGTH" NUMBER(38,0)
    , "LINES_OF_CODE" NUMBER(38,0)
    , "PARSE_RESULT" VARCHAR(16777216)
    , "TECHNOLOGY" VARCHAR(16777216)
    , "IGNORED" VARCHAR(16777216)
    , "ORIGIN_FILE_PATH" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."IO_FILES_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "IS_LITERAL" VARCHAR(16777216)
    , "FORMAT" VARCHAR(16777216)
    , "FORMAT_TYPE" VARCHAR(16777216)
    , "MODE" VARCHAR(16777216)
    , "SUPPORTED" VARCHAR(16777216)
    , "LINE" NUMBER(38,0)
    , "OPTION_SETTINGS" VARCHAR(16777216)
    , "CELL_ID" NUMBER(38,0)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."ISSUES_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "CODE" VARCHAR(16777216)
    , "DESCRIPTION" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "LINE" NUMBER(38,0)
    , "COLUMN" NUMBER(38,0)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."JAVA_BUILTINS"
(
      "NAME" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."JOINS_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "IS_SELF_JOIN" VARCHAR(16777216)
    , "HAS_LEFT_ALIAS" VARCHAR(16777216)
    , "HAS_RIGHT_ALIAS" VARCHAR(16777216)
    , "LINE" NUMBER(38,0)
    , "KIND" VARCHAR(16777216)
    , "CELL_ID" NUMBER(38,0)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."MANUAL_UPLOADED_ZIPS"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "EXTRACT_DATE" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."MAPPINGS_CORE_EWI_CATALOG"
(
      "VERSION" VARCHAR(16777216)
    , "EWI_CODE" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "CATEGORY" VARCHAR(16777216)
    , "DEPRECATED_VERSION" VARCHAR(16777216)
    , "SHORT_DESCRIPTION" VARCHAR(16777216)
    , "GENERAL_DESCRIPTION" VARCHAR(16777216)
    , "LONG_DESCRIPTION" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."MAPPINGS_CORE_LIBRARIES"
(
      "VERSION" VARCHAR(16777216)
    , "SOURCE_LIBRARY_NAME" VARCHAR(16777216)
    , "LIBRARY_PREFIX" VARCHAR(16777216)
    , "ORIGIN" VARCHAR(16777216)
    , "SUPPORTED" BOOLEAN
    , "STATUS" VARCHAR(16777216)
    , "TARGET_LIBRARY_NAME" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."MAPPINGS_CORE_PANDAS"
(
      "VERSION" VARCHAR(16777216)
    , "CATEGORY" VARCHAR(16777216)
    , "SPARK_FULLY_QUALIFIED_NAME" VARCHAR(16777216)
    , "SPARK_NAME" VARCHAR(16777216)
    , "SPARK_CLASS" VARCHAR(16777216)
    , "SPARK_DEF" VARCHAR(16777216)
    , "SNOWPARK_FULLY_QUALIFIED_NAME" VARCHAR(16777216)
    , "SNOWPARK_NAME" VARCHAR(16777216)
    , "SNOWPARK_CLASS" VARCHAR(16777216)
    , "SNOWPARK_DEF" VARCHAR(16777216)
    , "TOOL_SUPPORTED" BOOLEAN
    , "SNOWFLAKE_SUPPORTED" BOOLEAN
    , "MAPPING_STATUS" VARCHAR(16777216)
    , "WORKAROUND_COMMENT" VARCHAR(16777216)
    , "EWICODE" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."MAPPINGS_CORE_PYSPARK"
(
      "VERSION" VARCHAR(16777216)
    , "CATEGORY" VARCHAR(16777216)
    , "SPARK_FULLY_QUALIFIED_NAME" VARCHAR(16777216)
    , "SPARK_NAME" VARCHAR(16777216)
    , "SPARK_CLASS" VARCHAR(16777216)
    , "SPARK_DEF" VARCHAR(16777216)
    , "SNOWPARK_FULLY_QUALIFIED_NAME" VARCHAR(16777216)
    , "SNOWPARK_NAME" VARCHAR(16777216)
    , "SNOWPARK_CLASS" VARCHAR(16777216)
    , "SNOWPARK_DEF" VARCHAR(16777216)
    , "TOOL_SUPPORTED" BOOLEAN
    , "SNOWFLAKE_SUPPORTED" BOOLEAN
    , "MAPPING_STATUS" VARCHAR(16777216)
    , "WORKAROUND_COMMENT" VARCHAR(16777216)
    , "EWICODE" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."MAPPINGS_CORE_SPARK"
(
      "VERSION" VARCHAR(16777216)
    , "CATEGORY" VARCHAR(16777216)
    , "SPARK_FULLY_QUALIFIED_NAME" VARCHAR(16777216)
    , "SPARK_NAME" VARCHAR(16777216)
    , "SPARK_CLASS" VARCHAR(16777216)
    , "SPARK_DEF" VARCHAR(16777216)
    , "SNOWPARK_FULLY_QUALIFIED_NAME" VARCHAR(16777216)
    , "SNOWPARK_NAME" VARCHAR(16777216)
    , "SNOWPARK_CLASS" VARCHAR(16777216)
    , "SNOWPARK_DEF" VARCHAR(16777216)
    , "TOOL_SUPPORTED" BOOLEAN
    , "SNOWFLAKE_SUPPORTED" BOOLEAN
    , "MAPPING_STATUS" VARCHAR(16777216)
    , "WORKAROUND_COMMENT" VARCHAR(16777216)
    , "EWICODE" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."MAPPINGS_CORE_SQL_ELEMENTS"
(
      "VERSION" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "SOURCE_FULL_NAME" VARCHAR(16777216)
    , "CATEGORY" VARCHAR(16777216)
    , "FLAVOR" VARCHAR(16777216)
    , "EWI" VARCHAR(16777216)
    , "CONVERSION_STATUS" VARCHAR(16777216)
    , "TARGET_ELEMENT" VARCHAR(16777216)
    , "DEFAULT_MAPPING" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."MAPPINGS_CORE_SQL_FUNCTIONS"
(
      "VERSION" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "FLAVOR" VARCHAR(16777216)
    , "CATEGORY" VARCHAR(16777216)
    , "MIGRATION_STATUS" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."NOTEBOOK_CELLS_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "ARGUMENTS" VARCHAR(16777216)
    , "CELL_ID" NUMBER(38,0)
    , "LOC" NUMBER(38,0)
    , "SIZE" NUMBER(38,0)
    , "SUPPORTED_STATUS" VARCHAR(16777216)
    , "PARSING_RESULT" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."NOTEBOOK_SIZE_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "PYTHON_LOC" VARCHAR(16777216)
    , "SCALA_LOC" VARCHAR(16777216)
    , "SQL_LOC" VARCHAR(16777216)
    , "LINE" NUMBER(38,0)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."PACKAGES_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."PACKAGE_VERSIONS_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "VERSION" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."PANDAS_USAGES_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "ALIAS" VARCHAR(16777216)
    , "KIND" VARCHAR(16777216)
    , "LINE" NUMBER(38,0)
    , "PACKAGE_NAME" VARCHAR(16777216)
    , "SUPPORTED" VARCHAR(16777216)
    , "AUTOMATED" VARCHAR(16777216)
    , "STATUS" VARCHAR(16777216)
    , "SNOWCONVERT_CORE_VERSION" VARCHAR(16777216)
    , "PANDAS_VERSION" VARCHAR(16777216)
    , "CELL_ID" NUMBER(38,0)
    , "PARAMETERS_INFO" VARIANT
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."PENDING_EXECUTION_LIST"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "MESSAGE" VARCHAR(16777216)
    , "TIMESTAMP" TIMESTAMP_NTZ(9)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."REPORT_URL"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "FILE_NAME" VARCHAR(16777216)
    , "RELATIVE_REPORT_PATH" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."SPARK_USAGES_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "ALIAS" VARCHAR(16777216)
    , "KIND" VARCHAR(16777216)
    , "LINE" NUMBER(38,0)
    , "PACKAGE_NAME" VARCHAR(16777216)
    , "SUPPORTED" VARCHAR(16777216)
    , "AUTOMATED" VARCHAR(16777216)
    , "STATUS" VARCHAR(16777216)
    , "SNOWCONVERT_CORE_VERSION" VARCHAR(16777216)
    , "SNOWPARK_VERSION" VARCHAR(16777216)
    , "CELL_ID" NUMBER(38,0)
    , "PARAMETERS_INFO" VARIANT
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."SQL_ELEMENTS_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "NOTEBOOK_CELL_ID" NUMBER(38,0)
    , "LINE" NUMBER(38,0)
    , "COLUMN" NUMBER(38,0)
    , "SQL_FLAVOR" VARCHAR(16777216)
    , "ROOT_FULLNAME" VARCHAR(16777216)
    , "ROOT_LINE" NUMBER(38,0)
    , "ROOT_COLUMN" NUMBER(38,0)
    , "TOP_LEVEL_FULLNAME" VARCHAR(16777216)
    , "TOP_LEVEL_LINE" NUMBER(38,0)
    , "TOP_LEVEL_COLUMN" NUMBER(38,0)
    , "CONVERSION_STATUS" VARCHAR(16777216)
    , "CATEGORY" VARCHAR(16777216)
    , "EWI" VARCHAR(16777216)
    , "OBJECT_REFERENCE" VARIANT
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."SQL_EMBEDDED_USAGES_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "LIBRARY_NAME" VARCHAR(16777216)
    , "HAS_LITERAL" VARCHAR(16777216)
    , "HAS_VARIABLE" VARCHAR(16777216)
    , "HAS_FUNCTION" VARCHAR(16777216)
    , "PARSING_STATUS" VARCHAR(16777216)
    , "HAS_INTERPOLATION" VARCHAR(16777216)
    , "CELL_ID" NUMBER(38,0)
    , "LINE" NUMBER(38,0)
    , "COLUMN" NUMBER(38,0)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."SQL_FUNCTIONS_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "CATEGORY" VARCHAR(16777216)
    , "CELL_ID" NUMBER(38,0)
    , "LINE" NUMBER(38,0)
    , "COLUMN" NUMBER(38,0)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."TELEMETRY_EVENTS"
(
      "ID" NUMBER(38,0) DEFAULT SNOW_DB.SNOW_SCHEMA.AUTOINCREMENT_SEQUENCE.NEXTVAL
    , "TIMESTAMP" TIMESTAMP_NTZ(9)
    , "NAME" VARCHAR(16777216)
    , "USER" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."TELEMETRY_EVENTS_ATTRIBUTES"
(
      "EVENTID" NUMBER(38,0)
    , "TIMESTAMP" TIMESTAMP_NTZ(9)
    , "NAME" VARCHAR(16777216)
    , "VALUE" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."THIRD_PARTY_CATEGORIES"
(
      "CATEGORY" VARCHAR(16777216)
    , "FILTER" VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

CREATE OR REPLACE TABLE "SNOW_DB"."SNOW_SCHEMA"."THIRD_PARTY_USAGES_INVENTORY"
(
      "EXECUTION_ID" VARCHAR(16777216)
    , "ELEMENT" VARCHAR(16777216)
    , "PROJECT_ID" VARCHAR(16777216)
    , "FILE_ID" VARCHAR(16777216)
    , "COUNT" NUMBER(38,0)
    , "ALIAS" VARCHAR(16777216)
    , "KIND" VARCHAR(16777216)
    , "LINE" NUMBER(38,0)
    , "PACKAGE_NAME" VARCHAR(16777216)
    , "CELL_ID" NUMBER(38,0)
    , "PARAMETERS_INFO" VARIANT
)
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}';

ALTER TABLE "SNOW_DB"."SNOW_SCHEMA"."APP_LOG" ADD PRIMARY KEY ("ID");

CREATE OR REPLACE STREAM "SNOW_DB"."SNOW_SCHEMA"."ALL_MAPPINGS_CORE_STREAM"
ON TABLE "SNOW_DB"."SNOW_SCHEMA"."ALL_MAPPINGS_CORE_RAW";

CREATE OR REPLACE STREAM "SNOW_DB"."SNOW_SCHEMA"."ASSESSMENTS_FILES_STREAM"
ON TABLE "SNOW_DB"."SNOW_SCHEMA"."ASSESSMENTS_FILES_RAW";

CREATE OR REPLACE STREAM "SNOW_DB"."SNOW_SCHEMA"."EXECUTION_INFO_STREAM"
ON TABLE "SNOW_DB"."SNOW_SCHEMA"."EXECUTION_INFO";

CREATE OR REPLACE TASK "SNOW_DB"."SNOW_SCHEMA"."INSERT_COMPUTED_DEPENDENCIES"
WAREHOUSE = "SNOW_WAREHOUSE"
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
WHEN SYSTEM$STREAM_HAS_DATA('EXECUTION_INFO_STREAM')
AS
CALL INSERT_COMPUTED_DEPENDENCIES();

COMMENT ON TASK "SNOW_DB"."SNOW_SCHEMA"."INSERT_COMPUTED_DEPENDENCIES" IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE TASK "SNOW_DB"."SNOW_SCHEMA"."REFRESH_MAPPINGS_CORE_TASK"
WAREHOUSE = "SNOW_WAREHOUSE"
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
WHEN SYSTEM$STREAM_HAS_DATA('ALL_MAPPINGS_CORE_STREAM')
AS
CALL REFRESH_MAPPINGS_CORE();

COMMENT ON TASK "SNOW_DB"."SNOW_SCHEMA"."REFRESH_MAPPINGS_CORE_TASK" IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

CREATE OR REPLACE TASK "SNOW_DB"."SNOW_SCHEMA"."UPDATE_IAA_TABLES_TASK"
WAREHOUSE = "SNOW_WAREHOUSE"
COMMENT = '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}}'
WHEN SYSTEM$STREAM_HAS_DATA('ASSESSMENTS_FILES_STREAM')
AS
CALL INSERT_PIPE_DATA_TO_IAA_TABLES();

COMMENT ON TASK "SNOW_DB"."SNOW_SCHEMA"."UPDATE_IAA_TABLES_TASK" IS '{"origin":"sf_sit","name":"iaa","version":{"major":0,"minor":6,"patch":19}} ';

