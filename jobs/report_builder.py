from time import sleep
from datetime import datetime
from pyspark import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StringType, StructField

from st_connectors.db.snowflake.client import SnowflakeConnector
from st_utils.logger import get_logger
from project_settings import settings

logger = get_logger(__name__)
snow = SnowflakeConnector(sf_url= settings.sfUrl,
                          sf_user=settings.SFUSER,
                          sf_password=settings.SFPASSWORD,
                          sf_role=settings.SFROLE,
                          sf_warehouse=settings.SFWAREHOUSE,
                          sf_schema=settings.SFSCHEMA,
                          sf_database=settings.SFDATABASE)


def put_raw_data(spark, run_name, result_list):
    convert_str_to_list = [[i.rstrip("#").split("#")[0], i.rstrip("#").split("#")[1],
                           i.rstrip("#").split("#")[2]] for i in result_list]

    schema = StructType([
    StructField("QUERY_NAME", StringType()),
    StructField("QUERY_ID", StringType()),
    StructField("PYTHON_TIME", StringType())
    ])
    df = spark.createDataFrame(data=convert_str_to_list, schema=schema)
    df = df.withColumn("RUN_NAME", lit(run_name))
    snow.write_dataframe(df, "python_runs")


def build_report(spark, run_name, result_list):
    dt = datetime.utcnow().strftime("%Y-%m-%d-%H:%M")
    unique_run_name = f"{run_name}_{dt}"
    put_raw_data(spark, unique_run_name, result_list)
    query = f'''
        insert into cancer_test_runs (
                RUN_NAME,
                QUERY_ID,
                WAREHOUSE_NAME,
                WAREHOUSE_SIZE,
                WAREHOUSE_TYPE,
                CLUSTER_NUMBER,
                QUERY_TAG
                EXECUTION_STATUS,
                ERROR_CODE,
                ERROR_MESSAGE,
                START_TIME,
                END_TIME,
                TOTAL_ELAPSED_TIME,
                BYTES_SCANNED,
                PERCENTAGE_SCANNED_FROM_CACHE,
                PARTITIONS_SCANNED,
                PARTITIONS_TOTAL,
                COMPILATION_TIME,
                EXECUTION_TIME
                QUEUED_PROVISIONING_TIME,
                QUEUED_REPAIR_TIME,
                QUEUED_OVERLOAD_TIME,
                TRANSACTION_BLOCKED_TIME,
                CREDITS_USED_CLOUD_SERVICES,
                QUERY_LOAD_PERCENT,
                QUERY_NAME,
                PYTHON_TIME
    )
    select 
        a.RUN_NAME,
        b.QUERY_ID,
        b.WAREHOUSE_NAME,
        b.WAREHOUSE_SIZE,
        b.WAREHOUSE_TYPE,
        b.CLUSTER_NUMBER,
        b.QUERY_TAG,
        b.EXECUTION_STATUS,
        b.ERROR_CODE,
        b.ERROR_MESSAGE,
        b.START_TIME,
        b.END_TIME,
        b.TOTAL_ELAPSED_TIME,
        b.BYTES_SCANNED,
        b.PERCENTAGE_SCANNED_FROM_CACHE,
        b.PARTITIONS_SCANNED,
        b.PARTITIONS_TOTAL,
        b.COMPILATION_TIME,
        b.EXECUTION_TIME,
        b.QUEUED_PROVISIONING_TIME,
        b.QUEUED_REPAIR_TIME,
        b.QUEUED_OVERLOAD_TIME,
        b.TRANSACTION_BLOCKED_TIME,
        b.CREDITS_USED_CLOUD_SERVICES,
        b.QUERY_LOAD_PERCENT,
        a.QUERY_NAME,
        a.PYTHON_TIME
        from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY b, python_runs a
    where a.QUERY_ID = b.QUERY_ID
'''
    snow.run_ddl_dml_without_spark(query)

'''
select 
        a.RUN_NAME,
        b.TOTAL_ELAPSED_TIME,
        a.PYTHON_TIME,
        b.QUERY_ID,
        b.WAREHOUSE_NAME,
        b.WAREHOUSE_SIZE,
        b.WAREHOUSE_TYPE,
        b.CLUSTER_NUMBER,
        b.QUERY_TAG,
        b.EXECUTION_STATUS,
        b.ERROR_CODE,
        b.ERROR_MESSAGE,
        b.START_TIME,
        b.END_TIME,
        b.BYTES_SCANNED,
        b.PERCENTAGE_SCANNED_FROM_CACHE,
        b.PARTITIONS_SCANNED,
        b.PARTITIONS_TOTAL,
        b.COMPILATION_TIME,
        b.EXECUTION_TIME,
        b.QUEUED_PROVISIONING_TIME,
        b.QUEUED_REPAIR_TIME,
        b.QUEUED_OVERLOAD_TIME,
        b.TRANSACTION_BLOCKED_TIME,
        b.CREDITS_USED_CLOUD_SERVICES,
        b.QUERY_LOAD_PERCENT,
        a.QUERY_NAME
        from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY b, python_runs a
    where a.QUERY_ID = b.QUERY_ID
    and upper(a.run_name) like '%RUN_WITHOUT_FETCH%'
    and b.warehouse_name = 'WH_IRP_CANCER'
    and b.user_name = 'Q845332_CANCER' 
    and database_id = 408
'''