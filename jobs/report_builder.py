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

