import logging
import sys


class SnowflakeConnector:

    def __init__(self, sf_url=None, sf_user=None, sf_password=None,
                 sf_database=None, sf_role='SYSADMIN', sf_schema='PUBLIC',
                 sf_warehouse="INGEST"):

        logging.info("Initiating snowflake database connection")
        try:
            self.sf_url = sf_url
            self.sf_user = sf_user
            self.sf_password = sf_password
            self.sf_database = sf_database
            self.sf_role = sf_role
            self.sf_schema = sf_schema
            self.sf_warehouse = sf_warehouse

            self.options = dict(sfUrl=sf_url,
                                sfUser=sf_user,
                                sfPassword=sf_password,
                                sfDatabase=sf_database,
                                sfSchema=sf_schema,
                                sfWarehouse=sf_warehouse,
                                sfRole=sf_role)
        except Exception as error:
            logging.error(f"Snowflake connection failed - {error}")

    def run_ddl_dml_without_spark(self, query):
        from snowflake.connector import connect
        try:
            conn = connect(
                user=self.sf_user,
                password=self.sf_password,
                account=(self.sf_url).replace("https://", "") \
                        .replace(".snowflakecomputing.com", ""),
                warehouse=self.sf_warehouse,
                database=self.sf_database,
                schema=self.sf_schema)
            cur = conn.cursor()
            return cur
        except Exception as error:
            logging.error(f'error in run_ddl_dml  {error}')
            raise error.with_traceback(sys.exc_info()[2])

    def run_ddl_dml(self, spark, query):
        try:
            result = spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
                self.options, query)
            return result
        except Exception as error:
            logging.error(f'error in run_ddl_dml  {error}')
            raise error.with_traceback(sys.exc_info()[2])

    def read_snowflake_table(self, spark, table_name):
        try:
            dataframe = spark\
                .read \
                .format("snowflake") \
                .options(**self.options) \
                .option("dbtable", table_name) \
                .load()
            return dataframe
        except Exception as error:
            logging.error(f'error in read_snowflake_table  {error}')
            raise error.with_traceback(sys.exc_info()[2])

    def read_snowflake_query(self, spark, query, keep_column_case="on"):
        try:
            dataframe = spark\
                            .read \
                            .format("snowflake") \
                            .options(**self.options) \
                            .option("query", query) \
                            .option("keep_column_case", keep_column_case) \
                            .load()
            return dataframe
        except Exception as error:
            logging.error(f'error in read_snowflake_table  {error}')
            raise error.with_traceback(sys.exc_info()[2])

    def write_dataframe(self, dataframe, table_name, mode="append",
            truncate_table="off", keep_column_case="off",
            column_mismatch_behavior="ignore", preactions="", postactions=""):
        try:
            dataframe\
                .write \
                .format("snowflake") \
                .options(**self.options) \
                .option("dbtable", table_name) \
                .option("column_mapping","name") \
                .option("column_mismatch_behavior", column_mismatch_behavior) \
                .option("truncate_table", truncate_table) \
                .option("keep_column_case", keep_column_case) \
                .option("preactions", preactions) \
                .option("postactions", postactions) \
                .mode(mode) \
                .save()
        except Exception as error:
            logging.error(f'error in write_dataframe  {error}')
            raise error.with_traceback(sys.exc_info()[2])

    def foreach_batch_function(self, dataframe, table_name, mode, epoch_id):
        self.write_dataframe(dataframe, table_name, mode)

    def write_stream_to_snowflake(self, dataframe, table_name, mode,
                                  checkpoint_path,
                                  processing_time="20 seconds",
                                  query_name='write_to_snowflake'):
        dataframe\
            .writeStream \
            .foreachBatch(
            lambda df, epoch_id: self.foreach_batch_function(df, table_name,
                                                            mode, epoch_id)) \
            .option("checkpointLocation",
                    f'{checkpoint_path}/checkpoint/stream/scylla/') \
            .trigger(processingTime=processing_time) \
            .queryName(query_name) \
            .start()
