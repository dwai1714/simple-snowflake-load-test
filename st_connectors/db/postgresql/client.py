"""

"""
import logging

import psycopg2
import psycopg2.extras


class PostgresConnector:

    def __init__(self, host=None, username=None, password=None, dbname='postgres', port=5432):
        logging.info("Initiating postgres database connection")
        self.connection = None
        try:
            using = f"dbname='{dbname}' user='{username}' host='{host}' " \
                    f"password='{password}' port='{port}'"
            self._db_connection = psycopg2.connect(using)
            self._db_cur = self._db_connection.cursor(
                cursor_factory=psycopg2.extras.DictCursor)
            self.url = f'jdbc:postgresql://{host}:{port}/{dbname}'

            self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            self.properties = {"user": username,
                          "password": password,
                          "driver": "org.postgresql.Driver"}
            logging.info(
                f"Connection established to postgres database using {using}")
        except psycopg2.Error as err:
            logging.error(
                "Database connection failed due to postgres state {}".format(
                    " ".join(err.args)))

    def __exit__(self):
        logging.info("Closing postgres database connection")
        self._db_connection.close()
        return self._db_cur.close()

    def execute_query(self, query):
        logging.debug(f'Executing {query} on postgres')
        try:
            self._db_cur.execute(query)
            result = self._db_cur
        except Exception as error:
            logging.error(f'error executing query "{query}", error: {error}')
            return None
        else:
            return result

    def execute_commit(self):
        try:
            self._db_connection.commit()
        except Exception as error:
            logging.error(f'error in db commit {error}')
            return True
        else:
            return False

    def read_and_return_as_df(self, spark, query):
        try:
            df = spark.read.jdbc(url=self.url, table=query, properties=self.properties)
            return df
        except Exception as error:
            logging.error(f'error executing query read_and_return_as_df, error: {error}')
            return None

    def write_dataframe(self, df, table_name, mode="append"):
        try:
            df.write.jdbc(url=self.url, table=table_name, mode=mode, properties=self.properties)
        except Exception as error:
            logging.error(f'error executing query write_dataframe, error: {error}')
            return None
