"""

"""
import logging
import pandas as pd


class ExcelConnector:
    def __init__(self, file_name, sheet_name):
        logging.info("reading excel")
        self.file_name=file_name
        self.sheet_name=sheet_name

    def read_excel(self, spark):
        try:
            excel_data_df = pd.read_excel(
            self.file_name,
            sheet_name=self.sheet_name,
            ).dropna(how='all', axis=1)
            excel_data_df = excel_data_df.where(pd.notnull(excel_data_df), None)
            df = spark.createDataFrame(excel_data_df)
            return df
        except:
            return spark.range(0).drop("id") # Empty DF


    def write_excel(self, df):
        pandas_df = df.toPandas()
        pandas_df.to_excel(self.file_name)
