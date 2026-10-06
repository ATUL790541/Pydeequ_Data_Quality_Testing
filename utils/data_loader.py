from pyspark.sql import SparkSession
import pandas as pd
import io

#This version was used when I was able to read schema sheet.This is older version 
# def load_input_data(table, sheet_name, schema_path):
#     spark = SparkSession.getActiveSession()

#     # Load table
#     df = spark.sql(f"SELECT * FROM {table}")

#     # Load schema
#     binary_df = spark.read.format("binaryFile").load(schema_path)
#     file_bytes = binary_df.select("content").collect()[0][0]

#     excel_file = pd.ExcelFile(io.BytesIO(file_bytes))

#     base_df = pd.read_excel(
#         excel_file,
#         sheet_name=sheet_name,
#         header=1
#     )

#     return df, base_df


import io
import pandas as pd
from pyspark.sql import SparkSession


def load_input_data(table, sheet_name, schema_path):
    spark = SparkSession.getActiveSession()

    # Load table
    df = spark.sql(f"SELECT * FROM {table}")

    # Load schema (non-fatal: return None if the path can't be read/authorized)
    base_df = None
    try:
        binary_df = spark.read.format("binaryFile").load(schema_path)
        file_bytes = binary_df.select("content").collect()[0][0]

        excel_file = pd.ExcelFile(io.BytesIO(file_bytes))

        base_df = pd.read_excel(
            excel_file,
            sheet_name=sheet_name,
            header=1
        )
    except Exception as e:
        print(f"WARNING: could not read schema file {schema_path}: {e}")
        base_df = None

    return df, base_df
