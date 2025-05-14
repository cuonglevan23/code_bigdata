import pandas as pd

# Save processed data to CSV
def save_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

# Convert CSV to Spark DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

def convert_csv_to_spark_df(csv_path):
    spark = SparkSession.builder.appName("CrimeDataParquetWriter").getOrCreate()
    spark_df = spark.read.option("header", "true").csv(csv_path)
    spark_df = spark_df.withColumn("x", col("x").cast(DoubleType()))
    spark_df = spark_df.withColumn("y", col("y").cast(DoubleType()))
    return spark_df
