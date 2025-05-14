from pyspark.sql.functions import col


def load_and_process_data(spark, parquet_path):
    """
    Loads and processes data from a parquet file.
    Args:
        spark: SparkSession
        parquet_path: path to the parquet file

    Returns:
        Processed DataFrame
    """
    df = spark.read.parquet(parquet_path)
    df = df.filter((col("Arrest") == "true") | (col("Arrest") == "false"))
    df = df.select("Primary Type", "Description", "Arrest")
    return df
