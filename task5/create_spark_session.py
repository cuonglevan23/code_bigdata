from pyspark.sql import SparkSession

def create_spark_session():
    """
    Creates and returns a SparkSession.
    """
    spark = SparkSession.builder.appName("ArrestPrediction").getOrCreate()
    return spark
