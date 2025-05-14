from pyspark.sql.functions import concat_ws, col  # Thêm import col ở đây


def process_text_features(df):
    """
    Processes text features by combining 'Primary Type' and 'Description'.
    Args:
        df: DataFrame containing crime data.

    Returns:
        DataFrame with processed text column 'text'.
    """
    df = df.withColumn("text", concat_ws(" ", col("Primary Type"), col("Description")))
    return df
