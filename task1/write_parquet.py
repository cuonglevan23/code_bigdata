def write_parquet(spark_df, output_path):
    spark_df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Task complete: Parquet file written to: {output_path}")
