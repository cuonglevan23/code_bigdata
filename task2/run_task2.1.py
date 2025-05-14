#!/usr/bin/env python3
import argparse
import os
import pandas as pd
import geopandas as gpd
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(description="Task 2: Count crimes per ZIP and output shapefile for choropleth.")
    parser.add_argument("--parquet", required=True, help="Input Parquet file (with ZIPCode column)")
    parser.add_argument("--shapefile", required=True, help="ZIP Code shapefile path (.shp)")
    parser.add_argument("--output", default="ZIPCodeCrimeCount.shp", help="Output shapefile name")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    return parser.parse_args()


def main():
    args = parse_args()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Task2_ZIP_Crime_Count") \
        .master(args.master) \
        .getOrCreate()

    # Step 1: Load Parquet file
    df = spark.read.parquet(args.parquet)
    df.createOrReplaceTempView("crimes")

    # Step 2: Count crimes by ZIPCode
    crime_counts = spark.sql("""
        SELECT ZIPCode, COUNT(*) AS count
        FROM crimes
        GROUP BY ZIPCode
    """).toPandas()

    crime_counts["ZIPCode"] = crime_counts["ZIPCode"].astype(str)

    # Step 3: Load shapefile with GeoPandas
    gdf = gpd.read_file(args.shapefile)
    gdf["ZCTA5CE10"] = gdf["ZCTA5CE10"].astype(str)

    # Step 4: Merge GeoDataFrame with crime counts
    merged = gdf.merge(crime_counts, how="left", left_on="ZCTA5CE10", right_on="ZIPCode")
    merged["count"] = merged["count"].fillna(0).astype(int)

    # Step 5: Optional cleanup
    merged = merged.drop(columns=["ZIPCode"])  # Drop duplicate key column

    # Step 6: Ensure output folder exists
    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Step 7: Write to shapefile
    merged.to_file(args.output, encoding="utf-8")
    print(f"✅ Đã ghi shapefile với số lượng tội phạm theo ZIP: {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
