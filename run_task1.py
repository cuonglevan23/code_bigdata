#!/usr/bin/env python3
import argparse
import os
import geopandas as gpd
from shapely.geometry import Point
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DoubleType

from mongo.mongo_connector import insert_data_to_mongo
from models.crime import Crime

def parse_args():
    parser = argparse.ArgumentParser(
        description="Spatial join crime data with ZIP codes and load into MongoDB for multiple datasets."
    )
    parser.add_argument(
        "--inputs", nargs="+", default=[
            "data/Chicago_Crimes_1k.csv",
            "data/Chicago_Crimes_10k.csv",
            "data/Chicago_Crimes_100k.csv"
        ],
        help="List of input CSV files (default: 1k, 10k, 100k)"
    )
    parser.add_argument(
        "--shapefile", default="data/tl_2018_us_zcta510/tl_2018_us_zcta510.shp",
        help="Path to ZIP shapefile"
    )
    parser.add_argument(
        "--output_dir", default="output",
        help="Directory to write Parquet files"
    )
    parser.add_argument(
        "--mongo_collection_prefix", default="crimes",
        help="Prefix for MongoDB collection names"
    )
    return parser.parse_args()


def process_file(csv_file, shapefile, output_dir, mongo_collection):
    base_name = os.path.splitext(os.path.basename(csv_file))[0]
    spark = SparkSession.builder \
        .appName(f"CrimeZIPJoin_{base_name}") \
        .master("local[*]") \
        .getOrCreate()

    # Load shapefile
    zip_gdf = gpd.read_file(shapefile).to_crs("EPSG:4326")
    zip_polygons = zip_gdf[["ZCTA5CE10", "geometry"]].to_dict(orient="records")
    b_zip_polygons = spark.sparkContext.broadcast(zip_polygons)

    # UDF
    def find_zip(x, y):
        if x is None or y is None:
            return None
        pt = Point(float(x), float(y))
        for rec in b_zip_polygons.value:
            if rec["geometry"].contains(pt):
                return rec["ZCTA5CE10"]
        return None
    find_zip_udf = udf(find_zip, StringType())

    # Read CSV
    crime_df = spark.read.option("header", "true").csv(csv_file) \
        .withColumn("x",  col("x").cast(DoubleType())) \
        .withColumn("y",  col("y").cast(DoubleType()))

    # Add ZIP
    crime_with_zip = crime_df.withColumn("ZIPCode", find_zip_udf(col("x"), col("y")))

    # Write Parquet
    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, f"{base_name}_ZIP.parquet")
    crime_with_zip.write.mode("overwrite").parquet(parquet_path)
    print(f"✅ Written Parquet: {parquet_path}")

    crime_with_zip.show(3, truncate=False)

    # To Pandas and rename
    pandas_df = crime_with_zip.toPandas().rename(columns={
        "Case Number": "CaseNumber",
        "Primary Type": "PrimaryType",
        "Location Description": "LocationDescription",
        "Community Area": "CommunityArea",
        "FBI Code": "FBICode",
        "X Coordinate": "XCoordinate",
        "Y Coordinate": "YCoordinate",
        "Updated On": "UpdatedOn"
    })
    print(f"✅ Converted to Pandas: {len(pandas_df)} rows.")

    # Normalize and insert
    docs = []
    for _, row in pandas_df.iterrows():
        crime = Crime.from_series(row)
        if crime:
            docs.append(crime.to_dict())
    insert_data_to_mongo(docs, collection_name=mongo_collection)
    print(f"✅ Inserted {len(docs)} docs into MongoDB collection '{mongo_collection}'")

    spark.stop()


def main():
    args = parse_args()
    for input_file in args.inputs:
        suffix = os.path.splitext(os.path.basename(input_file))[0]
        collection_name = f"{args.mongo_collection_prefix}_{suffix}"
        print(f"\n--- Processing {input_file} -> collection {collection_name} ---")
        process_file(
            csv_file=input_file,
            shapefile=args.shapefile,
            output_dir=args.output_dir,
            mongo_collection=collection_name
        )

if __name__ == "__main__":
    main()