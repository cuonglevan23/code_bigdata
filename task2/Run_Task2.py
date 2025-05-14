#!/usr/bin/env python3
import argparse
import os
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext
from sedona.utils import KryoSerializer, SedonaKryoRegistrator
from sedona.core.formatMapper.shapefileParser import ShapefileReader
from sedona.utils.adapter import Adapter
import geopandas as gpd
import shapely


def parse_args():
    parser = argparse.ArgumentParser(description="Task 2: Count crimes per ZIP and output shapefile for choropleth.")
    parser.add_argument("--parquet", required=True, help="Input Parquet file (with ZIPCode column)")
    parser.add_argument("--shapefile", required=True, help="ZIP Code shapefile path (.shp)")
    parser.add_argument("--output", default="ZIPCodeCrimeCount.shp", help="Output shapefile name")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    return parser.parse_args()


def main():
    args = parse_args()

    spark = SparkSession.builder \
        .appName("Task2_ZIP_Crime_Count") \
        .master(args.master) \
        .config("spark.serializer", KryoSerializer.getName) \
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
        .config("spark.jars.packages",
                "org.apache.sedona:sedona-spark-3.1_2.12:1.3.0,"
                "org.datasyslab:geotools-wrapper:1.5.1-28.2")\
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2/") \
        .getOrCreate()

    # Tạo SedonaContext
    sedona = SedonaContext.create(spark)

    # **Bước 1: Tải dữ liệu tội phạm từ Parquet**
    df = spark.read.parquet(args.parquet)
    df.createOrReplaceTempView("crimes")

    # **Bước 2: Đếm số lượng tội phạm theo ZIPCode**
    crime_counts = spark.sql("""
        SELECT ZIPCode, COUNT(*) AS count
        FROM crimes
        GROUP BY ZIPCode
    """)
    crime_counts = crime_counts.coalesce(1)  # Giảm xuống 1 partition

    # **Bước 3: Tải shapefile ZIP Code bằng Sedona**
    spatial_rdd = ShapefileReader.readToGeometryRDD(spark.sparkContext, args.shapefile)
    zip_df = Adapter.toDf(spatial_rdd, spark)
    zip_df = zip_df.select("ZCTA5CE10", "geometry")  # Chọn các cột cần thiết

    # **Bước 4: Thực hiện equi-join trên ZIPCode và ZCTA5CE10**
    joined_df = zip_df.join(
        crime_counts,
        zip_df.ZCTA5CE10 == crime_counts.ZIPCode,
        "left"
    ).select(
        zip_df.ZCTA5CE10,
        zip_df.geometry,
        crime_counts["count"]
    )

    # Thay giá trị NULL trong cột count bằng 0
    joined_df = joined_df.fillna({"count": 0})

    # **Bước 5: Chuyển DataFrame thành GeoPandas để ghi shapefile**
    pdf = joined_df.toPandas()

    # Kiểm tra và chuyển đổi geometry nếu cần thiết
    pdf['geometry'] = pdf['geometry'].apply(lambda x: shapely.wkb.loads(x) if isinstance(x, bytes) else x)

    gdf = gpd.GeoDataFrame(
        pdf,
        geometry=gpd.GeoSeries.from_wkb(pdf["geometry"]),
        crs="EPSG:4326"
    )
    gdf["count"] = gdf["count"].astype(int)  # Đảm bảo cột count là kiểu integer
    gdf["ZCTA5CE10"] = gdf["ZCTA5CE10"].astype(str)  # ZIP code là string

    # **Bước 6: Tạo thư mục output nếu chưa tồn tại**
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    # **Bước 7: Ghi kết quả ra shapefile**
    gdf.to_file(args.output)
    print(f"✅ Đã ghi shapefile với số lượng tội phạm theo ZIP: {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
