#!/usr/bin/env python3
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp, col, lit
import pandas as pd
import matplotlib.pyplot as plt
import glob


def parse_args():
    parser = argparse.ArgumentParser(description="Task 3: Temporal analysis of crime data")
    parser.add_argument("--parquet", required=True, help="Input Parquet file")
    parser.add_argument("--start_date", required=True, help="Start date (MM/DD/YYYY)")
    parser.add_argument("--end_date", required=True, help="End date (MM/DD/YYYY)")
    parser.add_argument("--output", required=True, help="Output CSV file name")
    return parser.parse_args()


def main():
    args = parse_args()

    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("Crime Temporal Analysis") \
        .master("local[*]") \
        .getOrCreate()

    # Đọc dữ liệu từ Parquet
    df = spark.read.parquet(args.parquet)

    # Chuyển đổi cột Date thành timestamp
    df = df.withColumn("date", to_timestamp(col("date"), "MM/dd/yyyy hh:mm:ss a"))

    # Chuyển start_date và end_date thành định dạng ngày tháng
    start_date = to_date(lit(args.start_date), 'MM/dd/yyyy')
    end_date = to_date(lit(args.end_date), 'MM/dd/yyyy')

    # Lọc dữ liệu trong khoảng thời gian
    filtered_df = df.filter((col("date") >= start_date) & (col("date") <= end_date))

    # Đếm số lượng tội phạm theo PrimaryType
    crime_counts = filtered_df.groupBy("PrimaryType").count()

    # Lưu kết quả vào thư mục output
    output_path = "/Users/lvc/PycharmProjects/Prolog_1/crime_analysis_project/output/"
    os.makedirs(output_path, exist_ok=True)
    crime_counts.write.option("header", "true").mode("overwrite").csv(os.path.join(output_path, "CrimeTypeCount.csv"))


    print(f"✅ Đã lưu kết quả vào file CSV: {os.path.join(output_path, 'CrimeTypeCount.csv')}")

    # Đọc lại tất cả các file CSV trong thư mục
    csv_files = glob.glob(os.path.join(output_path, "CrimeTypeCount.csv/part-*.csv"))
    crime_counts_pd = pd.concat([pd.read_csv(f) for f in csv_files])

    # Vẽ biểu đồ
    plt.figure(figsize=(10, 6))
    plt.bar(crime_counts_pd['PrimaryType'], crime_counts_pd['count'])
    plt.xlabel('Crime Type')
    plt.ylabel('Count')
    plt.title(f'Crime Counts from {args.start_date} to {args.end_date}')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()

    # Dừng SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
