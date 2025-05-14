import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp, col, lit


def crime_temporal_analysis(parquet_file, start_date, end_date, output_dir):
    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("Crime Temporal Analysis") \
        .master("local[*]") \
        .getOrCreate()

    # Đọc dữ liệu từ Parquet
    df = spark.read.parquet(parquet_file)

    # Chuyển đổi cột Date thành timestamp
    df = df.withColumn("date", to_timestamp(col("date"), "MM/dd/yyyy hh:mm:ss a"))

    # Chuyển start_date và end_date thành định dạng ngày tháng
    start_date_lit = to_date(lit(start_date), 'MM/dd/yyyy')
    end_date_lit = to_date(lit(end_date), 'MM/dd/yyyy')

    # Lọc dữ liệu trong khoảng thời gian
    filtered_df = df.filter((col("date") >= start_date_lit) & (col("date") <= end_date_lit))

    # Đếm số lượng tội phạm theo "Primary Type"
    crime_counts = filtered_df.groupBy("Primary Type").count()

    # Tạo thư mục nếu chưa tồn tại và lưu kết quả
    os.makedirs(output_dir, exist_ok=True)
    crime_counts.write.option("header", "true").mode("overwrite").csv(output_dir)
    st.success(f"✅ Đã lưu kết quả vào thư mục: {output_dir}")

    # Đọc lại các file part CSV
    csv_files = glob.glob(os.path.join(output_dir, "part-*.csv"))
    if csv_files:
        crime_counts_pd = pd.concat([pd.read_csv(f) for f in csv_files])

        # Vẽ biểu đồ
        plt.figure(figsize=(10, 6))
        plt.bar(crime_counts_pd['Primary Type'], crime_counts_pd['count'])
        plt.xlabel('Crime Type')
        plt.ylabel('Count')
        plt.title(f'Crime Counts from {start_date} to {end_date}')
        plt.xticks(rotation=90)
        plt.tight_layout()
        st.pyplot(plt)  # DÙNG ĐÚNG CÁCH TRONG STREAMLIT
    else:
        st.error(f"❌ Không tìm thấy tệp CSV trong thư mục: {output_dir}")

    spark.stop()
