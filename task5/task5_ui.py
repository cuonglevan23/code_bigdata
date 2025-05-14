# task5_ui.py
import streamlit as st
from task5.main import run_experiment
from task5.create_spark_session import create_spark_session

import pandas as pd


def task5_ui():
    st.subheader("Task 5: Arrest Prediction using Machine Learning")

    # Chọn file Parquet
    parquet_10k = st.text_input("Parquet File Path (10K)", "output/Chicago_Crimes_10k_ZIP.parquet")
    parquet_100k = st.text_input("Parquet File Path (100K)", "output/Chicago_Crimes_100k_ZIP.parquet")

    # Chọn nơi lưu file CSV
    output_10k = st.text_input("Output CSV Path (10K)", "output/task5_predictions_10k.csv")
    output_100k = st.text_input("Output CSV Path (100K)", "output/task5_predictions_100k.csv")

    # Chọn loại mô hình
    model_type = st.selectbox("Choose a Model", ["logistic_regression", "random_forest"])

    # Nút chạy
    if st.button("Run Model Training and Evaluation"):
        spark = create_spark_session()

        # Chạy với cả 2 file
        results = []
        for parquet, output in zip([parquet_10k, parquet_100k], [output_10k, output_100k]):
            st.info(f"Running on dataset: {parquet}...")
            result = run_experiment(spark, parquet, output, model_type)
            results.append(result)

            st.success(f"✅ Done: {parquet}")
            st.write(f"⏱️ Time: {result['time']:.2f}s")
            st.write(f"🎯 Precision: {result['precision']:.4f}")
            st.write(f"📊 Recall: {result['recall']:.4f}")

            # Hiển thị mẫu dự đoán
            st.write("🔎 Sample Predictions:")
            sample_df = result["sample_df"].toPandas()
            st.dataframe(sample_df)

        spark.stop()
