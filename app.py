from mongo.mongo_connector import get_mongo_client  # Import kết nối MongoDB

import streamlit as st
import pandas as pd
import os

st.title("Chicago Crime Data Size Comparison")

def get_dir_size(path):
    total = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total

# File paths
csv_1k = "data/Chicago_Crimes_1k.csv"
csv_10k = "data/Chicago_Crimes_10k.csv"
parquet_1k = "output/Chicago_Crimes_1k_ZIP.parquet"
parquet_10k = "output/Chicago_Crimes_10k_ZIP.parquet"
csv_100k = "data/Chicago_Crimes_100k.csv"
parquet_100k = "output/Chicago_Crimes_100k_ZIP.parquet"
# Get file sizes
csv_1k_size = os.path.getsize(csv_1k) if os.path.exists(csv_1k) else 0
csv_10k_size = os.path.getsize(csv_10k) if os.path.exists(csv_10k) else 0
parquet_1k_size = get_dir_size(parquet_1k) if os.path.exists(parquet_1k) else 0
parquet_10k_size = get_dir_size(parquet_10k) if os.path.exists(parquet_10k) else 0
csv_100k_size = os.path.getsize(csv_100k) if os.path.exists(csv_100k) else 0
parquet_100k_size = get_dir_size(parquet_100k) if os.path.exists(parquet_100k) else 0

# Create comparison DataFrame
comparison_df = pd.DataFrame([
    {
        "Dataset": "1,000",
        "CSV size (MB)": csv_1k_size / 1024**2,
        "Parquet size (MB)": parquet_1k_size / 1024**2
    },
    {
        "Dataset": "10,000",
        "CSV size (MB)": csv_10k_size / 1024**2,
        "Parquet size (MB)": parquet_10k_size / 1024**2
    },
    {
        "Dataset": "100,000",
        "CSV size (MB)": csv_100k_size / 1024**2,
        "Parquet size (MB)": parquet_100k_size / 1024**2
    },
])


st.subheader("📊 Dataset Size Comparison (CSV vs Parquet)")
st.table(comparison_df.style.format({"CSV size (MB)": "{:.2f}", "Parquet size (MB)": "{:.2f}"}))



# ——————————————————————————————————————————————
# 2. Load dữ liệu và hiển thị chart
# ——————————————————————————————————————————————
# Kết nối đến MongoDB
client     = get_mongo_client()      # Lấy client MongoDB
db         = client["crime_data"]    # Chọn database
collection = db["crimes"]            # Chọn collection

# Lấy dữ liệu từ MongoDB
data = list(collection.find({}, {'_id': 0}))
if data:
    df = pd.DataFrame(data)

    st.subheader("Crime Data Sample")
    st.dataframe(df.head(10))

    # Thống kê theo năm
    if "Year" in df.columns:
        st.subheader("Crime Count by Year")
        # Ép kiểu Year trước khi vẽ
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
        st.bar_chart(df["Year"].value_counts().sort_index())
else:
    st.write("No data available.")
