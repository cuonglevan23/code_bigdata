#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import glob

# Thiết lập tiêu đề và mô tả cho ứng dụng Streamlit
st.title("Crime Cases Visualization")
st.markdown("This application visualizes crime cases by their X and Y coordinates from a CSV file.")

# Đường dẫn đến file CSV
csv_file = "/Users/lvc/PycharmProjects/Prolog_1/crime_analysis_project/output/RangeReportResult.csv/part-00000*.csv"

# Dùng glob để tìm file CSV
files = glob.glob(csv_file)

# Kiểm tra xem file có tồn tại hay không
if not files:
    st.error("❌ Không tìm thấy file CSV trong thư mục được chỉ định.")
else:
    # Đọc file CSV
    df = pd.read_csv(files[0])

    # Đổi tên cột và làm sạch tên cột
    df.columns = [col.strip() for col in df.columns]  # Xóa khoảng trắng trong tên cột
    df.rename(columns={
        'XCoordinate': 'x',
        'YCoordinate': 'y',
        'CaseNumber': 'CaseNumber',
        'date': 'Date'
    }, inplace=True)

    # Hiển thị dữ liệu đầu tiên
    st.subheader("Sample Data")
    st.write("Dưới đây là 5 dòng đầu tiên của dữ liệu:")
    st.dataframe(df.head())

    # Vẽ scatter plot
    st.subheader("Crime Cases by Location")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(df['x'], df['y'], alpha=0.5, c='red', edgecolor='k')
    ax.set_title("Crime Cases by Location")
    ax.set_xlabel("X Coordinate")
    ax.set_ylabel("Y Coordinate")
    ax.grid(True)

    # Hiển thị biểu đồ trên Streamlit
    st.pyplot(fig)

    # Thêm thông tin bổ sung
    st.write(f"Total number of crime cases: {len(df)}")