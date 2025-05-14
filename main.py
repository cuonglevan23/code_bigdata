#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import glob
from task1 import plot_map
from task2 import dataset_size_comparison
from task3 import crime_temporal_analysis
from task5.task5_ui import task5_ui
# Thiết lập cấu hình trang
st.set_page_config(layout="wide")
st.title("Crime Data Analysis")

# Chọn task từ menu điều hướng
task = st.sidebar.radio("Choose a Task", [
    "Task 1: Crime Map by ZIP Code",
    "Task 2: Crime Data Size Comparison",
    "Task 3: Temporal Analysis of Crime Data",
    "Task 4: Crime Cases by Location",
    "Task 5: Arrest Prediction"]) # Thêm Task 4


# Task 1: Hiển thị bản đồ
if task == "Task 1: Crime Map by ZIP Code":
    plot_map()

# Task 2: Hiển thị so sánh kích thước dữ liệu
elif task == "Task 2: Crime Data Size Comparison":
    dataset_size_comparison()

# Task 3: Hiển thị phân tích theo thời gian
elif task == "Task 3: Temporal Analysis of Crime Data":
    st.subheader("Temporal Analysis of Crime Data")

    # Nhận đầu vào từ người dùng cho các tham số
    parquet_file = st.text_input("Parquet File Path", "output/Chicago_Crimes_10k_ZIP.parquet")
    start_date = st.date_input("Start Date", value=pd.to_datetime("01/01/2015"))
    end_date = st.date_input("End Date", value=pd.to_datetime("12/31/2020"))
    output_file = st.text_input("Output Directory", "output/CrimeTypeCount")  # không có .csv

    st.write("Parquet File Path:", parquet_file)
    st.write("Start Date:", start_date)
    st.write("End Date:", end_date)
    st.write("Output File Path:", output_file)

    # Chạy phân tích khi người dùng nhấn nút
    if st.button("Run Temporal Analysis"):
        crime_temporal_analysis(parquet_file, start_date.strftime('%m/%d/%Y'), end_date.strftime('%m/%d/%Y'),
                                output_file)

# Task 4: Hiển thị scatter plot các vụ án theo tọa độ
elif task == "Task 4: Crime Cases by Location":
    st.subheader("Crime Cases by Location")

    # Đường dẫn mặc định đến file CSV
    default_csv_path = "/Users/lvc/PycharmProjects/Prolog_1/crime_analysis_project/output/RangeReportResult.csv/part-00000*.csv"
    csv_file = st.text_input("CSV File Path", default_csv_path)

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
        st.write("Dưới đây là 5 dòng đầu tiên của dữ liệu:")
        st.dataframe(df.head())

        # Vẽ scatter plot
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(df['x'], df['y'], alpha=0.5, c='red', edgecolor='k')
        ax.set_title("Crime Cases by Location")
        ax.set_xlabel("X Coordinate")
        ax.set_ylabel("Y Coordinate")
        ax.grid(True)

        # Hiển thị biểu đồ trên Streamlit
        st.pyplot(fig)

        # Hiển thị thông tin bổ sung
        st.write(f"Total number of crime cases: {len(df)}")


# Thêm vào logic hiển thị:
elif task == "Task 5: Arrest Prediction":
    task5_ui()