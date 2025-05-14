import pandas as pd
import matplotlib.pyplot as plt

# Đường dẫn đến file CSV thực tế bên trong thư mục output
csv_file = "/Users/lvc/PycharmProjects/Prolog_1/crime_analysis_project/output/RangeReportResult.csv/part-00000*.csv"

# Dùng glob để tự động tìm file
import glob
files = glob.glob(csv_file)
if not files:
    print("❌ Không tìm thấy file CSV")
    exit()

# Đọc file CSV
df = pd.read_csv(files[0])

# Đổi tên cột nếu cần thiết (tuỳ thuộc vào output Spark)
df.columns = [col.strip() for col in df.columns]  # clean header whitespace
df.rename(columns={
    'XCoordinate': 'x',
    'YCoordinate': 'y',
    'CaseNumber': 'CaseNumber',
    'date': 'Date'
}, inplace=True)

# Kiểm tra dữ liệu
print(df.head())

# Vẽ scatter plot
plt.figure(figsize=(10, 6))
plt.scatter(df['x'], df['y'], alpha=0.5, c='red', edgecolor='k')
plt.title("Crime Cases by Location")
plt.xlabel("X Coordinate")
plt.ylabel("Y Coordinate")
plt.grid(True)
plt.tight_layout()
plt.show()
