import pandas as pd
import matplotlib.pyplot as plt

# Đọc dữ liệu từ file CSV
df = pd.read_csv('/Users/lvc/PycharmProjects/Prolog_1/crime_analysis_project/output/RangeReportResult.csv/part-00000-e2fc21a9-24a0-44dd-bc85-88f979d323fa-c000.csv')

# Kiểm tra dữ liệu
print(df.head())

# Vẽ đồ thị
plt.figure(figsize=(10,6))
plt.scatter(df['x'], df['y'], c='blue', marker='o', alpha=0.5)
plt.title('Tội phạm trong phạm vi và thời gian')
plt.xlabel('X (Tọa độ X)')
plt.ylabel('Y (Tọa độ Y)')
plt.grid(True)
plt.show()
