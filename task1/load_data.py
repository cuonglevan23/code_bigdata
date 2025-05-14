
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
from mongo.mongo_connector import insert_data_to_mongo


# Load crime data
def load_crime_data(csv_path):


    # Đọc CSV và bỏ qua dòng lỗi
    crime_df = pd.read_csv(csv_path, on_bad_lines='skip')  # pandas >= 1.3.0
    crime_df = crime_df.rename(columns=lambda x: x.strip().replace(" ", ""))

    crime_df["geometry"] = crime_df.apply(lambda row: Point(row["x"], row["y"]), axis=1)
    crime_gdf = gpd.GeoDataFrame(crime_df, geometry="geometry", crs="EPSG:4326")
    return crime_gdf

# Load ZIP shapefile
def load_zip_data(shapefile_path):
    zip_gdf = gpd.read_file(shapefile_path)
    zip_gdf = zip_gdf.to_crs('EPSG:4326')  # Ensure same CRS
    return zip_gdf



def load_and_insert_data(csv_path):
    """Load dữ liệu từ CSV vào pandas DataFrame, chuyển đổi thành dictionary và chèn vào MongoDB."""
    try:
        # Đọc dữ liệu từ CSV vào pandas DataFrame, bỏ qua các dòng không hợp lệ
        df = pd.read_csv(csv_path, on_bad_lines='skip')  # Bỏ qua các dòng có lỗi
        print(f"Dữ liệu đã được tải thành công với {len(df)} dòng.")

        # Chuyển đổi DataFrame thành danh sách các dictionary (định dạng phù hợp với MongoDB)
        data = df.to_dict(orient='records')  # Mỗi dòng sẽ là một dictionary

        # Chèn dữ liệu vào MongoDB
        insert_data_to_mongo(data)
        print(f"Dữ liệu đã được chèn vào MongoDB thành công: {len(data)} bản ghi.")

    except Exception as e:
        print(f"Lỗi khi tải và chèn dữ liệu: {e}")

