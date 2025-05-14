# task1.py
import geopandas as gpd
import folium
from streamlit_folium import st_folium
import streamlit as st

# Đường dẫn đến shapefile đầu ra từ Task 2
SHAPEFILE_PATH = "output/ZIPCodeCrimeCount.shp"
ZIP_COLUMN = "ZCTA5CE10"
COUNT_COLUMN = "count"
CHICAGO_COORDINATES = [41.8781, -87.6298]
ZOOM_LEVEL = 10

# Hàm tải dữ liệu với caching
@st.cache_data
def load_data():
    gdf = gpd.read_file(SHAPEFILE_PATH)
    gdf['geometry'] = gdf['geometry'].simplify(tolerance=0.01)  # Giảm độ chi tiết để tăng hiệu suất
    chicago_zip_codes = ['60601', '60602', '60603', '60604', '60605']
    gdf = gdf[gdf[ZIP_COLUMN].isin(chicago_zip_codes)]
    return gdf

# Hàm vẽ bản đồ
def plot_map():
    with st.spinner("Đang tải dữ liệu..."):
        gdf = load_data()

    # Hiển thị bảng dữ liệu nếu cần
    if st.checkbox("Hiển thị bảng dữ liệu"):
        st.dataframe(gdf[[ZIP_COLUMN, COUNT_COLUMN]])

    # Tạo bản đồ Folium với tọa độ của Chicago
    m = folium.Map(location=CHICAGO_COORDINATES, zoom_start=ZOOM_LEVEL, tiles="CartoDB positron")

    # Thêm lớp choropleth
    folium.Choropleth(
        geo_data=gdf,
        name="choropleth",
        data=gdf,
        columns=[ZIP_COLUMN, COUNT_COLUMN],
        key_on=f"feature.properties.{ZIP_COLUMN}",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="Crime Count",
    ).add_to(m)

    # Thêm popup để hiện chi tiết
    folium.GeoJson(
        gdf,
        name="ZIPs",
        tooltip=folium.GeoJsonTooltip(fields=[ZIP_COLUMN, COUNT_COLUMN], aliases=["ZIP Code", "Crime Count"])
    ).add_to(m)

    # Hiển thị bản đồ trên Streamlit
    st_folium(m, width=1000, height=600)
