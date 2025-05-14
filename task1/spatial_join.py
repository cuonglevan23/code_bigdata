import geopandas as gpd

# Spatial join để gán mã ZIP cho mỗi vụ tội phạm
def spatial_join(crime_gdf, zip_gdf):
    joined = gpd.sjoin(crime_gdf, zip_gdf[['ZCTA5CE10', 'geometry']], how='left', predicate='within')
    joined = joined.rename(columns={'ZCTA5CE10': 'ZIPCode'})
    joined = joined.drop(columns=['geometry', 'index_right'])
    return joined
