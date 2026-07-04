import geopandas as gpd


gdf = gpd.read_file("../data/geo_data/ConstituencyBoundariesUngeneralised_National_Electoral_Boundaries_2023_1943258459586490165.geojson")

print(gdf.columns)
print(gdf.head())
print(gdf.crs)

print(gdf["ENG_NAME_VALUE"].sort_values().unique())
print(gdf["ENG_NAME_VALUE"].value_counts())