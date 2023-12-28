import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnnotationBbox, OffsetImage
from skimage import io
import geopandas as gpd
import contextily as ctx
import psycopg2
from config import config
matplotlib.use('TkAgg')
#%%
conn_details = None
try:
    params = config()
    print('Connecting to the postgreSQL database ...')
    conn_details = psycopg2.connect(**params)
except(Exception, psycopg2.DatabaseError) as error:
    print(error)

conn_details.autocommit = True
cursor = conn_details.cursor()



#%%
cursor.execute(
    '''
    SELECT
    swl.loc_key as city,swl.lat,swl.lon,wd.iconn
    FROM  student.ss_weather_detail wd
    JOIN ss_weather_locations swl
        ON wd.loc_key = swl.loc_key
    WHERE updated_date = CURRENT_DATE 
         ORDER BY updated_date desc, updated_time desc
;
''')
result = cursor.fetchall()
df = pd.DataFrame(result, columns=['city','lat', 'long', 'icon'])

#%%
# Map
# ref:https://max-coding.medium.com/create-a-weather-map-using-openweather-api-in-python-f048473ca6ae
# EPSG code:4326 - WGS 84, latitude/longitude coordinate system based on the Earth's center of mass, used
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.long, df.lat), crs=4326)
# plot city location marker
# Transform geometries to a new coordinate reference system.
# EPSG:3857 - Web Mercator projection used for display by many web-based mapping tools,
# including Google Maps and OpenStreetMap.
ax = gdf.to_crs(epsg=3857).plot(figsize=(12,8), color="black", alpha=0.3)

# add weather icon
def add_icon(row):
    # Image reading and writing via imread
    img = io.imread(f'https:{row.icon}')
    img_offset = OffsetImage(img, zoom=0.8, alpha=1, )
    ab = AnnotationBbox(img_offset, [row.geometry.x, row.geometry.y], frameon=False)
    ax.add_artist(ab)


gdf.to_crs(epsg=3857).apply(add_icon, axis=1)
gdf.to_crs(epsg=3857).apply(lambda x: ax.annotate
                                                 (text=f"{x.city.title()}  ",
                                                  fontsize=13,
                                                  color="black",
                                                  xy=x.geometry.centroid.coords[0],
                                                  ha='left'),
                            axis=1);

# add margins
xmin, ymin, xmax, ymax = gdf.to_crs(epsg=3857).total_bounds
y_margin = (ymax - ymin) * .2
x_margin = (xmax - xmin) * .2

ax.set_xlim(xmin - x_margin, xmax + x_margin)
ax.set_ylim(ymin - y_margin, ymax + y_margin)

# add basemap
ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)

ax.set_axis_off()
plt.title("Weather Map")
plt.show()
