import yaml, utils
import db_connection as db_con

import pandas as pd
import sqlalchemy
import time
import os, sys
from os.path import join, dirname

import datetime

import geopy
from geopy.geocoders import ArcGIS
from geopy.extra.rate_limiter import RateLimiter


# Connexion à pgsql
conn_pgsql_datalab = db_con.connect_to_db(config_file='config.yaml', section='pgsql_azure_olist', ssh=True, local_port=None, ssh_section= 'ssh_tunnel-azure')
# Test connect
# result = conn_pgsql_datalab.execute("""CREATE TABLE IF NOT EXISTS toto (
#    column1 VARCHAR(5) ,
#    column2 VARCHAR(5) ,
#    column3 VARCHAR(5)
# );""")


directory_path = os. getcwd() 
csv_folder = "Data"
csv_path = os.path.join(directory_path, csv_folder)
dirs = os.listdir(csv_path)
dtype = {'seller_zip_code_prefix': str,'customer_zip_code_prefix': str, 'geolocation_zip_code_prefix': str}
# for file in dirs:
#     print(file)
    
#     data_df = pd.read_csv(os.path.join(csv_path, file),dtype=dtype)
#     table_name = file .replace(".csv", "").replace("_dataset", "")
#     print(table_name,data_df.shape)
#     data_df.to_sql(table_name, con=conn_pgsql_datalab, index=False, index_label='id', if_exists='replace')
    
# création d'une table geolocalisation à partir de tout les zipcode unique dans toutes les tables

# list_zipcode = conn_pgsql_datalab.execute("""SELECT DISTINCT zip_code_prefix
# FROM (
#   SELECT customer_zip_code_prefix AS zip_code_prefix
#   FROM olist_customers
#   UNION
#   SELECT seller_zip_code_prefix AS zip_code_prefix
#   FROM olist_sellers
# ) AS all_zip_codes""")

# print(list_zipcode)

# execute the SQL query and read the result into a pandas DataFrame
df = pd.read_sql("""
    select *
from olist_sellers S
left join olist_geolocation_bis G
on (S.seller_zip_code_prefix=G.zip_code)
where G.zip_code is null;
""", conn_pgsql_datalab)

# print the resulting DataFrame
print(df.head(3))

from geopy.geocoders import Nominatim
import certifi
import ssl

context = ssl.create_default_context(cafile=certifi.where())
# create a geolocator object
locator = Nominatim(user_agent="google", ssl_context=context)

# iterate over the rows of the DataFrame
for index, row in df.iterrows():
    # get the zip code from the current row
    zip_code = row['seller_zip_code_prefix']
    
    # geocode the zip code
    location = locator.geocode(f"{zip_code}, Brazil")
    
    # if location is not None and is a Location object, extract latitude, longitude, city, and state and add them to the DataFrame
    if location is not None and type(location) == geopy.location.Location:
        latitude = location.latitude
        longitude = location.longitude
        city = location.raw.get('address', {}).get('city')
        state = location.raw.get('address', {}).get('state')
        
        df.at[index, 'latitude'] = latitude
        df.at[index, 'longitude'] = longitude
        
        
    # print the current row and its geocoded location
    print(f"Row {index}: {row}")
    print(f"Location: {location}")
        
# print the final DataFrame
print(df[])