import yaml, utils
import db_connection as db_con

import pandas as pd
import sqlalchemy
import time
import os, sys
from os.path import join, dirname

import datetime
# Geocoding
import geopy
from geopy.geocoders import ArcGIS
from geopy.extra.rate_limiter import RateLimiter
# traduction
from googletrans import Translator
import pandas as pd

# Connexion à pgsql
conn_pgsql_datalab = db_con.connect_to_db(config_file='config.yaml', section='pgsql_azure_olist', ssh=True, local_port=None, ssh_section= 'ssh_tunnel-azure')
# Test connect
# result = conn_pgsql_datalab.execute("""CREATE TABLE IF NOT EXISTS toto (
#    column1 VARCHAR(5) ,
#    column2 VARCHAR(5) ,
#    column3 VARCHAR(5)
# );""")


# directory_path = os. getcwd() 
# csv_folder = "Data"
# csv_path = os.path.join(directory_path, csv_folder)
# dirs = os.listdir(csv_path)
# dtype = {'seller_zip_code_prefix': str,'customer_zip_code_prefix': str, 'geolocation_zip_code_prefix': str}
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

# # execute the SQL query and read the result into a pandas DataFrame
# df = pd.read_sql("""
#     select *
# from olist_sellers S
# left join olist_geolocation_bis G
# on (S.seller_zip_code_prefix=G.zip_code)
# where G.zip_code is null;
# """, conn_pgsql_datalab)

# # print the resulting DataFrame
# print(df.head(3))

# from geopy.geocoders import Nominatim
# import certifi
# import ssl

# context = ssl.create_default_context(cafile=certifi.where())
# # create a geolocator object
# locator = Nominatim(user_agent="google", ssl_context=context)

# # iterate over the rows of the DataFrame
# for index, row in df.iterrows():
#     # get the zip code from the current row
#     zip_code = row['seller_zip_code_prefix']
    
#     # geocode the zip code
#     location = locator.geocode(f"{zip_code}, Brazil")
    
#     # if location is not None and is a Location object, extract latitude, longitude, city, and state and add them to the DataFrame
#     if location is not None and type(location) == geopy.location.Location:
#         latitude = location.latitude
#         longitude = location.longitude
#         city = location.raw.get('address', {}).get('city')
#         state = location.raw.get('address', {}).get('state')
        
#         df.at[index, 'latitude'] = latitude
#         df.at[index, 'longitude'] = longitude
        
        
#     # print the current row and its geocoded location
#     print(f"Row {index}: {row}")
#     print(f"Location: {location}")
        
# # print the final DataFrame
# print(df[])

traduction_df = pd.read_sql("""
select *
from product_category_name_translation T;
""", conn_pgsql_datalab)

# print the resulting DataFrame
print(traduction_df.head(3))

product_traduction_df = pd.read_sql("""
select distinct(product_category_name)
from olist_products P;
""", conn_pgsql_datalab)
product_traduction_df= product_traduction_df.apply(lambda x: x.replace("_"," ")
# print the resulting DataFrame
print(product_traduction_df.head(3))


print("-----------------------------------------------------------------")

# Initialize the translator
translator = Translator(service_urls=['translate.google.com'])

# Define a function to get the English translation
def get_english_translation(text):
    if text is None:
        return None
    else:
        return translator.translate(text).text

# Define a function to get the French translation
def get_french_translation(text):
    if text is None:
        return None
    else:
        return translator.translate(text, dest='fr').text

# Add the translation columns to the dataframe
product_traduction_df['product_category_name_english'] = product_traduction_df['product_category_name'].apply(get_english_translation)
product_traduction_df['product_category_name_french'] = product_traduction_df['product_category_name'].apply(get_french_translation)

# Print the updated dataframe

print(product_traduction_df)

