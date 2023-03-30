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

df = pd.read_sql("""SELECT DISTINCT zip_code_prefix, city
FROM (
  SELECT DISTINCT customer_zip_code_prefix AS zip_code_prefix , customer_city AS city
  FROM olist_customers
  UNION
  SELECT DISTINCT seller_zip_code_prefix AS zip_code_prefix, seller_city AS city
  FROM olist_sellers
) AS all_zip_codes""", conn_pgsql_datalab)

print(df.head(3))

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
# locator = Nominatim(user_agent="google", ssl_context=context, timeout=3)

# # iterate over the rows of the DataFrame
# for index, row in df.iterrows():
#     # get the zip code from the current row
#     zip_code = row['zip_code_prefix']
#     city = row['city']
#     print("zip_code_recherché: ",zip_code, "city_recherché: ", city)
#     # geocode the zip code
#     location = locator.geocode(f"{zip_code}, Brazil")
    
#     # if location is not None and is a Location object, extract latitude, longitude, city, and state and add them to the DataFrame
#     if location is not None and type(location) == geopy.location.Location:
#         print(location.raw)
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
# print(df)


########################################## Maj de la Table product_category_name_translation ajout trad french ########################################################

#Lecture de la table actuelle
traduction_df = pd.read_sql("""
select *
from product_category_name_translation T;
""", conn_pgsql_datalab)

# print the resulting DataFrame
print(traduction_df.head(3))

# Création d'une nouvelle Df product_traduction_df que l'on parrcoura pour maj la table sql 
product_traduction_df = pd.read_sql("""
select distinct(product_category_name)
from olist_products P;
""", conn_pgsql_datalab)


# print the resulting DataFrame
print(product_traduction_df['product_category_name'].head(3))


print("--------------------------- Utilisation API traduction --------------------------------------")

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
product_traduction_df['product_category_name_english'] = product_traduction_df['product_category_name'].str.replace("_", " ").apply(get_english_translation).str.replace(" ", "_")
product_traduction_df['product_category_name_french'] = product_traduction_df['product_category_name'].str.replace("_", " ").apply(get_french_translation).str.replace(" ", "_")

# Print the updated dataframe

# print(product_traduction_df)

#creation de la colonne product_category_name_french
create_column_query = """ALTER TABLE product_category_name_translation
                                ADD COLUMN IF NOT EXISTS product_category_name_french TEXT;"""
conn_pgsql_datalab.execute(create_column_query)



for index, row in product_traduction_df.iterrows():
    try:
        print(row['product_category_name'], '  ', row['product_category_name_english'], '  ', row['product_category_name_french'])
        if row['product_category_name_english'] is not None:
            row['product_category_name_english'] = row['product_category_name_english'].replace("'", "''")
        else:
            row['product_category_name_english'] = None
        if row['product_category_name_french'] is not None:
            row['product_category_name_french'] = row['product_category_name_french'].replace("'", "''")
        else:
            row['product_category_name_french'] = None
        update_query_trad = (f"""
            INSERT INTO product_category_name_translation (product_category_name, product_category_name_english, product_category_name_french )
            VALUES ('{row['product_category_name']}', '{row['product_category_name_english']}', '{row['product_category_name_french']}')
            ON CONFLICT (product_category_name)
            DO UPDATE SET 
                product_category_name_french = EXCLUDED.product_category_name_french,
                product_category_name_english = EXCLUDED.product_category_name_english;
        """)
        conn_pgsql_datalab.execute(update_query_trad)

    except ValueError:
        pass
