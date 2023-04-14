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

#contour geojson
from urllib.request import urlopen
import json

# Connexion à pgsql
#conn_pgsql_datalab = db_con.connect_to_db(config_file='config.yaml', section='pgsql_azure_olist', ssh=True, local_port=None, ssh_section= 'ssh_tunnel-azure')
conn_pgsql_datalab = db_con.connect_to_db(config_file='config.yaml', section='pgsql_azure_olist', ssh=False, local_port=None, ssh_section= 'ssh_tunnel-azure')
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
#     if file.endswith(".csv"):
#       data_df = pd.read_csv(os.path.join(csv_path, file),dtype=dtype)
#       table_name = file .replace(".csv", "").replace("_dataset", "")
#       data_df.to_sql(table_name, con=conn_pgsql_datalab, index=False, index_label='id', if_exists='replace')
#       print(f"{table_name} table created with {data_df.shape[0]} rows.")

# ###################### Création table olist_geolocation_bis + insert #####################################
# create_table_olist_geolocation_bis = """CREATE TABLE IF NOT EXISTS public.olist_geolocation_bis (
#     zip_code character varying(5) NOT NULL,
#     geolocation_lat double precision,
#     geolocation_lng double precision,
#     n smallint,
#     geolocation_state character varying(2)
# );"""
# conn_pgsql_datalab.execute(create_table_olist_geolocation_bis)

# insert_table_olist_geolocation_bis = """INSERT INTO olist_geolocation_bis (
#     SELECT 
#         zc.zip_code_prefix, 
#         COALESCE(avg(G.geolocation_lat), 0.0) AS geolocation_lat, 
#         COALESCE(avg(G.geolocation_lng), 0.0) AS geolocation_lng, 
#         COALESCE(count(*), 0) AS n, 
#         COALESCE(max(G.geolocation_state), '') AS geolocation_state 
#     FROM (
#         SELECT customer_zip_code_prefix AS zip_code_prefix FROM olist_customers 
#         UNION 
#         SELECT seller_zip_code_prefix AS zip_code_prefix FROM olist_sellers
#         UNION 
#         SELECT geolocation_zip_code_prefix AS zip_code_prefix FROM olist_geolocation
#     ) AS zc
#     LEFT JOIN olist_geolocation G 
#         ON G.geolocation_zip_code_prefix = zc.zip_code_prefix 
#     GROUP BY zc.zip_code_prefix
# );"""
# conn_pgsql_datalab.execute(insert_table_olist_geolocation_bis)


######################################### Maj de la Table product_category_name_translation ajout trad french ########################################################

# #Lecture de la table actuelle
# traduction_df = pd.read_sql("""
# select *
# from product_category_name_translation T;
# """, conn_pgsql_datalab)

# # print the resulting DataFrame
# print(traduction_df.head(3))

# # Création d'une nouvelle Df product_traduction_df que l'on parrcoura pour maj la table sql 
# product_traduction_df = pd.read_sql("""
# select distinct(product_category_name)
# from olist_products P;
# """, conn_pgsql_datalab)


# # print the resulting DataFrame
# print(product_traduction_df['product_category_name'].head(3))


# # print("--------------------------- Utilisation API traduction --------------------------------------")

# # Initialize the translator
# translator = Translator(service_urls=['translate.google.com'])

# # Define a function to get the English translation
# def get_english_translation(text):
#     if text is None:
#         return None
#     else:
#         return translator.translate(text).text

# # Define a function to get the French translation
# def get_french_translation(text):
#     if text is None:
#         return None
#     else:
#         return translator.translate(text, dest='fr').text

# # Add the translation columns to the dataframe
# product_traduction_df['product_category_name_english'] = product_traduction_df['product_category_name'].str.replace("_", " ").apply(get_english_translation).str.replace(" ", "_")
# product_traduction_df['product_category_name_french'] = product_traduction_df['product_category_name'].str.replace("_", " ").apply(get_french_translation).str.replace(" ", "_")

# # Print the updated dataframe

# print(product_traduction_df)

# # creation de la colonne product_category_name_french
# create_column_query = """ALTER TABLE product_category_name_translation
#                                 ADD COLUMN IF NOT EXISTS product_category_name_french TEXT;"""
# conn_pgsql_datalab.execute(create_column_query)



# for index, row in product_traduction_df.iterrows():
#     try:
#         print(row['product_category_name'], '  ', row['product_category_name_english'], '  ', row['product_category_name_french'])
#         if row['product_category_name_english'] is not None:
#             row['product_category_name_english'] = row['product_category_name_english'].replace("'", "''")
#         else:
#             row['product_category_name_english'] = None
#         if row['product_category_name_french'] is not None:
#             row['product_category_name_french'] = row['product_category_name_french'].replace("'", "''")
#         else:
#             row['product_category_name_french'] = None
#         update_query_trad = (f"""
#             INSERT INTO product_category_name_translation (product_category_name, product_category_name_english, product_category_name_french )
#             VALUES ('{row['product_category_name']}', '{row['product_category_name_english']}', '{row['product_category_name_french']}')
#             ON CONFLICT (product_category_name)
#             DO UPDATE SET 
#                 product_category_name_french = EXCLUDED.product_category_name_french,
#                 product_category_name_english = EXCLUDED.product_category_name_english;
#         """)
#         conn_pgsql_datalab.execute(update_query_trad)

#     except ValueError:
#         pass

########################################## Maj de la Table olist_geolocation_bis ajout region ########################################################

# # convertir le shape en geojson
# directory_path = os. getcwd() 
# data_folder = "Data"
# region_csv_name = "brazil_states"
# csv_path = os.path.join(directory_path, data_folder, region_csv_name)

# region_df = pd.read_csv(csv_path+".csv")
# print(region_df.head(3))

# #creation des colonnes state_name, region, municipal_districts, perc_pop_urb
# create_column_query = """ALTER TABLE olist_geolocation_bis
#                                 ADD COLUMN IF NOT EXISTS state_name VARCHAR(50),
#                                 ADD COLUMN IF NOT EXISTS region VARCHAR(50),
#                                 ADD COLUMN IF NOT EXISTS perc_pop_urb FLOAT4;"""
# conn_pgsql_datalab.execute(create_column_query)

# for index, row in region_df.iterrows():
#    update_query_region = (f"""
#             UPDATE olist_geolocation_bis SET (state_name, region, perc_pop_urb ) = ('{row['state']}', '{row['region']}', '{row['perc_pop_urb']}')
#             WHERE geolocation_state = '{row['abbreviation']}';
#         """)
#    conn_pgsql_datalab.execute(update_query_region)
# print("chargement fait")


#################################### Alter Tables for types ################################################
# change_olist_table_types ="""
# ALTER TABLE olist_customers ALTER COLUMN customer_city TYPE VARCHAR(50);
# ALTER TABLE olist_customers ALTER COLUMN customer_state TYPE VARCHAR(2);
# ALTER TABLE olist_customers ALTER COLUMN customer_id TYPE VARCHAR(100);
# ALTER TABLE olist_customers ALTER COLUMN customer_unique_id TYPE VARCHAR(100);
# ALTER TABLE public.olist_order_items ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;
# ALTER TABLE public.olist_order_items ALTER COLUMN product_id TYPE varchar(32) USING product_id::varchar;
# ALTER TABLE public.olist_order_items ALTER COLUMN seller_id TYPE varchar(32) USING seller_id::varchar;
# ALTER TABLE public.olist_order_items ALTER COLUMN shipping_limit_date TYPE timestamp USING shipping_limit_date::timestamp;
# ALTER TABLE public.olist_order_payments ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;
# ALTER TABLE public.olist_order_payments ALTER COLUMN payment_type TYPE varchar(32) USING payment_type::varchar;
# BEGIN;


# ALTER TABLE olist_geolocation ALTER COLUMN geolocation_city TYPE VARCHAR(50);
# ALTER TABLE olist_geolocation ALTER COLUMN geolocation_state TYPE VARCHAR(2);

# ALTER TABLE public.olist_order_reviews ALTER COLUMN review_id TYPE varchar(32) USING review_id::varchar;
# ALTER TABLE public.olist_order_reviews ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;
# ALTER TABLE olist_order_reviews ALTER COLUMN review_comment_title TYPE VARCHAR(100);
# ALTER TABLE olist_order_reviews ALTER COLUMN review_comment_message TYPE VARCHAR(500);
# ALTER TABLE public.olist_order_reviews ALTER COLUMN review_creation_date TYPE timestamp USING review_creation_date::timestamp;
# ALTER TABLE public.olist_order_reviews ALTER COLUMN review_answer_timestamp TYPE timestamp USING review_answer_timestamp::timestamp;

# ALTER TABLE public.olist_orders ALTER COLUMN order_id TYPE varchar(32) USING order_id::varchar;
# ALTER TABLE public.olist_orders ALTER COLUMN customer_id TYPE varchar(32) USING customer_id::varchar;
# ALTER TABLE public.olist_orders ALTER COLUMN order_status TYPE varchar(32) USING order_status::varchar;
# ALTER TABLE public.olist_orders ALTER COLUMN order_purchase_timestamp TYPE timestamp USING order_purchase_timestamp::timestamp;
# ALTER TABLE public.olist_orders ALTER COLUMN order_approved_at TYPE timestamp USING order_approved_at::timestamp;
# ALTER TABLE public.olist_orders ALTER COLUMN order_delivered_carrier_date TYPE timestamp USING order_delivered_carrier_date::timestamp;
# ALTER TABLE public.olist_orders ALTER COLUMN order_delivered_customer_date TYPE timestamp USING order_delivered_customer_date::timestamp;
# ALTER TABLE public.olist_orders ALTER COLUMN order_estimated_delivery_date TYPE timestamp USING order_estimated_delivery_date::timestamp;

# ALTER TABLE public.olist_products ALTER COLUMN product_id TYPE varchar(32) USING product_id::varchar;
# ALTER TABLE olist_products ALTER COLUMN product_category_name TYPE VARCHAR(50);
# ALTER TABLE olist_products ALTER COLUMN product_name_lenght TYPE VARCHAR(50);
# ALTER TABLE olist_products ALTER COLUMN product_description_lenght TYPE VARCHAR(50);
# ALTER TABLE olist_products ALTER COLUMN product_photos_qty TYPE VARCHAR(50);

# ALTER TABLE public.olist_sellers ALTER COLUMN seller_id TYPE varchar(32) USING seller_id::varchar;
# ALTER TABLE olist_sellers ALTER COLUMN seller_city TYPE VARCHAR(50);
# ALTER TABLE olist_sellers ALTER COLUMN seller_state TYPE VARCHAR(2);

# ALTER TABLE public.product_category_name_translation ALTER COLUMN product_category_name TYPE varchar(100) USING product_category_name::varchar;
# ALTER TABLE public.product_category_name_translation ALTER COLUMN product_category_name_english TYPE varchar(100) USING product_category_name_english::varchar;
# """
# conn_pgsql_datalab.execute(change_olist_table_types)
############################# Ajout de PK et fK ########################################################


# add_pk_olist_customers = """ALTER TABLE ONLY public.olist_customers
#     ADD CONSTRAINT  olist_customers_pk PRIMARY KEY (customer_id);"""
# conn_pgsql_datalab.execute(add_pk_olist_customers)




# add_pk_olist_geolocation_bis = """ALTER TABLE ONLY public.olist_geolocation_bis
#     ADD CONSTRAINT  olist_geolocation_bis_pk PRIMARY KEY (zip_code);"""
# conn_pgsql_datalab.execute(add_pk_olist_geolocation_bis)




# add_pk_olist_order_items = """ALTER TABLE ONLY public.olist_order_items
#     ADD CONSTRAINT  olist_order_items_pk PRIMARY KEY (order_id, order_item_id);"""
# conn_pgsql_datalab.execute(add_pk_olist_order_items)




# add_pk_olist_order_payments = """ALTER TABLE ONLY public.olist_order_payments
#     ADD CONSTRAINT  olist_order_payments_pk PRIMARY KEY (order_id, payment_sequential);"""
# conn_pgsql_datalab.execute(add_pk_olist_order_payments)




# drop_pk_olist_order_reviews = """ALTER TABLE public.olist_order_reviews
# DROP CONSTRAINT IF EXISTS olist_order_reviews_pkey;"""
# conn_pgsql_datalab.execute(drop_pk_olist_order_reviews)


# add_pk_olist_order_reviews = """ALTER TABLE ONLY public.olist_order_reviews
#     ADD CONSTRAINT  olist_order_reviews_pk PRIMARY KEY (review_id, order_id);"""
# conn_pgsql_datalab.execute(add_pk_olist_order_reviews)




# add_pk_olist_orders = """ALTER TABLE ONLY public.olist_orders
#     ADD CONSTRAINT  olist_orders_pk PRIMARY KEY (order_id);"""
# conn_pgsql_datalab.execute(add_pk_olist_orders)




# add_pk_olist_products = """ALTER TABLE ONLY public.olist_products
#     ADD CONSTRAINT  olist_products_pk PRIMARY KEY (product_id);"""
# conn_pgsql_datalab.execute(add_pk_olist_products)




# add_pk_olist_sellers = """ALTER TABLE ONLY public.olist_sellers
#     ADD CONSTRAINT olist_sellers_pk PRIMARY KEY (seller_id);"""
# conn_pgsql_datalab.execute(add_pk_olist_sellers)




# add_pk_product_category_name_translation = """ALTER TABLE ONLY public.product_category_name_translation
#     ADD CONSTRAINT  product_category_name_translation_pk PRIMARY KEY (product_category_name);
# """
# conn_pgsql_datalab.execute(add_pk_product_category_name_translation)



# add_fk_olist_customers = """ALTER TABLE ONLY public.olist_customers
#     ADD CONSTRAINT  olist_customers_fk FOREIGN KEY (customer_zip_code_prefix) REFERENCES public.olist_geolocation_bis(zip_code);
# """
# conn_pgsql_datalab.execute(add_fk_olist_customers)




# add_fk_olist_order_items = """ALTER TABLE ONLY public.olist_order_items
#     ADD CONSTRAINT  olist_order_items_fk FOREIGN KEY (order_id) REFERENCES public.olist_orders(order_id);
# """
# conn_pgsql_datalab.execute(add_fk_olist_order_items)




# add_fk2_olist_order_items = """ALTER TABLE ONLY public.olist_order_items
#     ADD CONSTRAINT  olist_order_items_fk2 FOREIGN KEY (product_id) REFERENCES public.olist_products(product_id);
# """
# conn_pgsql_datalab.execute(add_fk2_olist_order_items)




# add_fk3_olist_order_items = """ALTER TABLE ONLY public.olist_order_items
#     ADD CONSTRAINT  olist_order_items_fk3 FOREIGN KEY (seller_id) REFERENCES public.olist_sellers(seller_id);
# """
# conn_pgsql_datalab.execute(add_fk3_olist_order_items)




# add_fk_olist_order_payments = """ALTER TABLE ONLY public.olist_order_payments
#     ADD CONSTRAINT  olist_order_payments_fk FOREIGN KEY (order_id) REFERENCES public.olist_orders(order_id);

# """
# conn_pgsql_datalab.execute(add_fk_olist_order_payments)



# add_fk_olist_order_reviews = """ALTER TABLE ONLY public.olist_order_reviews
#     ADD CONSTRAINT  olist_order_reviews_fk FOREIGN KEY (order_id) REFERENCES public.olist_orders(order_id);

# """
# conn_pgsql_datalab.execute(add_fk_olist_order_reviews)




# add_fk_olist_orders = """ALTER TABLE ONLY public.olist_orders
#     ADD CONSTRAINT  olist_orders_fk FOREIGN KEY (customer_id) REFERENCES public.olist_customers(customer_id);

# """
# conn_pgsql_datalab.execute(add_fk_olist_orders)




# add_fk_olist_products = """ALTER TABLE ONLY public.olist_products
#     ADD CONSTRAINT  olist_products_fk FOREIGN KEY (product_category_name) REFERENCES public.product_category_name_translation(product_category_name);

# """
# conn_pgsql_datalab.execute(add_fk_olist_products)




add_fk_olist_sellers = """ALTER TABLE ONLY public.olist_sellers
    ADD CONSTRAINT  olist_sellers_fk FOREIGN KEY (seller_zip_code_prefix) REFERENCES public.olist_geolocation_bis(zip_code);

"""
conn_pgsql_datalab.execute(add_fk_olist_sellers)

add_table_time_order ="""
create table time_order_table as select
    ood.order_id,
    ood.customer_id,
    ood.order_purchase_timestamp,
    ood.order_approved_at - ood.order_purchase_timestamp as "approvement_time",
    ood.order_delivered_carrier_date - ood.order_purchase_timestamp as "carrier_deliver_time",
    ood.order_delivered_customer_date - ood.order_purchase_timestamp as "customer_deliver_time",
    min(oorda.review_creation_date) - ood.order_delivered_customer_date as "first_review_after_delivery",
    ood.order_delivered_customer_date - ood.order_estimated_delivery_date as "gap_estimated_delivery"
from olist_orders ood
left join olist_order_reviews oorda
    using (order_id)
group by ood.order_id
;
"""
conn_pgsql_datalab.execute(add_table_time_order)

# création d'une table geolocalisation à partir de tout les zipcode unique dans toutes les tables

# df = pd.read_sql("""SELECT DISTINCT zip_code_prefix, city
# FROM (
#   SELECT DISTINCT customer_zip_code_prefix AS zip_code_prefix , customer_city AS city
#   FROM olist_customers
#   UNION
#   SELECT DISTINCT seller_zip_code_prefix AS zip_code_prefix, seller_city AS city
#   FROM olist_sellers
# ) AS all_zip_codes""", conn_pgsql_datalab)

# print(df.head(3))

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





