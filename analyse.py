import yaml, utils
import db_connection as db_con
from sqlalchemy import create_engine, text

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

import geopandas as gpd
import plotly.express as px




# Connexion à pgsql
#conn_pgsql_datalab = db_con.connect_to_db(config_file='config.yaml', section='pgsql_datalab', ssh=False, local_port=None, ssh_section= 'ssh_tunnel_datalab')
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
engine = create_engine(config['olist_reader'])
print(engine)
# convertir le shape en geojson
directory_path = os. getcwd() 
data_folder = "Data"
shapefile_name = "estados_2010"
shapefile_path = os.path.join(directory_path, data_folder, shapefile_name)


#'https://raw.githubusercontent.com/luizpedone/municipal-brazilian-geodata/master/data/Brasil.json'

from urllib.request import urlopen
import json
with urlopen('https://raw.githubusercontent.com/luizpedone/municipal-brazilian-geodata/master/data/Brasil.json') as response:
    counties = json.load(response)

# # with open('data/states_brazil.json') as f:
# #     counties = json.load(f)
# # print(type(counties))
# # print(counties["features"][0])
# # # test pour avoir les state dans le geojson
# # ufs = []
# # for feature in counties['features']:
# #     ufs.append(feature['properties']['UF'])
# # print(ufs)

# # # test pour voir les valeurs unique des etats dans la df et le comparer
# # region_df['UF'] = region_df['UF'].str.upper() 
# # # print the resulting DataFrame
# # print(region_df.head(3))

#Lecture de la table actuelle
region_df = pd.read_sql("""
select geo.geolocation_state as "UF", geo.region
from olist_geolocation_bis geo;
""", engine)


# Get the unique values of 'UF'
uf_values = set(region_df['UF'].unique())

print(uf_values)

fig = px.choropleth_mapbox(region_df, geojson=counties, locations='UF', color='region',
                           color_continuous_scale="Viridis",
                           featureidkey='properties.UF',
                           mapbox_style="carto-positron",
                           zoom=3,
                           opacity=0.5,
                           center= {'lon': -55, 'lat': -15},
                           labels={'region':'régions'}
                          )
# show the plot
fig#.show()

# nombre de clients par states
clients_state_df = pd.read_sql("""
select  customer_state, count(oc.customer_id) as tot_clients
from olist_geolocation_bis geo
right join olist_customers oc
on oc.customer_zip_code_prefix = geo.zip_code 
group by customer_state;
""", engine)
print(clients_state_df)

fig = px.choropleth_mapbox(clients_state_df, geojson=counties, locations='customer_state', color='tot_clients',
                           color_continuous_scale="Viridis",
                           featureidkey='properties.UF',
                           mapbox_style="carto-positron",
                           zoom=3,
                           opacity=0.5,
                           center= {'lon': -55, 'lat': -15},
                           labels={'region':'régions'}
                          )
# show the plot
fig.show()



# 1 Question dataframe
q1_sql ="""
SELECT "Olist Customers - Customer"."customer_state" AS "Olist Customers - Customer__customer_state", "Olist Customers - Customer"."customer_state" AS "Olist Customers - Customer__customer_state", DATE_TRUNC('month', "Olist Orders - Order"."order_approved_at") AS "Olist Orders - Order__order_approved_at", "Olist Products - Product"."product_category_name" AS "Olist Products - Product__product_category_name", SUM("public"."olist_order_payments"."payment_value") AS "sum"
FROM "public"."olist_order_payments"
LEFT JOIN "public"."olist_orders" AS "Olist Orders - Order" ON "public"."olist_order_payments"."order_id" = "Olist Orders - Order"."order_id" 
LEFT JOIN "public"."olist_customers" AS "Olist Customers - Customer" ON "Olist Orders - Order"."customer_id" = "Olist Customers - Customer"."customer_id" 
LEFT JOIN "public"."olist_order_items" AS "Olist Order Items - Order" ON "public"."olist_order_payments"."order_id" = "Olist Order Items - Order"."order_id" LEFT JOIN "public"."olist_products" AS "Olist Products - Product" ON "Olist Order Items - Order"."product_id" = "Olist Products - Product"."product_id"
GROUP BY "Olist Customers - Customer"."customer_state", DATE_TRUNC('month', "Olist Orders - Order"."order_approved_at"), "Olist Products - Product"."product_category_name"
ORDER BY "Olist Customers - Customer"."customer_state" ASC, DATE_TRUNC('month', "Olist Orders - Order"."order_approved_at") ASC, "Olist Products - Product"."product_category_name" ASC;
"""
q1_df = pd.read_sql(q1_sql, engine)

print(q1_df.columns)




"""
SELECT 
    c.customer_state, 
    DATE_PART('month', CAST(o.order_approved_at AS TIMESTAMP)) AS order_month, 
    p.product_category_name, 
    SUM(op.payment_value) AS total_payments
FROM 
    public.olist_order_payments AS op
LEFT JOIN 
    public.olist_orders AS o ON op.order_id = o.order_id
LEFT JOIN 
    public.olist_customers AS c ON o.customer_id = c.customer_id
LEFT JOIN 
    public.olist_order_items AS oi ON op.order_id = oi.order_id
LEFT JOIN 
    public.olist_products AS p ON oi.product_id = p.product_id
GROUP BY 
    c.customer_state, order_month, p.product_category_name
ORDER BY 
    c.customer_state ASC, order_month ASC, p.product_category_name ASC;"""