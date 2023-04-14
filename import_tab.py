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
conn_pgsql_datalab = db_con.connect_to_db(config_file='config.yaml', section='pgsql_datalab', ssh=True, local_port=None, ssh_section= 'ssh_tunnel_datalab')
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
for file in dirs:
    print(file)
    if file.endswith(".csv"):
      data_df = pd.read_csv(os.path.join(csv_path, file),dtype=dtype)
      print(data_df.head(2))
      table_name = file .replace(".csv", "").replace("_dataset", "")
      data_df.to_sql(table_name, con=conn_pgsql_datalab, index=False, index_label='id', if_exists='replace')
      print(f"{table_name} table created with {data_df.shape[0]} rows.")