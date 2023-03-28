import yaml, utils
import db_connection as db_con

import pandas as pd
import sqlalchemy
import time
import os, sys
from os.path import join, dirname




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

for file in dirs:
    print(file)
    data_df = pd.read_csv(os.path.join(csv_path, file))
    table_name = file .replace(".csv", "")
    print(table_name)
    data_df.to_sql(table_name, con=conn_pgsql_datalab, index=True, index_label='id', if_exists='replace')
    
        