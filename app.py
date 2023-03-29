from flask import Flask, render_template, request
# from analyse import bar_plot_asc, n_Mooc_plus_populaires, dataset
import plotly.graph_objs as go
# For ploting graph / Visualization
import plotly.express as px
from plotly.io import to_json
from flask_bootstrap import Bootstrap
import pandas as pd
import numpy as np
import pickle
from unidecode import unidecode
from markupsafe import escape
import numpy as np

import db_connection as db_con

import os.path


# filename = os.path.join("data",'modele_redump.sav')

# # Connexion à mysql
# conn_mysql_datalab = db_con.connect_to_db(config_file='config.yaml', section='mysql_local_mooc', ssh=False, local_port=None, ssh_section= 'ssh_tunnel_datalab')
# variable = '21GG21'
# course_var = 'MinesTelecom/04017S02/session02'
# query = "SELECT corpus FROM dataset WHERE user = '21GG21' AND course_id = 'MinesTelecom/04017S02/session02'"
# # encode the query parameters as bytes
# variable_bytes = variable.encode('utf-8')
# course_var_bytes = course_var.encode('utf-8')
# message_query_user = conn_mysql_datalab.execute(query, variable_bytes,course_var_bytes) 
# results = message_query_user.fetchall()
# print(results)
# conn_mysql_datalab.close()

app = Flask(__name__)
bootstrap = Bootstrap(app)
@app.route('/')
def home():
    return render_template('home.html')

@app.route('/analyse')
def analyse():
    return render_template('analyse.html')

 
   

@app.route('/model', methods=['GET', 'POST'])
def model():
    return render_template('model.html')

if __name__ == '__main__':
    app.run(debug=True)