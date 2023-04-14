from flask import Flask, render_template, request, jsonify, redirect, url_for, Response
import json
# from analyse import bar_plot_asc, n_Mooc_plus_populaires, dataset
from sqlalchemy.orm import Session
from olist_model import *
import yaml, os
from sqlalchemy import create_engine, text
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

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
engine = create_engine(config['olist_writer'])

# OLIST_writer = os.environ['olist_writer']
# engine = create_engine(OLIST_writer)
# print(engine)



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

@app.route('/api', methods=['GET', 'POST'])
def api():
    if request.method == 'POST':
        # print(request.form)
        # product_category_name = request.form.get('product_category_name', np.nan)
        # product_category_name_english = request.form.get('product_category_name_english', np.nan)
        # product_category_name_french = request.form.get('product_category_name_french', np.nan)
        # # définition du df pour la prédiction
        # df_input = pd.DataFrame(columns=["product_category_name", "product_category_name_english", "product_category_name_french"])
        # df_input.loc[0] = [product_category_name, product_category_name_english, product_category_name_french]
        with Session(engine) as session:
            it = session.query(ProductCategory).all()
        return render_template('api.html', it=it)
    else:
        return render_template('api.html')

@app.route("/api/categories", methods=['GET'])
def cat_list():
    with Session(engine) as session:
        it = session.query(ProductCategory).all()
    json_data = json.dumps([pc.to_json() for pc in it], ensure_ascii=False).encode('utf-8')
    response = Response(json_data, content_type='application/json; charset=utf-8')
    return response

@app.route("/api/category", methods=['POST'])
def cat_update():
    pk=request.form['cat']
    fr=request.form['fr']
    print('cat_update: ', pk, fr)
    with Session(engine) as session:
        pc = session.query(ProductCategory).get(pk)
        pc.set_FR(fr)
        session.commit()
    return redirect(url_for('api')) # jsonify('OK')


if __name__ == '__main__':
    app.run(debug=True)