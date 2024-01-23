from flask import Flask, render_template
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import json
import threading
import time

app = Flask(__name__)

csv_file_path = 'test_data.csv'
stock_data = pd.read_csv(csv_file_path)

def update_data():
    global stock_data
    while True:
        new_data = pd.read_csv(csv_file_path)

        stock_data = new_data

        time.sleep(1)  

update_thread = threading.Thread(target=update_data)
update_thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def get_data():
    global stock_data
    return stock_data.to_json(orient='split')

if __name__ == '__main__':
    app.run(debug=True)
