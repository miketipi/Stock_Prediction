from flask import Flask, render_template, jsonify
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
import base64
import time
from pymongo import MongoClient

app = Flask(__name__)

# client = MongoClient("localhost:27017")
# db = client['database'] # mày thay tên database mình vào nha Nhân

def generate_plot(actual_prices, predicted_prices):
    plt.figure(figsize=(10, 6))
    plt.plot(actual_prices, marker='o', linestyle='-', color='b', label='Actual Price')
    plt.plot(predicted_prices, marker='o', linestyle='-', color='r', label='Predicted Price')
    plt.title('Stock Price Movement')
    plt.xlabel('Days')
    plt.ylabel('Price')
    plt.grid(True)
    plt.legend()

    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    
    image_png = buffer.getvalue()
    buffer.close()
    graph = base64.b64encode(image_png).decode('utf-8')

    return graph

@app.route('/')
def index():
    return render_template('index.html')

# @app.route('/update_prediction')
# def update_prediction():
#     predicted_price = db['stocks'].find_one()['predicted_price']
#     return jsonify({'predicted_price': predicted_price})

@app.route('/update_plot')
def update_plot():
    days = np.arange(1, 31)
    actual_prices = 100 + 2 * days + np.random.normal(0, 10, size=len(days))
    predicted_prices = 110 + 2.1 * days + np.random.normal(0, 5, size=len(days)) 
    
    graph = generate_plot(actual_prices, predicted_prices)
    return graph

if __name__ == '__main__':
    app.run(debug=True)
