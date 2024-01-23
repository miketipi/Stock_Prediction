from flask import Flask, render_template, Response
import pandas as pd
import time

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

def generate():
    data = pd.read_csv('test_data.csv')

    while True:
        latest_data = data.iloc[-1] 
        yield f"data:{latest_data.to_json()}\n\n"
        time.sleep(1) 

@app.route('/stream')
def stream():
    return Response(generate(), content_type='text/event-stream')

if __name__ == '__main__':
    app.run(debug=True)
