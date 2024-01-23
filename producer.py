from kafka import KafkaProducer
from json import dumps
from time import sleep
from random import seed
from random import randint
import pandas as pd
import datetime

topic_name = 'StockPredict'
kafka_server = 'localhost:9092'
df = pd.read_csv("./data/test_data.csv")

producer = KafkaProducer(bootstrap_servers=kafka_server,value_serializer = lambda x:dumps(x).encode('utf-8'))

df['time'] = pd.to_datetime(df['time'])
df = df.sort_values(by="time",ascending=True)
df['time'] = df['time'].dt.strftime("%Y-%m-%d")

# sleep(15)

seed(1)
    
for i in range(len(df)):
    data = {
        "time": df.loc[i]['time'],
        "open": int(df.loc[i]["open"]),
        "high": int(df.loc[i]["high"]),
        "low": int(df.loc[i]["low"]),
        "close": int(df.loc[i]["close"]),
        "volume": int(df.loc[i]["volume"]),
        "ticker": df.loc[i]["ticker"],
    }
    producer.send(topic_name, value=data)
    print(data)
    sleep(5)

producer.flush()