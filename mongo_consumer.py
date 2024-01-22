from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

topic_name = "StockPredict_Topic2"
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers = ['localhost:9092'],
    auto_offset_reset = "earliest",
    enable_auto_commit = True,
    group_id = "my_group",
    value_deserializer = lambda x: loads(x.decode('utf-8'))
)

client = MongoClient("localhost:27017")
collection = client.stock_predict.prediction
for message in consumer:
    message = message.value
    collection.insert_one(message)
    print('{} added to {}'.format(message, collection))