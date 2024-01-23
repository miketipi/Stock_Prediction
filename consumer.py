import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from time import sleep
from IPython.display import display, clear_output
from pyspark.sql.functions import col, concat, lit, to_json, struct
from pyspark.sql.types import *
from pyspark.sql import functions as f
from kafka import KafkaProducer
from json import dumps, loads

scala_version = '2.12'  # your scala version
spark_version = '3.5.0' # your spark version
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.6.0', #your kafka version
    'org.mongodb.spark:mongo-spark-connector_2.12:10.2.0'
]
spark = SparkSession.builder.master("local").appName("kafka-example") \
                                            .config("spark.mongodb.input.uri", "mongodb://localhost:27017/stock_prediction.prediction")\
                                            .config("spark.mongodb.output.uri", "mongodb://localhost:27017/stock_prediction.prediction")\
                                            .config("spark.jars.packages", ",".join(packages)).getOrCreate()

topic_name = 'StockPredict'
kafka_server = 'localhost:9092'

kafkaDf = spark.read.format("kafka").option("kafka.bootstrap.servers", kafka_server).option("subscribe", topic_name).option("startingOffsets", "earliest").load()

schema = StructType([ 
    StructField("time", DateType(), True),
    StructField("open" , FloatType(), True),
    StructField("high" , FloatType(), True),
    StructField("low" , FloatType(), True),
    StructField("close" , FloatType(), True),
    StructField("volume" , LongType(), True),
    StructField("ticker" , StringType(), True),
        ])
columns = ["prediction", "time","open","high","low","close","volume","ticker"]

streamRawDf = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_server).option("subscribe", topic_name).load()
streamDF = streamRawDf.select(f.from_json(f.col("value").cast("string"), schema).alias("parsed_value"))
parseDF = streamDF.select(f.col("parsed_value.*"))

stream_writer = (parseDF.writeStream.queryName("stream_test").trigger(processingTime="5 seconds").outputMode("append").format("memory"))
query1 = stream_writer.start()

topic_name = 'StockPredict_Topic2'
kafka_server = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=kafka_server,value_serializer = lambda x:dumps(x).encode('utf-8'))
import model

for x in range(0, 2000):
    try:
        result1 = spark.sql(f"SELECT * from {query1.name}")
        if (len(result1.toPandas()) !=0):
          df_pre = model.predict(result1)

          jsonDF = df_pre.withColumn("value", to_json(struct(columns)))

          print(jsonDF.select(f.col("value")).toPandas().values[-1][0])

          # producer.send(topic_name, value= loads(jsonDF.select(f.col("value")).toPandas().values[0][0]))

          if (len(jsonDF.select(f.col("value")).toPandas()) == 1):
              producer.send(topic_name, value= loads(jsonDF.select(f.col("value")).toPandas().values[0][0]))
          else:
              producer.send(topic_name, value= loads(jsonDF.select(f.col("value")).toPandas().values[-2][0]))
              producer.send(topic_name, value= loads(jsonDF.select(f.col("value")).toPandas().values[-1][0]))

          sleep(5)
          clear_output(wait=True)
    except KeyboardInterrupt:
        print("break")
        break
print("Live view ended...")



# result1 = pre_process(result1)
# predictions = model.transform(result1)
# for x in range(0, 2000):
#     try:
#         result1 = spark.sql(f"SELECT * from {query1.name}")

#         result1 = pre_process(result1)
#         predictions = model.transform(result1)

#         display(predictions.select("Item_Identifier", "Item_Weight", "Item_Visibility", "Item_MRP", "Outlet_Establishment_Year", "prediction").toPandas())
#         sleep(5)
#         clear_output(wait=True)
#     except KeyboardInterrupt:
#         print("break")
#         break
# print("Live view ended...")


# jsonDF = parseDF.withColumn("value", to_json(struct(columns)))


# query = jsonDF.selectExpr("CAST(value AS STRING)") \
#   .writeStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", kafka_server) \
#   .option("topic","StockPredict_Topic2")\
#   .option("checkpointLocation", "/tmp/pyspark/")\
#   .option("forceDeleteTempCheckpointLocation", "true")\
#   .start()

# query.awaitTermination()

# query1.stop()