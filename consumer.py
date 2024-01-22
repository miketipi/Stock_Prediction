import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from time import sleep
from IPython.display import display, clear_output
from pyspark.sql.functions import col, concat, lit, to_json, struct
from pyspark.sql.types import *
from pyspark.sql import functions as f

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
columns = ["time","open","high","low","close","volume","ticker"]

streamRawDf = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_server).option("subscribe", topic_name).load()
streamDF = streamRawDf.select(f.from_json(f.col("value").cast("string"), schema).alias("parsed_value"))
parseDF = streamDF.select(f.col("parsed_value.*"))
jsonDF = parseDF.withColumn("value", to_json(struct(columns)))

db = "stock_predict"
connection_string = "mongodb://localhost:27017"
collecion = "prediction"

# stream_writer1 = (parseDF.writeStream
#   .queryName("stock_predict")
#   .trigger(continuous="5 seconds")
#   .format("mongodb")
#   .option("checkpointLocation", "/tmp/pyspark/")
#   .option("forceDeleteTempCheckpointLocation", "true")
#   .option("spark.mongodb.connection.uri",connection_string)
#   .option("spark.mongodb.database", db)
#   .option("spark.mongodb.collection", collecion)
#   .outputMode("append")
# )
# query1 = stream_writer1.start()

# for x in range(0, 2000):
#     try:
#         # print("Showing live view refreshed every 5 seconds")
#         # print(f"Seconds passed: {x*5}")
#         # result1 = spark.sql(f"SELECT * from {query1.name}")
#         # display(result1.toPandas())
#         # clear_output(wait=True)
#         print("Write to MongoDB")
#         sleep(5)
#     except KeyboardInterrupt:
#         print("break")
#         break
# print("Live view ended...")

# def write_mongo_row(df, epoch_id):
#     mongoURL = "mongodb://localhost:27017/stock_predict.prediction"
#     df.write.format("mongo").mode("append").option("uri",mongoURL).save()
#     pass

# query=parseDF.writeStream.foreachBatch(write_mongo_row).start()


# def save(message, epoch_id):
#     # message.write \
#     #     .format("mongodb") \
#     #     .mode("append") \
#     #     .option("database", db) \
#     #     .option("collection", collecion) \
#     #     .save()
#     message.write\
#         .format("mongodb")\
#         .option("spark.mongodb.database", db)\
#         .option("spark.mongodb.collection", collecion)\
#         .mode("append")\
#         .save()
#     pass

# query = parseDF\
#     .writeStream \
#     .foreachBatch(save) \
#     .start()



query = jsonDF.selectExpr("CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_server) \
  .option("topic","StockPredict_Topic2")\
  .option("checkpointLocation", "/tmp/pyspark/")\
  .option("forceDeleteTempCheckpointLocation", "true")\
  .start()

query.awaitTermination()

# query1.stop()