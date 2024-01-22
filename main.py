import model
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def main():
    # Khởi tạo Spark Session
    spark = SparkSession.builder.appName("kafka_model").getOrCreate()
    sc = spark.sparkContext
    # Tạo DataFrame mẫu
    train_data = spark.read.csv("./data/train_data.csv", inferSchema=True, header=True)
    test_data = spark.read.csv("./data/test_data.csv", inferSchema=True, header=True)
    
    _model = model.train(train_data)
    y_pre = model.predict(test_data)
    print(y_pre)


if __name__ == "__main__":
    main()








