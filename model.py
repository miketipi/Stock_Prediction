import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql import Window
# import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dropout, Dense

from sklearn.preprocessing import MinMaxScaler

import os.path
from os import path
import matplotlib.pyplot as plt


# Khởi tạo Spark Session
spark = SparkSession.builder.appName("kafka_model").getOrCreate()
sc = spark.sparkContext
# Tạo DataFrame mẫu
train_data = spark.read.csv("./data/train_data.csv", inferSchema=True, header=True)
test_data = spark.read.csv("./data/test_data.csv", inferSchema=True, header=True)

training_set = train_data.select("close").toPandas().values
# Thuc hien scale du lieu gia ve khoang 0,1
scaler = MinMaxScaler(feature_range = (0, 1)).fit(training_set)

# Tao du lieu train, X = 60 time steps, Y =  1 time step
def pre_process(data, data_scaled):
    X=[]
    y=[]
    no_of_sample = len(data)

    for i in range(60, no_of_sample):
        X.append(data_scaled[i-60:i, 0])
        y.append(data_scaled[i, 0])

    X, y = np.array(X), np.array(y)
    # print(X)

    X = np.reshape(X, (X.shape[0], X.shape[1], 1))
    
    return X, y

def init_model():
    regressor = Sequential()
    regressor.add(LSTM(units=50, return_sequences=True, input_shape=(60, 1)))
    regressor.add(Dropout(0.2))
    regressor.add(LSTM(units=50, return_sequences=True))
    regressor.add(Dropout(0.2))
    regressor.add(LSTM(units=50, return_sequences=True))
    regressor.add(Dropout(0.2))
    regressor.add(LSTM(units=50))
    regressor.add(Dropout(0.2))
    regressor.add(Dense(units=1))
    regressor.compile(optimizer='adam', loss='mean_squared_error')
    return regressor


def train(data_train):
    training_set = data_train.select("close").toPandas().values
    
    train_scaler = scaler.transform(training_set)

    X_train, y_train = pre_process(training_set, train_scaler)

    regressor = init_model()

    # Neu ton tai file model thi load
    if path.exists("./model/mymodel.h5"):
        regressor.load_weights("./model/mymodel.h5")
    else:
        # Con khong thi train
        regressor.fit(X_train, y_train, epochs = 100, batch_size = 32)
        regressor.save("./model/mymodel.h5")

    return regressor

def predict(data_test):
    model_load= init_model()
    model_load.load_weights("./model/mymodel.h5")
    # data_test.show()
    test_set = data_test.select(f.col("close")).toPandas().values
    pre_data = training_set[len(training_set)-60:]
    dataset_test = np.concatenate((pre_data, test_set), axis=0)
    
    test_scaled = scaler.transform(dataset_test)
    # print(test_scaled)
    X_test, y_test = pre_process(dataset_test, test_scaled)

    y_pre= model_load.predict(X_test)

    predicted_stock_price = scaler.inverse_transform(y_pre)

    a = spark.createDataFrame([(float(l[0]),) for l in predicted_stock_price], ['prediction'])
    # a.show()

    # #add 'sequential' index and join both dataframe to get the final result
    a = a.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
    b = data_test.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))

    final_df = a.join(b, a.row_idx == b.row_idx).\
                drop("row_idx")
    print(len(final_df.toPandas()))
    

    return final_df



# data là dữ liệu của 60 ngày trước ngày dự đoán
def predict_future(data):
    data_pre = data.select("close").toPandas().values

    inputs = data_pre
    inputs = scaler.transform(inputs)

    list_predict = []
    model_load= init_model()
    model_load.load_weights("./model/mymodel.h5")

    i = 0
    while i<28:
        X_test = []
        no_of_sample = len(data)

        # Lay du lieu cuoi cung
        X_test.append(inputs[no_of_sample - 60:no_of_sample, 0])
        X_test = np.array(X_test)
        X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))

        # Du doan gia
        predicted_stock_price = model_load.predict(X_test)

        # chuyen gia tu khoang (0,1) thanh gia that
        predicted_stock_price = scaler.inverse_transform(predicted_stock_price)

        # Them ngay hien tai vao
        data = np.concatenate((data, predicted_stock_price), axis=0)
        inputs = data
        inputs = scaler.transform(inputs)

        list_predict.append(predicted_stock_price[0][0])

        i = i +1

    return list_predict


def img_predict(y_test, y_pre):
    # Ve bieu do gia that va gia du doan
    plt.plot(y_test, color = 'red', label = 'Real Stock Price')
    plt.plot(y_pre, color = 'blue', label = 'Predicted Stock Price')
    plt.title('Stock Price Prediction')
    plt.xlabel('Time')
    plt.ylabel('Stock Price')
    plt.legend()
    plt.savefig('./static/LSTM_result.jpg')
    plt.show()
