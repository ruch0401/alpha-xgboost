import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sklearn.metrics import accuracy_score
from xgboost import XGBRegressor
from datetime import datetime
from config import *

import alpha_data

# initialize spark session
spark = SparkSession.builder.appName('alpha-xgboost-iris').getOrCreate()


def predict():
    model = XGBRegressor()
    model.load_model("xgboost_on_iris.xgb")
    df = alpha_data.load_dataset_as_pandas_df()

    # df = alpha_data.load_dataset_from_local_as_pandas_df()
    x_train, x_test, y_train, y_test = alpha_data.get_train_test_split(df)

    # Predictions
    # concatenating x_train and x_test along rows to predict on all the data
    all_x_data = pd.concat([pd.DataFrame(x_train), pd.DataFrame(x_test)], axis=0)
    all_y_data = pd.concat([pd.DataFrame(y_train), pd.DataFrame(y_test)], axis=0)
    predictions = model.predict(
        pd.DataFrame(data=all_x_data, columns=alpha_data.get_feature_columns()).drop(EXTERNAL_ID, axis=1).astype(
            float)).astype(int)
    # concatenating the dataset vertically to bring together the features and label columns
    final_data = np.hstack((all_x_data, predictions.reshape(-1, 1)))

    # Calculate accuracy
    accuracy = accuracy_score(all_y_data, predictions)
    print(f'Accuracy: {accuracy}')
    return final_data


def write_prediction_to_app_object(final_data):
    # Write back to the object
    print(f'All columns: \n{alpha_data.get_all_columns()}')
    spark_dataset_to_write = spark.createDataFrame(final_data, alpha_data.get_all_columns())
    # spark_dataset_to_write.repartition(8)
    # spark_dataset_to_write.show()
    write_dataframe_to_citta(spark_dataset_to_write)
    # write_dataframe_to_local(spark_dataset_to_write)


def write_dataframe_to_citta(spark_dataset_to_write):
    write_start_time = datetime.now()
    print(f'Writing records below:')
    spark_dataset_to_write.show()
    print(f'Attempting to write {spark_dataset_to_write.count()} records')
    spark_dataset_to_write.write \
        .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider") \
        .option("applicationDataTypeName", APPLICATION) \
        .option("rootDAGContextName", APP_GROUP) \
        .option("structTypeName", APPLICATION_OBJECT) \
        .option("token", TOKEN) \
        .option("sessionString", SESSION_STRING) \
        .option("readWriteMode", "write") \
        .option("isExecuteValidationInParallel", "true") \
        .mode("append") \
        .save()
    write_end_time = datetime.now()
    print(f'Total write time: {write_end_time - write_start_time}')


def write_dataframe_to_local(spark_dataset_to_write):
    spark_dataset_to_write.write \
        .format("csv") \
        .option("header", "true") \
        .mode("append") \
        .save("iris.csv")


if __name__ == '__main__':
    start_instant = datetime.now()
    data = predict()
    write_prediction_to_app_object(data)
    end_instant = datetime.now()
    print(f'Total time taken for the code to execute: {end_instant - start_instant}')
