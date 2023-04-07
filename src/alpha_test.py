import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sklearn.metrics import accuracy_score
from xgboost import XGBRegressor

import alpha_data

# Load environment variables from .env file
load_dotenv()
EXTERNAL_ID = os.getenv("EXTERNAL_ID_FIELD_NAME")

# initialize spark session
spark = SparkSession.builder.appName('alpha-xgboost-iris').getOrCreate()


def predict():
    model = XGBRegressor()
    model.load_model("xgboost_on_iris.xgb")
    df = alpha_data.load_dataset_as_pandas_df()
    x_train, x_test, y_train, y_test = alpha_data.get_train_test_split(df)
    # Predictions
    predictions = model.predict(pd.DataFrame(data=x_test, columns=alpha_data.get_feature_columns())
                                .drop(EXTERNAL_ID, axis=1).astype(float)).astype(int)
    final_data = np.hstack((x_test, predictions.reshape(-1, 1)))

    # Calculate accuracy
    accuracy = accuracy_score(y_test, predictions)
    print(f'Accuracy: {accuracy}')
    return final_data


def write_prediction_to_app_object(final_data):
    # Write back to the object
    spark_dataset_to_write = spark.createDataFrame(final_data, alpha_data.get_all_columns())
    spark_dataset_to_write.show()
    spark_dataset_to_write.write \
        .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider") \
        .option("applicationDataTypeId", os.getenv("APPLICATION_DATATYPE_ID")) \
        .option("rootDAGContextId", os.getenv("ROOT_DAG_CONTEXT_ID")) \
        .option("structTypeId", os.getenv("STRUCT_TYPE_ID")) \
        .option("authToken", os.getenv("AUTH_TOKEN")) \
        .option("baseUrl", os.getenv("BASE_URL")) \
        .option("wsBaseUrl", os.getenv("WS_BASE_URL")) \
        .option("readWriteMode", "write") \
        .mode("append") \
        .save()


if __name__ == '__main__':
    data = predict()
    write_prediction_to_app_object(data)
