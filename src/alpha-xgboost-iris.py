import os

import numpy as np
from xgboost import XGBRegressor
from pyspark.sql import SparkSession
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env file
load_dotenv()
EXTERNAL_ID = os.getenv("EXTERNAL_ID_FIELD_NAME")

# initialize spark session
spark = SparkSession.builder.appName('alpha-xgboost-iris').getOrCreate()

iris_dataset = (spark.read
                .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider")
                .option("applicationDataTypeId", os.getenv("APPLICATION_DATATYPE_ID"))
                .option("rootDAGContextId", os.getenv("ROOT_DAG_CONTEXT_ID"))
                .option("structTypeId", os.getenv("STRUCT_TYPE_ID"))
                .option("authToken", os.getenv("AUTH_TOKEN"))
                .option("baseUrl", os.getenv("BASE_URL"))
                .option("wsBaseUrl", os.getenv("WS_BASE_URL"))
                .load())

# converting spark dataframe to pandas dataframe
iris_dataset = iris_dataset.toPandas()

# make a copy of the original dataset (we will use it later for writing back to the object)
original_dataset = iris_dataset.copy(deep=True)
original_dataset_columns_x = [EXTERNAL_ID, "is_deleted", "sepal_length_cm", "sepal_width_cm", "petal_length_cm", "petal_width_cm"]
original_dataset_columns_y = ["species"]
original_dataset_columns = original_dataset_columns_x + original_dataset_columns_y

iris_dataset = iris_dataset[original_dataset_columns]

# encode target labels with value starting from 0 and n_classes-1
label_encoder = LabelEncoder()
iris_dataset['species'] = label_encoder.fit_transform(iris_dataset['species'])

# uncomment the following 2 lines in case we need to assign specific values to the classes
# classification_column_mapping = {'Iris-setosa': 0, 'Iris-versicolor': 1, 'Iris-virginica': 2}
# iris_dataset['encoded_species'] = iris_dataset['species'].map(classification_column_mapping)
print(f'iris_dataset: \n{iris_dataset.head()}')

# preparing data for training
iris_dataset_external_id = iris_dataset[EXTERNAL_ID]
y = iris_dataset["species"]
X = iris_dataset.drop(["species"], axis=1)

# split data into train and test sets (20% for testing)
x_train, x_test, y_train, y_test = train_test_split(X.values, y.values, test_size=0.2, random_state=42)

# print shapes of the train and test sets
print(f'x_train (shape): {x_train.shape}')
print(f'y_train (shape): {y_train.shape}')
print(f'x_test (shape): {x_test.shape}')
print(f'y_test (shape): {y_test.shape}')

# Train the model
model = XGBRegressor()
model.fit(pd.DataFrame(data=x_train, columns=original_dataset_columns_x).drop(EXTERNAL_ID, axis=1).astype(float), y_train, verbose=False)

# Predictions
predictions = model.predict(pd.DataFrame(data=x_test, columns=original_dataset_columns_x).drop(EXTERNAL_ID, axis=1).astype(float)).astype(int)
final_data = np.hstack((x_test, predictions.reshape(-1, 1)))
print(f'Predictions: \n{predictions}, \n{original_dataset.head()} \n{final_data}')

# Calculate accuracy
accuracy = accuracy_score(y_test, predictions)
print(f'Accuracy: {accuracy}')

# Write back to the object
spark_dataset_to_write = spark.createDataFrame(final_data, original_dataset_columns)
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
