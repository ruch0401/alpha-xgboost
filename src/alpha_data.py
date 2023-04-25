import os
from datetime import datetime

from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

from config import *

print(f'environment variables are: {os.environ}')
print(f"the token is: {TOKEN}")

# initialize spark session
spark = (SparkSession
         .builder
         .appName('alpha-xgboost-iris')
         .getOrCreate())

# Get all Spark properties
all_properties = spark.sparkContext.getConf().getAll()
print(f'Count of all properties are: {len(all_properties)}')


# Print all properties
# for prop in all_properties:
#     print(f"{prop[0]} = {prop[1]}")


def load_dataset_as_pandas_df():
    print(f'SPARK: {os.environ["SPARK_HOME"]}')
    read_start_time = datetime.now()
    iris_dataset = (spark.read
                    .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider")
                    .option("applicationDataTypeName", APPLICATION)
                    .option("rootDAGContextName", APP_GROUP)
                    .option("structTypeName", APPLICATION_OBJECT)
                    .option("token", TOKEN)
                    .option("sessionString", SESSION_STRING)
                    .load())
    print(f'iris_dataset in alpha_data: {iris_dataset}')
    print(f'iris_dataset.count(): {iris_dataset.count()}')
    read_end_time = datetime.now()
    print(f'Total read time: {read_end_time - read_start_time}')
    # converting spark dataframe to pandas dataframe
    iris_dataset = iris_dataset.toPandas()
    return iris_dataset


def load_dataset_from_local_as_pandas_df():
    # print(f'SPARK: {os.environ["SPARK_HOME"]}')
    iris_dataset = spark.read.format("csv").option("header", "true").load("iris.csv")
    print(f'iris_dataset: \n{iris_dataset.head()}')
    # converting spark dataframe to pandas dataframe
    iris_dataset = iris_dataset.toPandas()
    return iris_dataset


def get_train_test_split(iris_dataset):
    original_dataset_columns = get_all_columns()
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
    x = iris_dataset.drop(get_label_columns(), axis=1)

    # split data into train and test sets (20% for testing)
    x_train, x_test, y_train, y_test = train_test_split(x.values, y.values, test_size=0.2, random_state=42)

    # print shapes of the train and test sets
    print(f'x_train (shape): {x_train.shape}')
    print(f'y_train (shape): {y_train.shape}')
    print(f'x_test (shape): {x_test.shape}')
    print(f'y_test (shape): {y_test.shape}')

    return x_train, x_test, y_train, y_test


def get_all_columns():
    original_dataset_columns_x = get_feature_columns()
    original_dataset_columns_y = get_label_columns()
    original_dataset_columns = original_dataset_columns_x + original_dataset_columns_y
    return original_dataset_columns


def get_label_columns():
    return ["species"]


def get_feature_columns():
    return [EXTERNAL_ID, "is_deleted", "sepal_length_cm", "sepal_width_cm", "petal_length_cm", "petal_width_cm"]
