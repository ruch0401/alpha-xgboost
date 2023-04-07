import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

# Load environment variables from .env file
load_dotenv()
EXTERNAL_ID = os.getenv("EXTERNAL_ID_FIELD_NAME")

# initialize spark session
spark = SparkSession.builder.appName('alpha-xgboost-iris').getOrCreate()


def load_dataset_as_pandas_df():
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