from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import xgboost as xgb
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import time

spark = SparkSession.builder.appName('alpha-xgboost-iris').getOrCreate()
print(spark)

# appObjectTableProvider = spark._jvm.cloud.alpha.spark.providers.appobject.AppObjectTableProvider
# print(appObjectTableProvider)

iris_dataset = (spark.read
                .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider")
                .option("applicationDataTypeId", "4141")
                .option("rootDAGContextId", "2348")
                .option("structTypeId", "4142")
                .load())
# print(f'iris_dataset: {iris_dataset.toPandas()}')
iris_dataset_2 = iris_dataset.toPandas()
# count1 = iris_dataset.limit(10).toJSON().collect()
# print(f'count: {count1}')
test = iris_dataset_2[["sepal_length_cm", "sepal_width_cm", "petal_length_cm", "petal_width_cm", "species"]]
print(f'iris_dataset: {test.head()}')

iris = load_iris()
print(f'iris: {iris}')
numSamples, numFeatures = iris.data.shape
print(f'numSamples: {numSamples}, numFeatures: {numFeatures}')
print(f'feature_names: {list(iris.feature_names)}')
print(f'target_names: {list(iris.target_names)}')
print(f'data: {iris.data}')
print(f'target: {iris.target}')

x_train, x_test, y_train, y_test = train_test_split(iris.data, iris.target, test_size=0.2, random_state=42)
# print(f'x_train: {x_train}')
print(f'x_test: {x_test}')
# print(f'y_train: {y_train}')
print(f'y_test: {y_test}')
#
# train = xgb.DMatrix(x_train, label=y_train)
# test = xgb.DMatrix(x_test, label=y_test)
#
# param = {
#     'max_depth': 3,
#     'eta': 0.3,
#     'objective': 'multi:softmax',
#     'num_class': 3
# }
#
# epochs = 5
#
# # Train the model
# model = xgb.train(param, train, epochs)
#
# # Predictions
# predictions = model.predict(test)
# print(f'Predictions: {predictions}')
#
# # Calculate accuracy
# accuracy = accuracy_score(y_test, predictions)
# print(f'Accuracy: {accuracy}')
#
# time.sleep(300)
