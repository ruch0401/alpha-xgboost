from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import xgboost as xgb
from pyspark.sql import SparkSession
from sklearn.preprocessing import LabelEncoder

# initialize spark session
spark = SparkSession.builder.appName('alpha-xgboost-iris').getOrCreate()

iris_dataset = (spark.read
                .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider")
                .option("applicationDataTypeId", "4141")
                .option("rootDAGContextId", "2348")
                .option("structTypeId", "4142")
                .load())

# converting spark dataframe to pandas dataframe
iris_dataset = iris_dataset.toPandas()

iris_dataset = iris_dataset[["sepal_length_cm", "sepal_width_cm", "petal_length_cm", "petal_width_cm", "species"]]

# encode target labels with value starting from 0 and n_classes-1
label_encoder = LabelEncoder()
iris_dataset['species'] = label_encoder.fit_transform(iris_dataset['species'])

# uncomment the following 2 lines in case we need to assign specific values to the classes
# classification_column_mapping = {'Iris-setosa': 0, 'Iris-versicolor': 1, 'Iris-virginica': 2}
# iris_dataset['encoded_species'] = iris_dataset['species'].map(classification_column_mapping)
print(f'iris_dataset: \n{iris_dataset.head()}')

# preparing data for training
y = iris_dataset['species']
# converting all columns to float (this is only essential if the input dataframe as some columns of type string)
X = (iris_dataset.drop(['species'], axis=1)
     .astype(float))

# split data into train and test sets (20% for testing)
x_train, x_test, y_train, y_test = train_test_split(X.values, y.values, test_size=0.2, random_state=42)

# print shapes of the train and test sets
print(f'x_train (shape): {x_train.shape}')
print(f'y_train (shape): {y_train.shape}')
print(f'x_test (shape): {x_test.shape}')
print(f'y_test (shape): {y_test.shape}')

# convert data into xgb format
train = xgb.DMatrix(x_train, label=y_train)
test = xgb.DMatrix(x_test, label=y_test)

# set xgboost parameters
param = {
    'max_depth': 3,
    'eta': 0.3,
    'objective': 'multi:softmax',
    'num_class': 3
}

epochs = 5

# Train the model
model = xgb.train(param, train, epochs)

# Predictions
predictions = model.predict(test).astype(int)
print(f'Predictions: {predictions}')

# Calculate accuracy
accuracy = accuracy_score(y_test, predictions)
print(f'Accuracy: {accuracy}')
