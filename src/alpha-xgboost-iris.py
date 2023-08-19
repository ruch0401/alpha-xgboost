from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import xgboost as xgb

iris = load_iris()
numSamples, numFeatures = iris.data.shape
print(f'numSamples: {numSamples}, numFeatures: {numFeatures}')
print(f'feature_names: {list(iris.feature_names)}')
print(f'target_names: {list(iris.target_names)}')

x_train, x_test, y_train, y_test = train_test_split(iris.data, iris.target, test_size=0.2, random_state=42)

train = xgb.DMatrix(x_train, label=y_train)
test = xgb.DMatrix(x_test, label=y_test)

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
predictions = model.predict(test)
print(f'Predictions: {predictions}')

# Calculate accuracy
accuracy = accuracy_score(y_test, predictions)
print(f'Accuracy: {accuracy}')

# Save the model
model.dump_model('../output/dump.raw.txt')
