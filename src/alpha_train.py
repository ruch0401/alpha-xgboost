import os

import pandas as pd
from dotenv import load_dotenv
from xgboost import XGBRegressor

import alpha_data

# Load environment variables from .env file
load_dotenv()
EXTERNAL_ID = os.getenv("EXTERNAL_ID_FIELD_NAME")


def train_model():
    df = alpha_data.load_dataset_as_pandas_df()
    x_train, x_test, y_train, y_test = alpha_data.get_train_test_split(df)
    # Train the model
    model = XGBRegressor()
    model.fit(pd.DataFrame(data=x_train, columns=alpha_data.get_feature_columns()).drop(EXTERNAL_ID, axis=1)
              .astype(float), y_train, verbose=False)
    model.save_model("xgboost_on_iris.xgb")


if __name__ == '__main__':
    train_model()
