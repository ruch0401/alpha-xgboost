import os
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id

# Load environment variables from .env file
load_dotenv()

spark = SparkSession.builder.appName("Spark Example to write data to Citta Object").getOrCreate()
# input_path = "file:///home/ruchit/Documents/citta-code/csv-generator/1out_1000.csv"
input_path = "file:///home/ruchit/Desktop/output.csv"
output_path = "file:///home/ruchit/Desktop/testop"
EXTERNAL_ID = os.getenv("EXTERNAL_ID_FIELD_NAME")
columns = ['productlarge_external_id', 'is_deleted', 'PRODUCT_ID', 'channel_id', 'market_id', 'customer_id', 'nrx',
           'trx', 'factored_nrx', 'total_units', 'amount', 'custom_metric1', 'custom_metric2', 'custom_metric3',
           'custom_metric4', 'custom_metric5', 'custom_metric6', 'custom_metric7', 'custom_metric8', 'custom_metric9',
           'custom_metric10']
# columns = ['iris6_external_id', 'is_deleted', 'sepal_length_cm', 'sepal_width_cm', 'petal_length_cm', 'petal_width_cm',
#            'species']


def read_csv_as_df():
    csv = spark.read.csv(input_path, header=True, inferSchema=True)
    csv = csv.withColumn(EXTERNAL_ID, lit(monotonically_increasing_id()))
    return csv


def get_df_with_lowercase_cols(df1):
    columns_ = [col.lower() for col in df1.columns]
    df1 = df1.toDF(*columns_)
    return df1


def write_dataframe_to_local(df3):
    df3.coalesce(1).write.csv(output_path, header=True, mode="append")


def write_dataframe_to_citta(df4):
    write_start_time = datetime.now()
    limit = df4.limit(1)
    limit.write \
        .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider") \
        .option("applicationDataTypeName", os.getenv("APPLICATION")) \
        .option("rootDAGContextName", os.getenv("APP_GROUP")) \
        .option("structTypeName", os.getenv("APPLICATION_OBJECT")) \
        .option("token", os.getenv("TOKEN")) \
        .option("sessionString", os.getenv("SESSION_STRING")) \
        .option("readWriteMode", "write") \
        .mode("append") \
        .save()
    write_end_time = datetime.now()
    print(f'Total write time: {write_end_time - write_start_time}')


def read_dataframe_from_citta():
    read_start_time = datetime.now()
    iris_dataset = (spark.read
                    .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider")
                    .option("applicationDataTypeName", os.getenv("APPLICATION"))
                    .option("rootDAGContextName", os.getenv("APP_GROUP"))
                    .option("structTypeName", os.getenv("APPLICATION_OBJECT"))
                    .option("token", os.getenv("TOKEN"))
                    .option("sessionString", os.getenv("SESSION_STRING"))
                    .load())
    read_end_time = datetime.now()
    print(f'Total read time: {read_end_time - read_start_time}')
    return iris_dataset


def reorder_columns(df5):
    df5 = df5.select(*columns)
    return df5


if __name__ == '__main__':
    # df = read_dataframe_from_citta()
    # print(f'Total records read: {df.count()}')
    # print(df.limit(10).show())
    df = read_csv_as_df()
    df = get_df_with_lowercase_cols(df)
    df = reorder_columns(df)
    df.show()
    print(f'Total records read: {df.count()}')
    # write_dataframe_to_local(df)
    write_dataframe_to_citta(df)
