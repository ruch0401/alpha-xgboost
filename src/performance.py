import os
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id

from visualization import visualize_as_grouped_bar

# Load environment variables from .env file
load_dotenv()

spark = SparkSession.builder.appName("Spark Example to write data to Citta Object").getOrCreate()
input_path = "file:///home/ruchit/Desktop/output.csv"
output_path = "file:///home/ruchit/Desktop/testop"
EXTERNAL_ID = os.getenv("EXTERNAL_ID_FIELD_NAME")
columns = [EXTERNAL_ID, 'is_deleted', 'PRODUCT_ID', 'channel_id', 'market_id', 'customer_id', 'nrx',
           'trx', 'factored_nrx', 'total_units', 'amount', 'custom_metric1', 'custom_metric2', 'custom_metric3',
           'custom_metric4', 'custom_metric5', 'custom_metric6', 'custom_metric7', 'custom_metric8', 'custom_metric9',
           'custom_metric10']


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


def read_dataframe_from_citta(records_to_read):
    read_start_time = datetime.now()
    data_frame = (spark.read
                  .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider")
                  .option("applicationDataTypeName", os.getenv("APPLICATION"))
                  .option("rootDAGContextName", os.getenv("APP_GROUP"))
                  .option("structTypeName", os.getenv("APPLICATION_OBJECT"))
                  .option("token", os.getenv("TOKEN"))
                  .option("sessionString", os.getenv("SESSION_STRING"))
                  .load()
                  .limit(records_to_read))
    read_end_time = datetime.now()
    time_elapsed = read_end_time - read_start_time
    print(f'Total records read: {data_frame.count()}; Time taken to read: {time_elapsed}')
    return data_frame, time_elapsed.total_seconds()


def write_dataframe_to_citta(df4, records_to_write):
    write_start_time = datetime.now()
    limit = df4.limit(records_to_write)
    limit.write \
        .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider") \
        .option("applicationDataTypeName", os.getenv("APPLICATION")) \
        .option("rootDAGContextName", os.getenv("APP_GROUP")) \
        .option("structTypeName", os.getenv("APPLICATION_OBJECT")) \
        .option("token", os.getenv("TOKEN")) \
        .option("sessionString", os.getenv("SESSION_STRING")) \
        .option("readWriteMode", "write") \
        .option("isExecuteValidationInParallel", "true") \
        .mode("append") \
        .save()
    write_end_time = datetime.now()
    time_elapsed = write_end_time - write_start_time
    print(f'Total records written: {records_to_write}; Time taken to write: {time_elapsed}')
    return time_elapsed.total_seconds()


def reorder_columns(df5):
    df5 = df5.select(*columns)
    return df5


def test_write(records_to_write):
    df = read_csv_as_df()
    df = get_df_with_lowercase_cols(df)
    df = reorder_columns(df)
    df.show()
    write_time_elapsed = write_dataframe_to_citta(df, records_to_write)
    return write_time_elapsed


def test_read(records_to_read):
    df, read_time = read_dataframe_from_citta(records_to_read)
    return df, read_time


if __name__ == '__main__':
    data_volume = [10000, 20000]
    frequency = [1, 5]

    read_times_store = []
    write_times_store = []

    for record_count in data_volume:
        for freq in frequency:
            for i in range(0, freq):
                # we have to execute any operation for 'records' number of records, 'freq' number of times
                print(f'Executing {record_count}, {i}th time')
                write_time = test_write(record_count)
                df2, read_time = test_read(record_count)
                df2.show(10)
                read_times_store.append(read_time)
                write_times_store.append(write_time)

            print(f'Read times: {read_times_store}')
            print(f'Write times: {write_times_store}')
            visualize_as_grouped_bar(read_times_store, write_times_store, data_volume,
                                     'Read Time', 'Write Time', 'Number of records',
                                     'Time taken', f'Time taken to read and write {record_count} records with frequency {freq}',
                                     freq)
