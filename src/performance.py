import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id
from visualization import *


# Load environment variables from .env file
load_dotenv()

spark = SparkSession.builder.appName("Spark Example to write data to Citta Object").getOrCreate()

input_path = "file:///home/ruchit/Desktop/output.csv"
output_path = "file:///home/ruchit/Desktop/testop"
EXTERNAL_ID = os.getenv("EXTERNAL_ID_FIELD_NAME")
APPLICATION = os.getenv("APPLICATION")
APPLICATION_GROUP = os.getenv("APP_GROUP")
APPLICATION_OBJECT = os.getenv("APPLICATION_OBJECT")
TOKEN = os.getenv("TOKEN")
SESSION_STRING = os.getenv("SESSION_STRING")
columns = ['is_deleted', 'PRODUCT_ID', 'channel_id', 'market_id', 'customer_id', 'nrx',
           'trx', 'factored_nrx', 'total_units', 'amount', 'custom_metric1', 'custom_metric2', 'custom_metric3',
           'custom_metric4', 'custom_metric5', 'custom_metric6', 'custom_metric7', 'custom_metric8', 'custom_metric9',
           'custom_metric10']
is_external_id_col_required = True
is_standard_object = False


def read_csv_as_df():
    csv = spark.read.csv(input_path, header=True, inferSchema=True)
    if is_external_id_col_required:
        csv = csv.withColumn(EXTERNAL_ID, lit(monotonically_increasing_id()))
    if is_standard_object:
        col_name = os.getenv("APPLICATION_OBJECT") + "_id"
        csv = csv.withColumn(col_name, lit(None).cast("double"))
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
                  .option("applicationDataTypeName", APPLICATION)
                  .option("rootDAGContextName", APPLICATION_GROUP)
                  .option("structTypeName", APPLICATION_OBJECT)
                  .option("token", TOKEN)
                  .option("sessionString", SESSION_STRING)
                  .load()
                  .limit(records_to_read))
    if data_frame.count() != records_to_read:
        error = f"Data frame which was read does not have the expected number of records. The test has probably failed. Expected: {records_to_read}, found: {data_frame.count()}"
        raise Exception(error)
    read_end_time = datetime.now()
    time_elapsed = read_end_time - read_start_time
    print(f'Total records read: {data_frame.count()}; Time taken to read: {time_elapsed}')
    return data_frame, time_elapsed.total_seconds()


def write_dataframe_to_citta(df4, records_to_write):
    write_start_time = datetime.now()
    limit = df4.limit(records_to_write)
    limit.write \
        .format("cloud.alpha.spark.providers.appobject.AppObjectTableProvider") \
        .option("applicationDataTypeName", APPLICATION) \
        .option("rootDAGContextName", APPLICATION_GROUP) \
        .option("structTypeName", APPLICATION_OBJECT) \
        .option("token", TOKEN) \
        .option("sessionString", SESSION_STRING) \
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


def get_geom_series(first_number, geometric_ratio, number_of_elements):
    series = []
    for i in range(1, number_of_elements + 1):
        series.append(first_number * geometric_ratio ** (i - 1))
    return series


if __name__ == '__main__':
    factor = 10000
    data_volume = [(i * factor) for i in get_geom_series(1, 2, 2)]
    frequency = get_geom_series(1, 2, 3)

    print(f"Data Volume: {data_volume}; Frequency: {frequency}")
    print('System arguments passed to the code are: ', sys.argv)

    if 'is_standard_object' in sys.argv:
        is_standard_object = True
        is_external_id_col_required = False
        columns.insert(0, APPLICATION_OBJECT + "_id")
        print('Columns that will be used for computation: ', columns)

    if 'is_standard_with_external_id' in sys.argv or 'is_bitemporal_object' in sys.argv:
        is_external_id_col_required = True
        columns.insert(0, EXTERNAL_ID)
        print('Columns that will be used for computation: ', columns)

    if 'is_complete_snapshot_object' in sys.argv or 'is_incremental_snapshot_object' in sys.argv or 'is_transactional_object' in sys.argv:
        is_external_id_col_required = False

    stats = {}
    for freq in frequency:
        for record_count in data_volume:
            for i in range(0, freq):
                # we have to execute any operation for 'records' number of records, 'freq' number of times
                print(f'Executing {record_count}, {i + 1}th time')
                write_time = test_write(record_count)
                df2, read_time = test_read(record_count)
                df2.show(10)
                entry = {'readTime': read_time, 'writeTime': write_time}
                if (freq, record_count) not in stats:
                    stats[(freq, record_count)] = []
                stats[(freq, record_count)].append(entry)
            print(f'The statistics are: {stats}')
    log_to_file(stats)
    visualize_as_grouped_bar(stats)