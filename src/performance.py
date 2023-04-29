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
columns = ['is_deleted', 'PRODUCT_ID', 'channel_id', 'market_id', 'customer_id', 'nrx',
           'trx', 'factored_nrx', 'total_units', 'amount', 'custom_metric1', 'custom_metric2', 'custom_metric3',
           'custom_metric4', 'custom_metric5', 'custom_metric6', 'custom_metric7', 'custom_metric8', 'custom_metric9',
           'custom_metric10']
is_external_id_col_required = True
is_standard_object = False

TOKEN = ""
SESSION_STRING = ""
APP_GROUP = ""
APPLICATION = ""
APPLICATION_OBJECT = ""
EXTERNAL_ID = ""


def read_csv_as_df():
    csv = spark.read.csv(input_path, header=True, inferSchema=True)
    if is_external_id_col_required:
        csv = csv.withColumn(EXTERNAL_ID, lit(monotonically_increasing_id()))
    if is_standard_object:
        col_name = APPLICATION_OBJECT + "_id"
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
                  .option("rootDAGContextName", APP_GROUP)
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
        .option("rootDAGContextName", APP_GROUP) \
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


def set_variables_for_standard_object():
    global TOKEN, SESSION_STRING, APP_GROUP, APPLICATION, APPLICATION_OBJECT, EXTERNAL_ID
    TOKEN = "eyJob3N0TmFtZSI6Imh0dHBzOi8vcWluc3RhbmNlMS10ZW5hbnQxLnFhMS5jaXR0YWRhdGEuY29tIiwicmVmcmVzaFRva2VuIjoiZXlKamRIa2lPaUpLVjFRaUxDSmxibU1pT2lKQk1qVTJSME5OSWl3aVlXeG5Jam9pVWxOQkxVOUJSVkFpZlEub2Q0bWFBeE1rMU5OUTcwSmxlSk1aVk9objdpQnZjWVBHeU1Hc1NKaG5KSG9IQm9yZjlrd2trYTBtSk8zSUYyMzdKYjl6RzVrdlZYMmtjb2wwZkVURW9seFBjaWQ2MElzSjlMb3gyT1JtQmRNNXVSSjlHd2RvMjJPZ25lNnhWZy1aYTQ2RUtjalpnY1VKRS1PTFlkQWFGMnF2dWdHRFF6UDJGcFRQeTRQaTh4V2J6U1lYclJ0Ynl2djNKSlRBTmVpb0wtQ01SV19HUl94UzBkTXRWam5fTFBITUdsdl83VnhWUkNGdU1lc1ZWT3FwVkZBS0h1bFEwZHBJamNTV3NGX2dFZUhYTjB1elAyQ1BiODhzOTJ1TmEzcmQxVEVhcEtsekhfWUlVeGVkRGlDWkFyS1dWNmFwSklCYXVCYlpTcTdaNkRXZEFTeU1PU2p1dlFadDVrbkVBLlBnZEZ5OG5LZWhsbS1adjcuSEtxX1FhU1VEUzhpcE5IWDlRLXhKREtWSHRpQVc5dFltZ04wX1dDSkk2dnJsQUVjY2d0ejB6UmE0aGI2c0lacGkyaTdtUXc4V0FSTGkweUJtNGZKNDFXX0Q3OElCU1JQNXFaYjF2Vm0yczhzaXhmTW4zRzFyMEg5YlRTTDNDeEFEUXRrQzNFZEppSldBbDlVOXBUZ25NLWJnM0lEYnJURUpWNmZ3Nl9YaldzTW1Xai1DOFNKU2N5TzFSQlhnT3ZlN3dkbDVsdTFWWE1zMDM0QWkwd09nZEFuQURLMlBSYjRsb1ZaUWtJa0Y2ZGxsX0ludmhDZVo0V25TSDJob18tNFU1Q0FMZ2pPVXNzZnJXVF8zcThYd0dxMlRicnd1bm9zTzlDU21aMmVJcUlrRnRtaDhueEJxZzZCVDVSQWdadFJ0b3docjhaT1VPUjdLbzBGcHNrWGdIdktvN3FXN3BHUUVhLTRLajJQclIyYTFPVHRtMF9GYlF5WUhLN29CMW9PS0tQejFObHZudHhrN1M3OTdzX0UtWVVmWXdyUEZwS0YzTkt2TXM4R0xTelVLanA0OU50THZtWWItc3hLTS1XN3dFVmtmTW1iVDJRLW1SS0FTai1HVHQ1SFd0MERwSWtUdXgwTXhBQ3pYVW9nemxtLThWZVF5T294V1VwUE5zQ2ZkUnhHd1lyamN4SEREcVc5X0RPLU1XSG5XaTdJeWR3Y2RwV2t2T2Y3cG9Pc3AyaXprUk9jc0Q4a0hyY0t1aXJFWmJFWTFsSzN6dGhUR2dTS2c5NERTZloxeFZiR2VRRXBWZmhrb1BtRkRtblBFWWswZEhtcERVbDlfdExJQXFIRDlUbDZYMmlBUTdNLU1Lb3hqZ1A2LXZnRTg1SXl5ak55NDhLdXhJeTVMVnpGeW9LS3UtaVBna0kwbjFLQjR1ZmpqbnhXdThXVlBPbzZWSkNzQ0sxaU9uQUZtenhzWEZCenlTYnVrbk5qTUpsdl9GcFJPaE90dUtNMm5YUVlydGgtS2Q5bGNhSTNpQjktMkFmOHZhSTN1UC1oLTY0ZUNKR2JFVmkyQ3MzSEx5cHVIcldFQlVxT29FRVZ2bmVjVnBPRXB6bFdxOWZXdHNqbXVJLXFaNWxoQmY5WWZ0bmdvTG1UYnp1VDVxTmc0bnVaY084ZnpEb2JhdS1qNk8xeXl1MG5KWF9PSTkzZlRtM1J1bkxTNTZVTUdPVVp5cnIxSGt6ZVUwMFpieVduenpBNjF1eDMteWkwNENXVlJaOEJkR2xDeEtXNklzZGU5bG5RSjY2dFQySW5PYkYxRGJKd1JiUzVDbld6YlVndWtWLWF0SXYxODd4Y0ZzVXZyeUxXMHZ6Y2UyblFXa0hoeS1TRFdUbklOUGUxVGNDbVRldWNvcENLeDcwZF80X1FWbkJEeGJJc3ZUSW1iYjNIcjl0UVBKUmJDamdnaHB3N2lUZHBJdDBGZm14Q1lfN2dmQ090NVZtRTU5SDdZNzJTeHFLT3RTTmtLWFJPOGZiU0ZWNU9VVjR6eFZITUptMFFaOG53VHlqTWFkeVR3WFZYbVh5cEFnMkUzQWFab1R5OEJaZFVveFBEb1hDV0tkM3JNd2ROS3dQYjU5QVFZVFc4ZGFDeGs2N2ZNdHMyWXM0cDB3YllIVEtZb2htS0JrTkpMNVYwN0JpNERwbjc1bWxQa3JocEM4bXBEVTJxckdQMC1zVl9iT0I4N05rLXB0Z2ctcjJpcExzeHA4RWtwS2lBUU5vTWdmMmpETldrRXV4RUk5NEVFV3owLkhaeHVMa2tNRXZnSTR0N1pzdUpoVHciLCJ3c0hvc3ROYW1lIjoid3NzOi8vcWluc3RhbmNlMS10ZW5hbnQxLndzLnFhMS5jaXR0YWRhdGEuY29tIn0="
    SESSION_STRING = "zps_34_67_3"
    APP_GROUP = "ag_large_data2"
    APPLICATION = "app1"
    APPLICATION_OBJECT = "productlargestandard"
    EXTERNAL_ID = "external_id"


def set_variables_for_standard_with_external_id():
    global TOKEN, SESSION_STRING, APP_GROUP, APPLICATION, APPLICATION_OBJECT, EXTERNAL_ID
    TOKEN = "eyJob3N0TmFtZSI6Imh0dHBzOi8vcWluc3RhbmNlMS10ZW5hbnQxLnFhMS5jaXR0YWRhdGEuY29tIiwicmVmcmVzaFRva2VuIjoiZXlKamRIa2lPaUpLVjFRaUxDSmxibU1pT2lKQk1qVTJSME5OSWl3aVlXeG5Jam9pVWxOQkxVOUJSVkFpZlEub2Q0bWFBeE1rMU5OUTcwSmxlSk1aVk9objdpQnZjWVBHeU1Hc1NKaG5KSG9IQm9yZjlrd2trYTBtSk8zSUYyMzdKYjl6RzVrdlZYMmtjb2wwZkVURW9seFBjaWQ2MElzSjlMb3gyT1JtQmRNNXVSSjlHd2RvMjJPZ25lNnhWZy1aYTQ2RUtjalpnY1VKRS1PTFlkQWFGMnF2dWdHRFF6UDJGcFRQeTRQaTh4V2J6U1lYclJ0Ynl2djNKSlRBTmVpb0wtQ01SV19HUl94UzBkTXRWam5fTFBITUdsdl83VnhWUkNGdU1lc1ZWT3FwVkZBS0h1bFEwZHBJamNTV3NGX2dFZUhYTjB1elAyQ1BiODhzOTJ1TmEzcmQxVEVhcEtsekhfWUlVeGVkRGlDWkFyS1dWNmFwSklCYXVCYlpTcTdaNkRXZEFTeU1PU2p1dlFadDVrbkVBLlBnZEZ5OG5LZWhsbS1adjcuSEtxX1FhU1VEUzhpcE5IWDlRLXhKREtWSHRpQVc5dFltZ04wX1dDSkk2dnJsQUVjY2d0ejB6UmE0aGI2c0lacGkyaTdtUXc4V0FSTGkweUJtNGZKNDFXX0Q3OElCU1JQNXFaYjF2Vm0yczhzaXhmTW4zRzFyMEg5YlRTTDNDeEFEUXRrQzNFZEppSldBbDlVOXBUZ25NLWJnM0lEYnJURUpWNmZ3Nl9YaldzTW1Xai1DOFNKU2N5TzFSQlhnT3ZlN3dkbDVsdTFWWE1zMDM0QWkwd09nZEFuQURLMlBSYjRsb1ZaUWtJa0Y2ZGxsX0ludmhDZVo0V25TSDJob18tNFU1Q0FMZ2pPVXNzZnJXVF8zcThYd0dxMlRicnd1bm9zTzlDU21aMmVJcUlrRnRtaDhueEJxZzZCVDVSQWdadFJ0b3docjhaT1VPUjdLbzBGcHNrWGdIdktvN3FXN3BHUUVhLTRLajJQclIyYTFPVHRtMF9GYlF5WUhLN29CMW9PS0tQejFObHZudHhrN1M3OTdzX0UtWVVmWXdyUEZwS0YzTkt2TXM4R0xTelVLanA0OU50THZtWWItc3hLTS1XN3dFVmtmTW1iVDJRLW1SS0FTai1HVHQ1SFd0MERwSWtUdXgwTXhBQ3pYVW9nemxtLThWZVF5T294V1VwUE5zQ2ZkUnhHd1lyamN4SEREcVc5X0RPLU1XSG5XaTdJeWR3Y2RwV2t2T2Y3cG9Pc3AyaXprUk9jc0Q4a0hyY0t1aXJFWmJFWTFsSzN6dGhUR2dTS2c5NERTZloxeFZiR2VRRXBWZmhrb1BtRkRtblBFWWswZEhtcERVbDlfdExJQXFIRDlUbDZYMmlBUTdNLU1Lb3hqZ1A2LXZnRTg1SXl5ak55NDhLdXhJeTVMVnpGeW9LS3UtaVBna0kwbjFLQjR1ZmpqbnhXdThXVlBPbzZWSkNzQ0sxaU9uQUZtenhzWEZCenlTYnVrbk5qTUpsdl9GcFJPaE90dUtNMm5YUVlydGgtS2Q5bGNhSTNpQjktMkFmOHZhSTN1UC1oLTY0ZUNKR2JFVmkyQ3MzSEx5cHVIcldFQlVxT29FRVZ2bmVjVnBPRXB6bFdxOWZXdHNqbXVJLXFaNWxoQmY5WWZ0bmdvTG1UYnp1VDVxTmc0bnVaY084ZnpEb2JhdS1qNk8xeXl1MG5KWF9PSTkzZlRtM1J1bkxTNTZVTUdPVVp5cnIxSGt6ZVUwMFpieVduenpBNjF1eDMteWkwNENXVlJaOEJkR2xDeEtXNklzZGU5bG5RSjY2dFQySW5PYkYxRGJKd1JiUzVDbld6YlVndWtWLWF0SXYxODd4Y0ZzVXZyeUxXMHZ6Y2UyblFXa0hoeS1TRFdUbklOUGUxVGNDbVRldWNvcENLeDcwZF80X1FWbkJEeGJJc3ZUSW1iYjNIcjl0UVBKUmJDamdnaHB3N2lUZHBJdDBGZm14Q1lfN2dmQ090NVZtRTU5SDdZNzJTeHFLT3RTTmtLWFJPOGZiU0ZWNU9VVjR6eFZITUptMFFaOG53VHlqTWFkeVR3WFZYbVh5cEFnMkUzQWFab1R5OEJaZFVveFBEb1hDV0tkM3JNd2ROS3dQYjU5QVFZVFc4ZGFDeGs2N2ZNdHMyWXM0cDB3YllIVEtZb2htS0JrTkpMNVYwN0JpNERwbjc1bWxQa3JocEM4bXBEVTJxckdQMC1zVl9iT0I4N05rLXB0Z2ctcjJpcExzeHA4RWtwS2lBUU5vTWdmMmpETldrRXV4RUk5NEVFV3owLkhaeHVMa2tNRXZnSTR0N1pzdUpoVHciLCJ3c0hvc3ROYW1lIjoid3NzOi8vcWluc3RhbmNlMS10ZW5hbnQxLndzLnFhMS5jaXR0YWRhdGEuY29tIn0="
    SESSION_STRING = "zps_34_67_3"
    APP_GROUP = "ag_large_data2"
    APPLICATION = "app1"
    APPLICATION_OBJECT = "productlargeextid"
    EXTERNAL_ID = "external_id"


def set_variables_for_bitemporal_object():
    global TOKEN, SESSION_STRING, APP_GROUP, APPLICATION, APPLICATION_OBJECT, EXTERNAL_ID
    TOKEN = "eyJob3N0TmFtZSI6Imh0dHBzOi8vcWluc3RhbmNlMS10ZW5hbnQxLnFhMS5jaXR0YWRhdGEuY29tIiwicmVmcmVzaFRva2VuIjoiZXlKamRIa2lPaUpLVjFRaUxDSmxibU1pT2lKQk1qVTJSME5OSWl3aVlXeG5Jam9pVWxOQkxVOUJSVkFpZlEub2Q0bWFBeE1rMU5OUTcwSmxlSk1aVk9objdpQnZjWVBHeU1Hc1NKaG5KSG9IQm9yZjlrd2trYTBtSk8zSUYyMzdKYjl6RzVrdlZYMmtjb2wwZkVURW9seFBjaWQ2MElzSjlMb3gyT1JtQmRNNXVSSjlHd2RvMjJPZ25lNnhWZy1aYTQ2RUtjalpnY1VKRS1PTFlkQWFGMnF2dWdHRFF6UDJGcFRQeTRQaTh4V2J6U1lYclJ0Ynl2djNKSlRBTmVpb0wtQ01SV19HUl94UzBkTXRWam5fTFBITUdsdl83VnhWUkNGdU1lc1ZWT3FwVkZBS0h1bFEwZHBJamNTV3NGX2dFZUhYTjB1elAyQ1BiODhzOTJ1TmEzcmQxVEVhcEtsekhfWUlVeGVkRGlDWkFyS1dWNmFwSklCYXVCYlpTcTdaNkRXZEFTeU1PU2p1dlFadDVrbkVBLlBnZEZ5OG5LZWhsbS1adjcuSEtxX1FhU1VEUzhpcE5IWDlRLXhKREtWSHRpQVc5dFltZ04wX1dDSkk2dnJsQUVjY2d0ejB6UmE0aGI2c0lacGkyaTdtUXc4V0FSTGkweUJtNGZKNDFXX0Q3OElCU1JQNXFaYjF2Vm0yczhzaXhmTW4zRzFyMEg5YlRTTDNDeEFEUXRrQzNFZEppSldBbDlVOXBUZ25NLWJnM0lEYnJURUpWNmZ3Nl9YaldzTW1Xai1DOFNKU2N5TzFSQlhnT3ZlN3dkbDVsdTFWWE1zMDM0QWkwd09nZEFuQURLMlBSYjRsb1ZaUWtJa0Y2ZGxsX0ludmhDZVo0V25TSDJob18tNFU1Q0FMZ2pPVXNzZnJXVF8zcThYd0dxMlRicnd1bm9zTzlDU21aMmVJcUlrRnRtaDhueEJxZzZCVDVSQWdadFJ0b3docjhaT1VPUjdLbzBGcHNrWGdIdktvN3FXN3BHUUVhLTRLajJQclIyYTFPVHRtMF9GYlF5WUhLN29CMW9PS0tQejFObHZudHhrN1M3OTdzX0UtWVVmWXdyUEZwS0YzTkt2TXM4R0xTelVLanA0OU50THZtWWItc3hLTS1XN3dFVmtmTW1iVDJRLW1SS0FTai1HVHQ1SFd0MERwSWtUdXgwTXhBQ3pYVW9nemxtLThWZVF5T294V1VwUE5zQ2ZkUnhHd1lyamN4SEREcVc5X0RPLU1XSG5XaTdJeWR3Y2RwV2t2T2Y3cG9Pc3AyaXprUk9jc0Q4a0hyY0t1aXJFWmJFWTFsSzN6dGhUR2dTS2c5NERTZloxeFZiR2VRRXBWZmhrb1BtRkRtblBFWWswZEhtcERVbDlfdExJQXFIRDlUbDZYMmlBUTdNLU1Lb3hqZ1A2LXZnRTg1SXl5ak55NDhLdXhJeTVMVnpGeW9LS3UtaVBna0kwbjFLQjR1ZmpqbnhXdThXVlBPbzZWSkNzQ0sxaU9uQUZtenhzWEZCenlTYnVrbk5qTUpsdl9GcFJPaE90dUtNMm5YUVlydGgtS2Q5bGNhSTNpQjktMkFmOHZhSTN1UC1oLTY0ZUNKR2JFVmkyQ3MzSEx5cHVIcldFQlVxT29FRVZ2bmVjVnBPRXB6bFdxOWZXdHNqbXVJLXFaNWxoQmY5WWZ0bmdvTG1UYnp1VDVxTmc0bnVaY084ZnpEb2JhdS1qNk8xeXl1MG5KWF9PSTkzZlRtM1J1bkxTNTZVTUdPVVp5cnIxSGt6ZVUwMFpieVduenpBNjF1eDMteWkwNENXVlJaOEJkR2xDeEtXNklzZGU5bG5RSjY2dFQySW5PYkYxRGJKd1JiUzVDbld6YlVndWtWLWF0SXYxODd4Y0ZzVXZyeUxXMHZ6Y2UyblFXa0hoeS1TRFdUbklOUGUxVGNDbVRldWNvcENLeDcwZF80X1FWbkJEeGJJc3ZUSW1iYjNIcjl0UVBKUmJDamdnaHB3N2lUZHBJdDBGZm14Q1lfN2dmQ090NVZtRTU5SDdZNzJTeHFLT3RTTmtLWFJPOGZiU0ZWNU9VVjR6eFZITUptMFFaOG53VHlqTWFkeVR3WFZYbVh5cEFnMkUzQWFab1R5OEJaZFVveFBEb1hDV0tkM3JNd2ROS3dQYjU5QVFZVFc4ZGFDeGs2N2ZNdHMyWXM0cDB3YllIVEtZb2htS0JrTkpMNVYwN0JpNERwbjc1bWxQa3JocEM4bXBEVTJxckdQMC1zVl9iT0I4N05rLXB0Z2ctcjJpcExzeHA4RWtwS2lBUU5vTWdmMmpETldrRXV4RUk5NEVFV3owLkhaeHVMa2tNRXZnSTR0N1pzdUpoVHciLCJ3c0hvc3ROYW1lIjoid3NzOi8vcWluc3RhbmNlMS10ZW5hbnQxLndzLnFhMS5jaXR0YWRhdGEuY29tIn0="
    SESSION_STRING = "zps_34_67_3"
    APP_GROUP = "ag_large_data2"
    APPLICATION = "app1"
    APPLICATION_OBJECT = "productlargebitemporal"
    EXTERNAL_ID = "external_id"


def set_variables_for_complete_snapshot():
    global TOKEN, SESSION_STRING, APP_GROUP, APPLICATION, APPLICATION_OBJECT, EXTERNAL_ID
    TOKEN = "eyJob3N0TmFtZSI6Imh0dHBzOi8vcWluc3RhbmNlMS10ZW5hbnQxLnFhMS5jaXR0YWRhdGEuY29tIiwicmVmcmVzaFRva2VuIjoiZXlKamRIa2lPaUpLVjFRaUxDSmxibU1pT2lKQk1qVTJSME5OSWl3aVlXeG5Jam9pVWxOQkxVOUJSVkFpZlEub2Q0bWFBeE1rMU5OUTcwSmxlSk1aVk9objdpQnZjWVBHeU1Hc1NKaG5KSG9IQm9yZjlrd2trYTBtSk8zSUYyMzdKYjl6RzVrdlZYMmtjb2wwZkVURW9seFBjaWQ2MElzSjlMb3gyT1JtQmRNNXVSSjlHd2RvMjJPZ25lNnhWZy1aYTQ2RUtjalpnY1VKRS1PTFlkQWFGMnF2dWdHRFF6UDJGcFRQeTRQaTh4V2J6U1lYclJ0Ynl2djNKSlRBTmVpb0wtQ01SV19HUl94UzBkTXRWam5fTFBITUdsdl83VnhWUkNGdU1lc1ZWT3FwVkZBS0h1bFEwZHBJamNTV3NGX2dFZUhYTjB1elAyQ1BiODhzOTJ1TmEzcmQxVEVhcEtsekhfWUlVeGVkRGlDWkFyS1dWNmFwSklCYXVCYlpTcTdaNkRXZEFTeU1PU2p1dlFadDVrbkVBLlBnZEZ5OG5LZWhsbS1adjcuSEtxX1FhU1VEUzhpcE5IWDlRLXhKREtWSHRpQVc5dFltZ04wX1dDSkk2dnJsQUVjY2d0ejB6UmE0aGI2c0lacGkyaTdtUXc4V0FSTGkweUJtNGZKNDFXX0Q3OElCU1JQNXFaYjF2Vm0yczhzaXhmTW4zRzFyMEg5YlRTTDNDeEFEUXRrQzNFZEppSldBbDlVOXBUZ25NLWJnM0lEYnJURUpWNmZ3Nl9YaldzTW1Xai1DOFNKU2N5TzFSQlhnT3ZlN3dkbDVsdTFWWE1zMDM0QWkwd09nZEFuQURLMlBSYjRsb1ZaUWtJa0Y2ZGxsX0ludmhDZVo0V25TSDJob18tNFU1Q0FMZ2pPVXNzZnJXVF8zcThYd0dxMlRicnd1bm9zTzlDU21aMmVJcUlrRnRtaDhueEJxZzZCVDVSQWdadFJ0b3docjhaT1VPUjdLbzBGcHNrWGdIdktvN3FXN3BHUUVhLTRLajJQclIyYTFPVHRtMF9GYlF5WUhLN29CMW9PS0tQejFObHZudHhrN1M3OTdzX0UtWVVmWXdyUEZwS0YzTkt2TXM4R0xTelVLanA0OU50THZtWWItc3hLTS1XN3dFVmtmTW1iVDJRLW1SS0FTai1HVHQ1SFd0MERwSWtUdXgwTXhBQ3pYVW9nemxtLThWZVF5T294V1VwUE5zQ2ZkUnhHd1lyamN4SEREcVc5X0RPLU1XSG5XaTdJeWR3Y2RwV2t2T2Y3cG9Pc3AyaXprUk9jc0Q4a0hyY0t1aXJFWmJFWTFsSzN6dGhUR2dTS2c5NERTZloxeFZiR2VRRXBWZmhrb1BtRkRtblBFWWswZEhtcERVbDlfdExJQXFIRDlUbDZYMmlBUTdNLU1Lb3hqZ1A2LXZnRTg1SXl5ak55NDhLdXhJeTVMVnpGeW9LS3UtaVBna0kwbjFLQjR1ZmpqbnhXdThXVlBPbzZWSkNzQ0sxaU9uQUZtenhzWEZCenlTYnVrbk5qTUpsdl9GcFJPaE90dUtNMm5YUVlydGgtS2Q5bGNhSTNpQjktMkFmOHZhSTN1UC1oLTY0ZUNKR2JFVmkyQ3MzSEx5cHVIcldFQlVxT29FRVZ2bmVjVnBPRXB6bFdxOWZXdHNqbXVJLXFaNWxoQmY5WWZ0bmdvTG1UYnp1VDVxTmc0bnVaY084ZnpEb2JhdS1qNk8xeXl1MG5KWF9PSTkzZlRtM1J1bkxTNTZVTUdPVVp5cnIxSGt6ZVUwMFpieVduenpBNjF1eDMteWkwNENXVlJaOEJkR2xDeEtXNklzZGU5bG5RSjY2dFQySW5PYkYxRGJKd1JiUzVDbld6YlVndWtWLWF0SXYxODd4Y0ZzVXZyeUxXMHZ6Y2UyblFXa0hoeS1TRFdUbklOUGUxVGNDbVRldWNvcENLeDcwZF80X1FWbkJEeGJJc3ZUSW1iYjNIcjl0UVBKUmJDamdnaHB3N2lUZHBJdDBGZm14Q1lfN2dmQ090NVZtRTU5SDdZNzJTeHFLT3RTTmtLWFJPOGZiU0ZWNU9VVjR6eFZITUptMFFaOG53VHlqTWFkeVR3WFZYbVh5cEFnMkUzQWFab1R5OEJaZFVveFBEb1hDV0tkM3JNd2ROS3dQYjU5QVFZVFc4ZGFDeGs2N2ZNdHMyWXM0cDB3YllIVEtZb2htS0JrTkpMNVYwN0JpNERwbjc1bWxQa3JocEM4bXBEVTJxckdQMC1zVl9iT0I4N05rLXB0Z2ctcjJpcExzeHA4RWtwS2lBUU5vTWdmMmpETldrRXV4RUk5NEVFV3owLkhaeHVMa2tNRXZnSTR0N1pzdUpoVHciLCJ3c0hvc3ROYW1lIjoid3NzOi8vcWluc3RhbmNlMS10ZW5hbnQxLndzLnFhMS5jaXR0YWRhdGEuY29tIn0="
    SESSION_STRING = "zps_34_67_3"
    APP_GROUP = "ag_large_data2"
    APPLICATION = "app1"
    APPLICATION_OBJECT = "productlargecompletesnapshot"
    EXTERNAL_ID = "external_id"


def set_variables_for_incremental_snapshot():
    global TOKEN, SESSION_STRING, APP_GROUP, APPLICATION, APPLICATION_OBJECT, EXTERNAL_ID
    TOKEN = "eyJob3N0TmFtZSI6Imh0dHBzOi8vcWluc3RhbmNlMS10ZW5hbnQxLnFhMS5jaXR0YWRhdGEuY29tIiwicmVmcmVzaFRva2VuIjoiZXlKamRIa2lPaUpLVjFRaUxDSmxibU1pT2lKQk1qVTJSME5OSWl3aVlXeG5Jam9pVWxOQkxVOUJSVkFpZlEub2Q0bWFBeE1rMU5OUTcwSmxlSk1aVk9objdpQnZjWVBHeU1Hc1NKaG5KSG9IQm9yZjlrd2trYTBtSk8zSUYyMzdKYjl6RzVrdlZYMmtjb2wwZkVURW9seFBjaWQ2MElzSjlMb3gyT1JtQmRNNXVSSjlHd2RvMjJPZ25lNnhWZy1aYTQ2RUtjalpnY1VKRS1PTFlkQWFGMnF2dWdHRFF6UDJGcFRQeTRQaTh4V2J6U1lYclJ0Ynl2djNKSlRBTmVpb0wtQ01SV19HUl94UzBkTXRWam5fTFBITUdsdl83VnhWUkNGdU1lc1ZWT3FwVkZBS0h1bFEwZHBJamNTV3NGX2dFZUhYTjB1elAyQ1BiODhzOTJ1TmEzcmQxVEVhcEtsekhfWUlVeGVkRGlDWkFyS1dWNmFwSklCYXVCYlpTcTdaNkRXZEFTeU1PU2p1dlFadDVrbkVBLlBnZEZ5OG5LZWhsbS1adjcuSEtxX1FhU1VEUzhpcE5IWDlRLXhKREtWSHRpQVc5dFltZ04wX1dDSkk2dnJsQUVjY2d0ejB6UmE0aGI2c0lacGkyaTdtUXc4V0FSTGkweUJtNGZKNDFXX0Q3OElCU1JQNXFaYjF2Vm0yczhzaXhmTW4zRzFyMEg5YlRTTDNDeEFEUXRrQzNFZEppSldBbDlVOXBUZ25NLWJnM0lEYnJURUpWNmZ3Nl9YaldzTW1Xai1DOFNKU2N5TzFSQlhnT3ZlN3dkbDVsdTFWWE1zMDM0QWkwd09nZEFuQURLMlBSYjRsb1ZaUWtJa0Y2ZGxsX0ludmhDZVo0V25TSDJob18tNFU1Q0FMZ2pPVXNzZnJXVF8zcThYd0dxMlRicnd1bm9zTzlDU21aMmVJcUlrRnRtaDhueEJxZzZCVDVSQWdadFJ0b3docjhaT1VPUjdLbzBGcHNrWGdIdktvN3FXN3BHUUVhLTRLajJQclIyYTFPVHRtMF9GYlF5WUhLN29CMW9PS0tQejFObHZudHhrN1M3OTdzX0UtWVVmWXdyUEZwS0YzTkt2TXM4R0xTelVLanA0OU50THZtWWItc3hLTS1XN3dFVmtmTW1iVDJRLW1SS0FTai1HVHQ1SFd0MERwSWtUdXgwTXhBQ3pYVW9nemxtLThWZVF5T294V1VwUE5zQ2ZkUnhHd1lyamN4SEREcVc5X0RPLU1XSG5XaTdJeWR3Y2RwV2t2T2Y3cG9Pc3AyaXprUk9jc0Q4a0hyY0t1aXJFWmJFWTFsSzN6dGhUR2dTS2c5NERTZloxeFZiR2VRRXBWZmhrb1BtRkRtblBFWWswZEhtcERVbDlfdExJQXFIRDlUbDZYMmlBUTdNLU1Lb3hqZ1A2LXZnRTg1SXl5ak55NDhLdXhJeTVMVnpGeW9LS3UtaVBna0kwbjFLQjR1ZmpqbnhXdThXVlBPbzZWSkNzQ0sxaU9uQUZtenhzWEZCenlTYnVrbk5qTUpsdl9GcFJPaE90dUtNMm5YUVlydGgtS2Q5bGNhSTNpQjktMkFmOHZhSTN1UC1oLTY0ZUNKR2JFVmkyQ3MzSEx5cHVIcldFQlVxT29FRVZ2bmVjVnBPRXB6bFdxOWZXdHNqbXVJLXFaNWxoQmY5WWZ0bmdvTG1UYnp1VDVxTmc0bnVaY084ZnpEb2JhdS1qNk8xeXl1MG5KWF9PSTkzZlRtM1J1bkxTNTZVTUdPVVp5cnIxSGt6ZVUwMFpieVduenpBNjF1eDMteWkwNENXVlJaOEJkR2xDeEtXNklzZGU5bG5RSjY2dFQySW5PYkYxRGJKd1JiUzVDbld6YlVndWtWLWF0SXYxODd4Y0ZzVXZyeUxXMHZ6Y2UyblFXa0hoeS1TRFdUbklOUGUxVGNDbVRldWNvcENLeDcwZF80X1FWbkJEeGJJc3ZUSW1iYjNIcjl0UVBKUmJDamdnaHB3N2lUZHBJdDBGZm14Q1lfN2dmQ090NVZtRTU5SDdZNzJTeHFLT3RTTmtLWFJPOGZiU0ZWNU9VVjR6eFZITUptMFFaOG53VHlqTWFkeVR3WFZYbVh5cEFnMkUzQWFab1R5OEJaZFVveFBEb1hDV0tkM3JNd2ROS3dQYjU5QVFZVFc4ZGFDeGs2N2ZNdHMyWXM0cDB3YllIVEtZb2htS0JrTkpMNVYwN0JpNERwbjc1bWxQa3JocEM4bXBEVTJxckdQMC1zVl9iT0I4N05rLXB0Z2ctcjJpcExzeHA4RWtwS2lBUU5vTWdmMmpETldrRXV4RUk5NEVFV3owLkhaeHVMa2tNRXZnSTR0N1pzdUpoVHciLCJ3c0hvc3ROYW1lIjoid3NzOi8vcWluc3RhbmNlMS10ZW5hbnQxLndzLnFhMS5jaXR0YWRhdGEuY29tIn0="
    SESSION_STRING = "zps_34_67_3"
    APP_GROUP = "ag_large_data2"
    APPLICATION = "app1"
    APPLICATION_OBJECT = "productlargeincrementalsnapshot"
    EXTERNAL_ID = "productlargeincrementalsnapshot_external_id"


def set_variables_for_transactional_object():
    global TOKEN, SESSION_STRING, APP_GROUP, APPLICATION, APPLICATION_OBJECT, EXTERNAL_ID
    TOKEN = ""
    SESSION_STRING = ""
    APP_GROUP = ""
    APPLICATION = ""
    APPLICATION_OBJECT = ""
    EXTERNAL_ID = ""
    raise NotImplementedError("Not implemented yet")


if __name__ == '__main__':
    factor = 10000
    data_volume = [(i * factor) for i in get_geom_series(1, 2, 2)]
    frequency = get_geom_series(1, 2, 3)

    print(f"Data Volume: {data_volume}; Frequency: {frequency}")
    print('System arguments passed to the code are: ', sys.argv)

    if 'is_standard_object' in sys.argv:
        set_variables_for_standard_object()
        is_standard_object = True
        is_external_id_col_required = False
        columns.insert(0, APPLICATION_OBJECT + "_id")
        print('Columns that will be used for computation: ', columns)

    if 'is_standard_with_external_id' in sys.argv:
        set_variables_for_standard_with_external_id()
        is_external_id_col_required = True
        columns.insert(0, EXTERNAL_ID)
        print('Columns that will be used for computation: ', columns)

    if 'is_bitemporal_object' in sys.argv:
        set_variables_for_bitemporal_object()
        is_external_id_col_required = True
        columns.insert(0, EXTERNAL_ID)
        print('Columns that will be used for computation: ', columns)

    if 'is_complete_snapshot_object' in sys.argv:
        set_variables_for_complete_snapshot()
        is_external_id_col_required = False

    if 'is_incremental_snapshot_object' in sys.argv:
        set_variables_for_incremental_snapshot()
        is_external_id_col_required = False

    if 'is_transactional_object' in sys.argv:
        set_variables_for_transactional_object()
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
