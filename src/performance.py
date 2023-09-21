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
                  .option("appName", APPLICATION)
                  .option("appGroupName", APP_GROUP)
                  .option("objectName", APPLICATION_OBJECT)
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
        .option("appName", APPLICATION) \
        .option("appGroupName", APP_GROUP) \
        .option("objectName", APPLICATION_OBJECT) \
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
    # TOKEN = "eyJob3N0TmFtZSI6Imh0dHBzOi8vcWluc3RhbmNlMS10ZW5hbnQxLnFhMS5jaXR0YWRhdGEuY29tIiwicmVmcmVzaFRva2VuIjoiZXlKamRIa2lPaUpLVjFRaUxDSmxibU1pT2lKQk1qVTJSME5OSWl3aVlXeG5Jam9pVWxOQkxVOUJSVkFpZlEub2Q0bWFBeE1rMU5OUTcwSmxlSk1aVk9objdpQnZjWVBHeU1Hc1NKaG5KSG9IQm9yZjlrd2trYTBtSk8zSUYyMzdKYjl6RzVrdlZYMmtjb2wwZkVURW9seFBjaWQ2MElzSjlMb3gyT1JtQmRNNXVSSjlHd2RvMjJPZ25lNnhWZy1aYTQ2RUtjalpnY1VKRS1PTFlkQWFGMnF2dWdHRFF6UDJGcFRQeTRQaTh4V2J6U1lYclJ0Ynl2djNKSlRBTmVpb0wtQ01SV19HUl94UzBkTXRWam5fTFBITUdsdl83VnhWUkNGdU1lc1ZWT3FwVkZBS0h1bFEwZHBJamNTV3NGX2dFZUhYTjB1elAyQ1BiODhzOTJ1TmEzcmQxVEVhcEtsekhfWUlVeGVkRGlDWkFyS1dWNmFwSklCYXVCYlpTcTdaNkRXZEFTeU1PU2p1dlFadDVrbkVBLlBnZEZ5OG5LZWhsbS1adjcuSEtxX1FhU1VEUzhpcE5IWDlRLXhKREtWSHRpQVc5dFltZ04wX1dDSkk2dnJsQUVjY2d0ejB6UmE0aGI2c0lacGkyaTdtUXc4V0FSTGkweUJtNGZKNDFXX0Q3OElCU1JQNXFaYjF2Vm0yczhzaXhmTW4zRzFyMEg5YlRTTDNDeEFEUXRrQzNFZEppSldBbDlVOXBUZ25NLWJnM0lEYnJURUpWNmZ3Nl9YaldzTW1Xai1DOFNKU2N5TzFSQlhnT3ZlN3dkbDVsdTFWWE1zMDM0QWkwd09nZEFuQURLMlBSYjRsb1ZaUWtJa0Y2ZGxsX0ludmhDZVo0V25TSDJob18tNFU1Q0FMZ2pPVXNzZnJXVF8zcThYd0dxMlRicnd1bm9zTzlDU21aMmVJcUlrRnRtaDhueEJxZzZCVDVSQWdadFJ0b3docjhaT1VPUjdLbzBGcHNrWGdIdktvN3FXN3BHUUVhLTRLajJQclIyYTFPVHRtMF9GYlF5WUhLN29CMW9PS0tQejFObHZudHhrN1M3OTdzX0UtWVVmWXdyUEZwS0YzTkt2TXM4R0xTelVLanA0OU50THZtWWItc3hLTS1XN3dFVmtmTW1iVDJRLW1SS0FTai1HVHQ1SFd0MERwSWtUdXgwTXhBQ3pYVW9nemxtLThWZVF5T294V1VwUE5zQ2ZkUnhHd1lyamN4SEREcVc5X0RPLU1XSG5XaTdJeWR3Y2RwV2t2T2Y3cG9Pc3AyaXprUk9jc0Q4a0hyY0t1aXJFWmJFWTFsSzN6dGhUR2dTS2c5NERTZloxeFZiR2VRRXBWZmhrb1BtRkRtblBFWWswZEhtcERVbDlfdExJQXFIRDlUbDZYMmlBUTdNLU1Lb3hqZ1A2LXZnRTg1SXl5ak55NDhLdXhJeTVMVnpGeW9LS3UtaVBna0kwbjFLQjR1ZmpqbnhXdThXVlBPbzZWSkNzQ0sxaU9uQUZtenhzWEZCenlTYnVrbk5qTUpsdl9GcFJPaE90dUtNMm5YUVlydGgtS2Q5bGNhSTNpQjktMkFmOHZhSTN1UC1oLTY0ZUNKR2JFVmkyQ3MzSEx5cHVIcldFQlVxT29FRVZ2bmVjVnBPRXB6bFdxOWZXdHNqbXVJLXFaNWxoQmY5WWZ0bmdvTG1UYnp1VDVxTmc0bnVaY084ZnpEb2JhdS1qNk8xeXl1MG5KWF9PSTkzZlRtM1J1bkxTNTZVTUdPVVp5cnIxSGt6ZVUwMFpieVduenpBNjF1eDMteWkwNENXVlJaOEJkR2xDeEtXNklzZGU5bG5RSjY2dFQySW5PYkYxRGJKd1JiUzVDbld6YlVndWtWLWF0SXYxODd4Y0ZzVXZyeUxXMHZ6Y2UyblFXa0hoeS1TRFdUbklOUGUxVGNDbVRldWNvcENLeDcwZF80X1FWbkJEeGJJc3ZUSW1iYjNIcjl0UVBKUmJDamdnaHB3N2lUZHBJdDBGZm14Q1lfN2dmQ090NVZtRTU5SDdZNzJTeHFLT3RTTmtLWFJPOGZiU0ZWNU9VVjR6eFZITUptMFFaOG53VHlqTWFkeVR3WFZYbVh5cEFnMkUzQWFab1R5OEJaZFVveFBEb1hDV0tkM3JNd2ROS3dQYjU5QVFZVFc4ZGFDeGs2N2ZNdHMyWXM0cDB3YllIVEtZb2htS0JrTkpMNVYwN0JpNERwbjc1bWxQa3JocEM4bXBEVTJxckdQMC1zVl9iT0I4N05rLXB0Z2ctcjJpcExzeHA4RWtwS2lBUU5vTWdmMmpETldrRXV4RUk5NEVFV3owLkhaeHVMa2tNRXZnSTR0N1pzdUpoVHciLCJ3c0hvc3ROYW1lIjoid3NzOi8vcWluc3RhbmNlMS10ZW5hbnQxLndzLnFhMS5jaXR0YWRhdGEuY29tIn0="
    # SESSION_STRING = "zps_34_67_3"
    # APP_GROUP = "ag_large_data2"
    # APPLICATION = "app1"
    # APPLICATION_OBJECT = "productlargestandard"
    # EXTERNAL_ID = "external_id"
    TOKEN = "eyJob3N0TmFtZSI6Imh0dHBzOi8vYWxwaGEzLXRlbmFudDMtcnVjaGl0LmFscGhhLmxvY2FsOjk5OTgiLCJyZWZyZXNoVG9rZW4iOiJleUpqZEhraU9pSktWMVFpTENKbGJtTWlPaUpCTWpVMlIwTk5JaXdpWVd4bklqb2lVbE5CTFU5QlJWQWlmUS50RkFKNkl3akxSTnZnX1kwNkVRM0JOVEl3a1JjdjZ2YnJiUU05dmhxTVlfdXdrMTAweWtuRXFELVZXUG5WS0NYNkp4RXM0MmFpVnFxajg1SlFjQlpuaUJKWmFnQTIweHNkUjY4WEttVmpQcXAtNUUwUnVBSkh4SjN2T2VTVXdUOUVBZlByNWQxc3dMbVF5NXJkc0ttdnM0bEpfaUZGR0JGd01sS0RYT3huWXJIaUpvNUtsNXhuVEl5c1pRRzRFVklEV0hGMGZvY21YQ0lkMnhsOXlXaUplY2RZTlItbEFURjN4N2gyaWM4eGMyYVlPS3lBeWtWb3c5OW9XOW9TS0JjSm5IMzd6VWs2OXJUSGpaX21CVHJ6VS1LS1VGVkFoLUU4aVJQcnFGRUJ3ZXJpWlpzTlRTd1pqMDQwdjZKeHJtUTVOcVBLVkhzc25Zd3lpZlZCMUpPVVEuTWY2S3cxd1pSSWFTZXdBeC51QlVGU1FJUVVXNS1QREdpQldRNXZtOXVWTUJ6RUpOSTcydWprVkR3dm9OTkdLNDJDMVlPRmplMWgxcjJjX2ZFa2pxRjJnZUV1SGQ0N3h3U3kyWjM4Y1c0bTBqellxazlFVVJEbDR4WjZzM1dCc2xFWVRDVng4Y1BSS2ZPR1M3UlE5d3A1emp2Y2t1a29iVUVlSklsNXpmRVNiRDN5SXZVaFdfOW00TlpQZkV2c2ExX09uRDhuY2lxeVlGWmUwZDFGeU5GYjR2ZEFiN01GdDBsWThRSjlQRDktbGktcEhoMXpfUDJaX3ItWVREZ0JNblpsbUdRZ0lHckFUSzVHMVhXRHROQmdldGFrQ01FVTBYNld3YkZLaDlQLVhfaWRnX0lacW1QeEFUc2VuNXNVU3VsdU5kWnc3Ml9pdFlwQWM4RVdqSE9nWVhOSGplT2pvMUpJOUZ1ampaTEhyTk9NbTVUOVJyUm5IZlYxazZQTGdLYVdJRlFLNUF6N2R1bEpMUjZzNE9INjJMOFAzaXBEMVdxYW1WMnlFUjJhRXZZWGl1ZWdjV3VubG5qOWVGZ09halFCdUJRTzBKZFRUZFRvaU9KRVlRZnlyanBaRmtWSC1heUUyczBtWXZOczJDWTZralpQSlY3MGctNUtBWWM4NlF3TERfU0otSVhYQ3UzYmZPMEloU094dEtEVngzdVZDQnhlby1xa0RZb2J3aXNVSVJjUDhGWGFXS2p2dUgyZFJZN1BrRWdDa0JYOWlFZzY5R0NfWlBWWGxIX3VMR2g5OVI0Q0M2RXlTTEpPNG1XOWlRcFdzcUFuMEk3RjRjcV8weDBleG1ONzVmVjNpVkJpUEpobVpkSzRKb0RxWlZFTGpRbk90Y2RVVFNfRDFuekFJMlVqTm5NX3ZxWmhnNHJ1cy1jcF95c3ZhWF9kb1Bjb0tFdEVCWVZfWWRwbWZNOGNSQmpMcWIyWVdLV2FldmJ4V3BLN01ySzcybk9Wb1VxZWhzNC1naklNMFQ0bnMyUC1HZk9NOGk3M2xnckRrVTktQnRKRVBXUXI4VUlKNk1IYXQ0MXhJXzF3Qk9UajVSaVNfUjRDcElZZVhpTHpIUk8xWGszek9BNG5FdnpUczVmVmRzaG10MG1acTVuZUlnN09oTXJETkJwZ2tOVTg2RTkxby1sZGFjU3hldHhPVERMMDN2T2k5Y3ROX3RzcUhkanhkaUtSMktiSWtsMzJaMld6dnBlU2NaZVhzUi1QeWcwSGZCbUF4cHZOaUFRS2pwdnV6ekRhT2VCOXBpUUFJaElNU29QcXFjWWhPRkxtdXFKanRJSG1maEd5ZklMVFAxT0U3X284SHhaeHFZV0RXdzhaUGY2dHA4RXF3QjlnbUdhazZpZWtkRXFMMTJjSzdvMm41ZUdmMGZDVmRxSkkxZFNGQU1jRzdFTUFLc1dwZGNzOWJ0OE13eDR5MGh0MFFaa09lR04tZE8xSDBYTXI5MVNNQjN2R3prMWhmU2NuOHNIYzYtQjR0NTZrWVpxNXdYQjFFamNJSWlmd2lXbk1XS0lpeVUyMmhibzlxdFotT0I1RDFZY0Nrbi1qWERrRkFRQmJPbHJMdkMxeEhVekE5RGhGSmpPd1V5NDZyRVNMUjdESXJWY0xmMnl4Mzk1MVlVTGQ2UGpucTAua2hvamR2YUQyY0Z2cU92RWFrM3psQSIsIndzSG9zdE5hbWUiOiJ3czovL2FscGhhMy10ZW5hbnQzLXJ1Y2hpdC53cy5hbHBoYS5sb2NhbDo5OTk5L3dzIn0="
    SESSION_STRING = "zps_5_9_3"
    APP_GROUP = "app-group-largedata"
    APPLICATION = "app_productlarge"
    APPLICATION_OBJECT = "productlarge"
    EXTERNAL_ID = "productlarge_external_id"


def set_variables_for_standard_with_external_id():
    global TOKEN, SESSION_STRING, APP_GROUP, APPLICATION, APPLICATION_OBJECT, EXTERNAL_ID
    # TOKEN = "eyJob3N0TmFtZSI6Imh0dHBzOi8vcWluc3RhbmNlMS10ZW5hbnQxLnFhMS5jaXR0YWRhdGEuY29tIiwicmVmcmVzaFRva2VuIjoiZXlKamRIa2lPaUpLVjFRaUxDSmxibU1pT2lKQk1qVTJSME5OSWl3aVlXeG5Jam9pVWxOQkxVOUJSVkFpZlEub2Q0bWFBeE1rMU5OUTcwSmxlSk1aVk9objdpQnZjWVBHeU1Hc1NKaG5KSG9IQm9yZjlrd2trYTBtSk8zSUYyMzdKYjl6RzVrdlZYMmtjb2wwZkVURW9seFBjaWQ2MElzSjlMb3gyT1JtQmRNNXVSSjlHd2RvMjJPZ25lNnhWZy1aYTQ2RUtjalpnY1VKRS1PTFlkQWFGMnF2dWdHRFF6UDJGcFRQeTRQaTh4V2J6U1lYclJ0Ynl2djNKSlRBTmVpb0wtQ01SV19HUl94UzBkTXRWam5fTFBITUdsdl83VnhWUkNGdU1lc1ZWT3FwVkZBS0h1bFEwZHBJamNTV3NGX2dFZUhYTjB1elAyQ1BiODhzOTJ1TmEzcmQxVEVhcEtsekhfWUlVeGVkRGlDWkFyS1dWNmFwSklCYXVCYlpTcTdaNkRXZEFTeU1PU2p1dlFadDVrbkVBLlBnZEZ5OG5LZWhsbS1adjcuSEtxX1FhU1VEUzhpcE5IWDlRLXhKREtWSHRpQVc5dFltZ04wX1dDSkk2dnJsQUVjY2d0ejB6UmE0aGI2c0lacGkyaTdtUXc4V0FSTGkweUJtNGZKNDFXX0Q3OElCU1JQNXFaYjF2Vm0yczhzaXhmTW4zRzFyMEg5YlRTTDNDeEFEUXRrQzNFZEppSldBbDlVOXBUZ25NLWJnM0lEYnJURUpWNmZ3Nl9YaldzTW1Xai1DOFNKU2N5TzFSQlhnT3ZlN3dkbDVsdTFWWE1zMDM0QWkwd09nZEFuQURLMlBSYjRsb1ZaUWtJa0Y2ZGxsX0ludmhDZVo0V25TSDJob18tNFU1Q0FMZ2pPVXNzZnJXVF8zcThYd0dxMlRicnd1bm9zTzlDU21aMmVJcUlrRnRtaDhueEJxZzZCVDVSQWdadFJ0b3docjhaT1VPUjdLbzBGcHNrWGdIdktvN3FXN3BHUUVhLTRLajJQclIyYTFPVHRtMF9GYlF5WUhLN29CMW9PS0tQejFObHZudHhrN1M3OTdzX0UtWVVmWXdyUEZwS0YzTkt2TXM4R0xTelVLanA0OU50THZtWWItc3hLTS1XN3dFVmtmTW1iVDJRLW1SS0FTai1HVHQ1SFd0MERwSWtUdXgwTXhBQ3pYVW9nemxtLThWZVF5T294V1VwUE5zQ2ZkUnhHd1lyamN4SEREcVc5X0RPLU1XSG5XaTdJeWR3Y2RwV2t2T2Y3cG9Pc3AyaXprUk9jc0Q4a0hyY0t1aXJFWmJFWTFsSzN6dGhUR2dTS2c5NERTZloxeFZiR2VRRXBWZmhrb1BtRkRtblBFWWswZEhtcERVbDlfdExJQXFIRDlUbDZYMmlBUTdNLU1Lb3hqZ1A2LXZnRTg1SXl5ak55NDhLdXhJeTVMVnpGeW9LS3UtaVBna0kwbjFLQjR1ZmpqbnhXdThXVlBPbzZWSkNzQ0sxaU9uQUZtenhzWEZCenlTYnVrbk5qTUpsdl9GcFJPaE90dUtNMm5YUVlydGgtS2Q5bGNhSTNpQjktMkFmOHZhSTN1UC1oLTY0ZUNKR2JFVmkyQ3MzSEx5cHVIcldFQlVxT29FRVZ2bmVjVnBPRXB6bFdxOWZXdHNqbXVJLXFaNWxoQmY5WWZ0bmdvTG1UYnp1VDVxTmc0bnVaY084ZnpEb2JhdS1qNk8xeXl1MG5KWF9PSTkzZlRtM1J1bkxTNTZVTUdPVVp5cnIxSGt6ZVUwMFpieVduenpBNjF1eDMteWkwNENXVlJaOEJkR2xDeEtXNklzZGU5bG5RSjY2dFQySW5PYkYxRGJKd1JiUzVDbld6YlVndWtWLWF0SXYxODd4Y0ZzVXZyeUxXMHZ6Y2UyblFXa0hoeS1TRFdUbklOUGUxVGNDbVRldWNvcENLeDcwZF80X1FWbkJEeGJJc3ZUSW1iYjNIcjl0UVBKUmJDamdnaHB3N2lUZHBJdDBGZm14Q1lfN2dmQ090NVZtRTU5SDdZNzJTeHFLT3RTTmtLWFJPOGZiU0ZWNU9VVjR6eFZITUptMFFaOG53VHlqTWFkeVR3WFZYbVh5cEFnMkUzQWFab1R5OEJaZFVveFBEb1hDV0tkM3JNd2ROS3dQYjU5QVFZVFc4ZGFDeGs2N2ZNdHMyWXM0cDB3YllIVEtZb2htS0JrTkpMNVYwN0JpNERwbjc1bWxQa3JocEM4bXBEVTJxckdQMC1zVl9iT0I4N05rLXB0Z2ctcjJpcExzeHA4RWtwS2lBUU5vTWdmMmpETldrRXV4RUk5NEVFV3owLkhaeHVMa2tNRXZnSTR0N1pzdUpoVHciLCJ3c0hvc3ROYW1lIjoid3NzOi8vcWluc3RhbmNlMS10ZW5hbnQxLndzLnFhMS5jaXR0YWRhdGEuY29tIn0="
    # SESSION_STRING = "zps_34_67_3"
    # APP_GROUP = "ag_large_data2"
    # APPLICATION = "app1"
    # APPLICATION_OBJECT = "productlargeextid"
    # EXTERNAL_ID = "external_id"
    TOKEN = "eyJob3N0TmFtZSI6Imh0dHBzOi8vYWxwaGEzLXRlbmFudDMtcnVjaGl0LmFscGhhLmxvY2FsOjk5OTgiLCJyZWZyZXNoVG9rZW4iOiJleUpqZEhraU9pSktWMVFpTENKbGJtTWlPaUpCTWpVMlIwTk5JaXdpWVd4bklqb2lVbE5CTFU5QlJWQWlmUS50RkFKNkl3akxSTnZnX1kwNkVRM0JOVEl3a1JjdjZ2YnJiUU05dmhxTVlfdXdrMTAweWtuRXFELVZXUG5WS0NYNkp4RXM0MmFpVnFxajg1SlFjQlpuaUJKWmFnQTIweHNkUjY4WEttVmpQcXAtNUUwUnVBSkh4SjN2T2VTVXdUOUVBZlByNWQxc3dMbVF5NXJkc0ttdnM0bEpfaUZGR0JGd01sS0RYT3huWXJIaUpvNUtsNXhuVEl5c1pRRzRFVklEV0hGMGZvY21YQ0lkMnhsOXlXaUplY2RZTlItbEFURjN4N2gyaWM4eGMyYVlPS3lBeWtWb3c5OW9XOW9TS0JjSm5IMzd6VWs2OXJUSGpaX21CVHJ6VS1LS1VGVkFoLUU4aVJQcnFGRUJ3ZXJpWlpzTlRTd1pqMDQwdjZKeHJtUTVOcVBLVkhzc25Zd3lpZlZCMUpPVVEuTWY2S3cxd1pSSWFTZXdBeC51QlVGU1FJUVVXNS1QREdpQldRNXZtOXVWTUJ6RUpOSTcydWprVkR3dm9OTkdLNDJDMVlPRmplMWgxcjJjX2ZFa2pxRjJnZUV1SGQ0N3h3U3kyWjM4Y1c0bTBqellxazlFVVJEbDR4WjZzM1dCc2xFWVRDVng4Y1BSS2ZPR1M3UlE5d3A1emp2Y2t1a29iVUVlSklsNXpmRVNiRDN5SXZVaFdfOW00TlpQZkV2c2ExX09uRDhuY2lxeVlGWmUwZDFGeU5GYjR2ZEFiN01GdDBsWThRSjlQRDktbGktcEhoMXpfUDJaX3ItWVREZ0JNblpsbUdRZ0lHckFUSzVHMVhXRHROQmdldGFrQ01FVTBYNld3YkZLaDlQLVhfaWRnX0lacW1QeEFUc2VuNXNVU3VsdU5kWnc3Ml9pdFlwQWM4RVdqSE9nWVhOSGplT2pvMUpJOUZ1ampaTEhyTk9NbTVUOVJyUm5IZlYxazZQTGdLYVdJRlFLNUF6N2R1bEpMUjZzNE9INjJMOFAzaXBEMVdxYW1WMnlFUjJhRXZZWGl1ZWdjV3VubG5qOWVGZ09halFCdUJRTzBKZFRUZFRvaU9KRVlRZnlyanBaRmtWSC1heUUyczBtWXZOczJDWTZralpQSlY3MGctNUtBWWM4NlF3TERfU0otSVhYQ3UzYmZPMEloU094dEtEVngzdVZDQnhlby1xa0RZb2J3aXNVSVJjUDhGWGFXS2p2dUgyZFJZN1BrRWdDa0JYOWlFZzY5R0NfWlBWWGxIX3VMR2g5OVI0Q0M2RXlTTEpPNG1XOWlRcFdzcUFuMEk3RjRjcV8weDBleG1ONzVmVjNpVkJpUEpobVpkSzRKb0RxWlZFTGpRbk90Y2RVVFNfRDFuekFJMlVqTm5NX3ZxWmhnNHJ1cy1jcF95c3ZhWF9kb1Bjb0tFdEVCWVZfWWRwbWZNOGNSQmpMcWIyWVdLV2FldmJ4V3BLN01ySzcybk9Wb1VxZWhzNC1naklNMFQ0bnMyUC1HZk9NOGk3M2xnckRrVTktQnRKRVBXUXI4VUlKNk1IYXQ0MXhJXzF3Qk9UajVSaVNfUjRDcElZZVhpTHpIUk8xWGszek9BNG5FdnpUczVmVmRzaG10MG1acTVuZUlnN09oTXJETkJwZ2tOVTg2RTkxby1sZGFjU3hldHhPVERMMDN2T2k5Y3ROX3RzcUhkanhkaUtSMktiSWtsMzJaMld6dnBlU2NaZVhzUi1QeWcwSGZCbUF4cHZOaUFRS2pwdnV6ekRhT2VCOXBpUUFJaElNU29QcXFjWWhPRkxtdXFKanRJSG1maEd5ZklMVFAxT0U3X284SHhaeHFZV0RXdzhaUGY2dHA4RXF3QjlnbUdhazZpZWtkRXFMMTJjSzdvMm41ZUdmMGZDVmRxSkkxZFNGQU1jRzdFTUFLc1dwZGNzOWJ0OE13eDR5MGh0MFFaa09lR04tZE8xSDBYTXI5MVNNQjN2R3prMWhmU2NuOHNIYzYtQjR0NTZrWVpxNXdYQjFFamNJSWlmd2lXbk1XS0lpeVUyMmhibzlxdFotT0I1RDFZY0Nrbi1qWERrRkFRQmJPbHJMdkMxeEhVekE5RGhGSmpPd1V5NDZyRVNMUjdESXJWY0xmMnl4Mzk1MVlVTGQ2UGpucTAua2hvamR2YUQyY0Z2cU92RWFrM3psQSIsIndzSG9zdE5hbWUiOiJ3czovL2FscGhhMy10ZW5hbnQzLXJ1Y2hpdC53cy5hbHBoYS5sb2NhbDo5OTk5L3dzIn0="
    SESSION_STRING = "zps_5_9_3"
    APP_GROUP = "app-group-largedata"
    APPLICATION = "app_productlarge"
    APPLICATION_OBJECT = "productlarge"
    EXTERNAL_ID = "productlarge_external_id"


def set_variables_for_bitemporal_object():
    global TOKEN, SESSION_STRING, APP_GROUP, APPLICATION, APPLICATION_OBJECT, EXTERNAL_ID
    TOKEN = "eyJob3N0TmFtZSI6Imh0dHBzOi8vcWluc3RhbmNlMS10ZW5hbnQxLnFhMS5jaXR0YWRhdGEuY29tIiwicmVmcmVzaFRva2VuIjoiZXlKamRIa2lPaUpLVjFRaUxDSmxibU1pT2lKQk1qVTJSME5OSWl3aVlXeG5Jam9pVWxOQkxVOUJSVkFpZlEuUDBueE95Uld3Sk80QjNJUER0ZzNINTk0QkxRSTU0cXo3QVhtdTlSNVJ3RkRfc3JlUzd5MTV3azBOeWJpejU0RHNya210UThNd0ZjSFBuWmhFXzYtakt4ek1pNTZpaFR0YXctZUlFZ05OWDJMTko3WHAxME5RTE9tb3N6bUJqRXMxWlY4c2xDT3c4S185VkFXSU83N3c4MGZpYmdmTHZTcW5mZjJrQjVjNmJTaUIwUlo4R2J3UmZ5a3BJX25raXRrQWZRTkpGQ2JranNwUDZQeFZfSVM3N29WcGpPSF9oZVhJOGVqTU9OUDFjUmNZSlJURi00dVJpT3hlXzVLR2VLMDJaVERNZzRPZWdRWF9iTnJiYlh4LW5aMXg0ZGRpMWNDNUdncHVSYUxWaG5JSDlSVlFfZmVSYTY2djhUOV9qSmxfZ0FUN1IxYVFWSEtrdTQ5TC14MG53LlJzTEgtOG91MDlHOEduMVcub3VXcHVLNzJfMWk1by1pZ1RmMlJRRGRaTFpqSXdNeUdSYjlmVHlWRGp3MGZsOVJpRHpfQnNzZnVTUVh2RWdodlg0V0QtckxuVDgydlh3TFpHcDdFNldra2pKMmU1d2h6eDhtMEFiOUJHOFo2YkQ0ckRMbVg1LWpZZVYwcWhfenZDczZNbjhXajZnZloyZXp5SVg5Y2FaOGhFbktYNkhtVTFFaVFKc0FjVHJGQTBLWUtrSEdramlkVXdrQk5qWGxmZkxGYU5wSUhYLUM0Nmk0bUkySWZPRzZmSW1rTDQtYTk2RHJIcktCaktYWmhLcjJjSFVsTVA1RWJpR0xaT29aU09PeC1NdE45SDNPYS1kTjAzb3RuaG4tM3lVSGUxLXpTbnBaREdtNVZ4NEE0LUFWRWtkZXpDSDlZdkVwVTRWYVRFWFJUMjJaQjlnRnZUYXdRc2pYLWg0Z3NESmxkbWo5R0xMeHBPblhycEhVY2pIU0NXblVvcUpsWEd1a01RaDRHWE5QbHgtVDVPMVd5VVRWN3NDQ2tVaDhQeUh5ODZPcm5JVVRlZ0lWd2VUVDRzTzgyNGNuQjE1UVVvdWxWSm5qekJ0a2RyTVRWaDZrTTZwYy1QMHZLRGttRTc5NFFwRnJOWVdTSURuMnYwR1QySGNDZ1p4V01LNUNxS2pFWmJhWTFLSklXMzgzUXJiZmoxcU9vSkYwNUI0SkNENE9SaE5XQTFVdlRISkY2Q2JQcFpwdmRKZGN4RElVdnpPZ285RTU1bFJFM05FUXVVV1lIRm9fcHV3dkw0M2FfY05EcVhxckczLWhkckE2bElGU1VEU2tkRWliZXpXS092RC1UZE9uWHdTZGR3MktxeWIxMkJjWGZubElpVjFpMC03VFZucFJ3UWhkbVIxZm9ZRjk4RlBNMDVwZTZEUm0zVmpMNDdLTVNXdms2NUVJc0FzYzJ4OW5DbzdZY2hhbGZNZWgtQTJwemY0bURXVVVZSUxVVFlncGwxckdNUVN2NENMY09ZMUx4OENjSWdMRWh0QVZPZUNjMDczcGlfdUxOVWJMNHZwamEzUVJWZ1JwT1pCdXJDOTFzTlR5WjdWWEdZTHg2Tlp3Zjl1Y2ZBeWpjbmRTa2Itc3BIZGp3LWZORy1jeWN1cjFVbGNCbm9lamdUcnNoTDR2TEk5T0dLam1kRV9KeDJmek5nQ0tUaXpOa3VxRUk2WkgzYnhMVTI4MGtLaUxZSFQxTFhkY0U2SXpOdUdhQTRHUC1hWjR0dG5zT0lPZXNRVHVWV1lPeElBdEQ2WUdLVHhkci12TktBY0VmWm91RXdnaUxNRTVGTV94Sl9zUGtMaVpsUkZZUXVkUnJGU1RpYmpnSS1RVjVjZExSMG5LQ3pxZTk3MkZ2ejg5NHduYXhJdmQycmlBUVYtSFAwdDZaRDk5TFhDdmxlRGVHVEhwME9WZ2g5aENPN3Z1M0JZZzc2UUZJN2ZOTWtNRnh4YmlIeUFuaWlhc1M2Vno2eHRsX3lqdGYwcFdSdHBwMi1XTTBvZ0RrUlhmRnhHYmxkN01QSnFDaHlTM25HSl81SEtsUXFZSEkxbW5OZ29uQVl2QjFETmpVZFBJMUtUR1dTQ0gxTUkyVjA2cHZzNDI5cUhWS2dFejNObms1RVBIWkRpY05mcVl6TFhqTEtpMC53alJaUlh6MWk4ZWZjTEJLNm1xQ0VBIiwid3NIb3N0TmFtZSI6IndzczovL3FpbnN0YW5jZTEtdGVuYW50MS53cy5xYTEuY2l0dGFkYXRhLmNvbSJ9"
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
    factor = 10
    data_volume = [(i * factor) for i in get_geom_series(1, 2, 1)]
    frequency = get_geom_series(1, 2, 1)

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
                # entry = {'readTime': read_time, 'writeTime': write_time}
                entry = {'readTime': read_time, 'writeTime': 0}
                if (freq, record_count) not in stats:
                    stats[(freq, record_count)] = []
                stats[(freq, record_count)].append(entry)
            print(f'The statistics are: {stats}')
    log_to_file(stats)
    # visualize_as_grouped_bar(stats)
