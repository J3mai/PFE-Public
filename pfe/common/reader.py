import datetime
import re
from pfe.config.config import AWS_Bucket,credentials
from datetime import datetime as dt
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pfe.common.s3 import list_objects_names,list_folders_names
from pfe.context.context import spark
import boto3
import pandas as pd
import io 

def read_from_parquet(bucket_name: str, file_key: str) -> DataFrame:
    s3 = boto3.client('s3', aws_access_key_id=credentials['aws_access_key_id'],
                  aws_secret_access_key=credentials['aws_secret_access_key'], region_name=credentials['region_name'])
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    parquet_object = response['Body'].read()
    df = pd.read_parquet(io.BytesIO(parquet_object))
    return spark.createDataFrame(df)


def get_date_from_string(text: str) -> datetime.date:
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    assert match is not None
    return dt.strptime(match.group(), "%Y-%m-%d").date()


def get_folder_with_max_evendate(path: str, prefix: str) -> str:
    event_date_folder_list: list[str] = list_folders_names(path, prefix)

    if len(event_date_folder_list) == 1:
        folder_with_max_eventdate: str = event_date_folder_list[0]
    else:
        folder_with_max_eventdate = filter_max_eventdate(event_date_folder_list)

    return folder_with_max_eventdate

def get_parquet_file_with_max_evendate(path: str, prefix: str) -> str:
    return get_folder_with_max_evendate(path,prefix)+".parquet"


def filter_max_eventdate(eventdate_folder_list: list) -> str:
    date_dict: dict = {}

    for foldername in eventdate_folder_list:

        eventdate: datetime.date = get_date_from_string(foldername)
        date_dict[foldername] = eventdate

    eventdate_list = [value for key, value in date_dict.items()]
    newer_eventdate = max(eventdate_list)
    return list(date_dict.keys())[list(date_dict.values()).index(newer_eventdate)]


def create_df_with_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Create a dataframe and enforce the given schema."""
    return spark.createDataFrame(
        schema=schema, data=spark.sparkContext.emptyRDD()
    ).union(df)


