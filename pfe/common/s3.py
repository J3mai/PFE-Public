from typing import List, Set
from pfe.config.config import credentials
import boto3
from pyspark.sql import DataFrame
import pandas as pd
import io


def list_objects_names(folder_key: str, prefix: str) -> List[str]:
    """
    Display a list of object in the path provided.

    Parameters:
    --------
    folder_key (string): the path of the bucket.
        => example : s3://bucket-name/
    prefix (string): the name of the folder to explore.
        => example : data/

    Returns:
    --------
    list : the names of objects in the provided path.
    """

    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        region_name=credentials["region_name"])
    bucket_name = folder_key.split("/")[2]

    buckets: set = set()

    element: int = prefix.count('/')

    for s3_object in s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)["Contents"]:
        array_split = s3_object["Key"].split("/")
        if len(array_split) >= element:
            if "parquet" in array_split[element]:
                buckets.add(array_split[element])


    return list(buckets)


def list_folders_names(folder_key: str, prefix: str) -> List[str]:
    """
    Display a list of folder names in the path provided.

    Parameters:
    --------
    folder_key (string): the path of the bucket.
        => example : s3://bucket-name/
    prefix (string): the name of the folder to explore.
        => example : data/

    Returns:
    --------
    list : the names of folders in the provided path.
    """

    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        region_name=credentials["region_name"])
    bucket_name = folder_key.split("/")[2]

    folders: set = set()

    element: int = prefix.count('/')

    for s3_object in s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)["Contents"]:
        array_split = s3_object["Key"].split("/")
        if len(array_split) >= element + 1:
            if not array_split[element] == '' and  '.' not in array_split[element]:
                folders.add(array_split[element])
            else:
                continue

    return list(folders)

def write_to_parquet(df: DataFrame, bucket_name: str , file_key : str) -> str:
    """
    Display a list of object in the path provided.

    Parameters:
    --------
    df (DataFrame): the dataframe to be saved.

    output_file_path (string): the path of saving.

    Returns:
    --------
    str : the path of saving.
    """
    df = df.toPandas()
    s3 = boto3.client('s3', aws_access_key_id=credentials['aws_access_key_id'],
                  aws_secret_access_key=credentials['aws_secret_access_key'], region_name=credentials['region_name'])
    session = boto3.session.Session(credentials['aws_access_key_id'],credentials['aws_secret_access_key'],region_name=credentials['region_name'])
    try:
        # Read the existing append-only file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        parquet_object = response['Body'].read()
        existing_data = pd.read_parquet(io.BytesIO(parquet_object))
        # existing_data = wr.s3.read_parquet(output_file_path,boto3_session=session)
        df = pd.concat([existing_data, df], ignore_index=True)
        df = df.dropna()
        df = df.drop_duplicates()
    except :
        pass

    # Convert DataFrame to Parquet file in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    # Write CSV data to S3 bucket (append-only file)
    s3.put_object(Body=parquet_buffer.getvalue(), Bucket=bucket_name, Key=file_key)
    
    return bucket_name+file_key
