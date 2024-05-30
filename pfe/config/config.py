import os

credentials = {
    "aws_access_key_id": os.environ["aws_access_key_id"],
    "aws_secret_access_key": os.environ["aws_secret_access_key"],
    "region_name":"eu-west-3"
}

AWS_Bucket = {
    'bucket_name': 'talan-pfe-etl-data-lake',
    'output_path':"data/output-data",
    "url": "s3://talan-pfe-etl-data-lake/",
    "daily_data_path": "data/raw-data/daily",
    "states_data_path":"data/processed-data/states",
    "processed_data_path": "data/processed-data/"
}

