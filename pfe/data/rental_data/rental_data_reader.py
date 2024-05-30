
from datetime import datetime
from pfe.context.context import spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pfe.config.config import AWS_Bucket
from pfe.common.reader import (
    get_folder_with_max_evendate,
    get_parquet_file_with_max_evendate,
    read_from_parquet,
)
from pfe.context.context import logger
from pfe.data.rental_data.rental_data_schema import (
    RENTAL_DATA_SCHEMA,

    PREFIX_PATH_RENTAL_DATA,
)

N_APPLIC_INFQ_VALUE = 38


class RentalDataReader:
    def __init__(self, path: str) -> None:
        self.bucket_name = AWS_Bucket["bucket_name"]
        self .path = path
    
    def read(self) -> DataFrame:
        logger.info("start reading table")
        date: str = datetime.now().strftime("%Y-%m-%d")
        rental_data_df: DataFrame = read_from_parquet(
            self.bucket_name,
            PREFIX_PATH_RENTAL_DATA + "/"+ date + "/" + date + '.parquet'
            # + get_folder_with_max_evendate(self.path, PREFIX_PATH_RENTAL_DATA)+"/" 
            # + get_parquet_file_with_max_evendate(self.path, PREFIX_PATH_RENTAL_DATA)
        )#.select(*RENTAL_DATA_SCHEMA.fieldNames())
        logger.info("Done reading table")
        return rental_data_df
