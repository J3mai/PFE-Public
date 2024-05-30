import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pfe.common.reader import (
    create_df_with_schema,
    get_folder_with_max_evendate,
    read_from_parquet,
)
from pfe.config.config import AWS_Bucket
from pfe.context.context import logger
from pfe.data.state_data.state_schema import (
    PREFIX_PATH_STATE,
    STATE_SCHEMA,
)

N_APPLIC_INFQ_VALUE = 38


class StateDataReader:
    def __init__(self, path: str) -> None:
        self.bucket_name = AWS_Bucket["bucket_name"]
        self.path = path+"/"

    def read(self) -> DataFrame:
        logger.info("start reading table")
        states_df: DataFrame = read_from_parquet(
            self.bucket_name,
            PREFIX_PATH_STATE + "states.parquet"
        )
        logger.info("Done reading table")
        return create_df_with_schema(
            states_df,
            STATE_SCHEMA,
        )
