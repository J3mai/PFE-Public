from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pfe.common.s3 import write_to_parquet
from pfe.config.config import AWS_Bucket,credentials

from pfe.data.actifs.actifs_schema import ACTIFS_SCHEMA
from pfe.data.rental_data.rental_data_reader import (
    RentalDataReader,
)
from pfe.data.rental_data.rental_data_schema import STATE
from pfe.process.transformation import Transformation
from pfe.data.state_data.state_reader import StateDataReader
from pfe.data.state_data.state_schema import STATECODE



class ActifsJob:
    def __init__(
        self,
        rental_data_input_path: str,
        state_data_input_path: str,
        actifs_output_path: str,
    ) -> None:
        self.bucket_name : str =  AWS_Bucket["bucket_name"]
        self.rental_data_input_path: str = rental_data_input_path
        self.state_data_input_path: str = state_data_input_path
        self.actifs_output_path: str = actifs_output_path

    def run(self) -> None:
        rental_data_df: DataFrame = self._transform_data()
            
        state_data_df: DataFrame = self._get_data_from_state_data(
            self.state_data_input_path
        )

        dataset_actifs: DataFrame = self._create_dataset_actifs(
            rental_data_df,
            state_data_df,
        )
        self._write_dataset_actifs_to_s3(dataset_actifs,self.bucket_name ,self.actifs_output_path)

    def _transform_data(self) -> DataFrame:
        rental_data_df: DataFrame = self._get_data_from_rental_data(
            self.rental_data_input_path
        )
        transformer = Transformation()
        data = transformer.Transformer(rental_data_df)
        return data

    def _get_data_from_rental_data(self, path: str) -> DataFrame:
        rental_data_reader: RentalDataReader = RentalDataReader(path)
        rental_data = rental_data_reader.read()
        return rental_data

    def _get_data_from_state_data(self, path: str) -> DataFrame:
        state_data_reader: StateDataReader = StateDataReader(path)
        state_data = state_data_reader.read()
        return state_data

    
    
    def _create_dataset_actifs(
        self,
        rental_data_df: DataFrame,
        state_data_df: DataFrame,
    ) -> DataFrame:

        try:
            COLS = ACTIFS_SCHEMA.fieldNames()
            COLS.remove("state")
        except:
            pass
        actifs_df = (
            (
                state_data_df.join(
                    rental_data_df, rental_data_df[STATE] == state_data_df[STATECODE]
                )
            )
            .distinct()
            .select(*COLS)
        )
        return actifs_df

    def _write_dataset_actifs_to_s3(self, df: DataFrame, bucket_name: str, file_key: str) -> None:
        write_to_parquet(df, bucket_name, file_key)


def run_job(**kwargs: Any) -> None:
    print(f"Running Job with arguments[{kwargs}]")

    bucket_name_datalake = AWS_Bucket["bucket_name"]
    bucket_name_results = AWS_Bucket["output_path"]

    file_path_rental_data = AWS_Bucket["daily_data_path"]
    file_path_state_data = AWS_Bucket["states_data_path"]

    output_file_path_actifs = AWS_Bucket["output_path"]

    date: str = datetime.now().strftime("%Y%m%d")
    rental_data_path: str = (
        f"s3://{bucket_name_datalake}/{file_path_rental_data}"
    )
    state_data_path: str = (
        f"s3://{bucket_name_datalake}/{file_path_state_data}"
    )
    
    actifs_output_path: str = (
        f"{output_file_path_actifs}/rental.parquet"
    )

    job: ActifsJob = ActifsJob(
        rental_data_path,
        state_data_path,
        actifs_output_path,
    )
    job.run()

run_job()