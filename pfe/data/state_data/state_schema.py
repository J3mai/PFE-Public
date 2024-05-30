
from typing import Tuple

from pyspark.sql.types import StringType, StructField, StructType

# rnat_ref_nat fields
STATENAME: str = "StateName"
STATECODE: str = "StateCode"


PREFIX_PATH_STATE: str = (
    "data/processed-data/states/"
)


STATE_SCHEMA: StructType = StructType(
    [
        StructField(STATENAME, StringType()),
        StructField(STATECODE, StringType()),
        
    ]
)


def build_state(
    StateName: str,
    StateCode: str,
) -> Tuple:
    return (
        StateName,
        StateCode,
    )
