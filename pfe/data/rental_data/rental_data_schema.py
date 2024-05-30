from datetime import datetime
from typing import Tuple

from pyspark.sql.types import LongType, StringType, StructField, StructType,DoubleType,DateType

# rental data columns

ID: str = "id"
BEDS: str = 'Beds'
BATHS: str = "Baths"
SURFACE: str = "Surface"
STREET: str = "street"
CITY: str = "city"
STATE: str = "state"
ZIPCODE: str= "ZipCode"
COUNTRY: str= "country"
POSTED: str= "Posted"
MONTHLY_RENT: str= "Monthly_rent"


PREFIX_PATH_RENTAL_DATA: str = (
    "data/raw-data/daily"
)


RENTAL_DATA_SCHEMA: StructType = StructType(
    [
        StructField(ID, StringType()),
        StructField(BEDS, DoubleType()),
        StructField(BATHS, DoubleType()),
        StructField(SURFACE, DoubleType()),
        StructField(STREET, StringType()),
        StructField(CITY, StringType()),
        StructField(STATE, StringType()),
        StructField(ZIPCODE, LongType()),
        StructField(COUNTRY, StringType()),
        StructField(POSTED, DateType()),
        StructField(MONTHLY_RENT, DoubleType())
    ]
)


def build_rental_data(
    id: str,
    beds: float,
    baths: float,
    surface: float,
    street: str,
    city: str,
    state: str,
    zipcode: int,
    country: str,
    posted: datetime,
    monthly_rent: float,
) -> Tuple:
    return (
        id,
        beds,
        baths,
        surface,
        street,
        city,
        state,
        zipcode,
        country,
        posted,
        monthly_rent,
    )
