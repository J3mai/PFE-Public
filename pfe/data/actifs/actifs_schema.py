from pyspark.sql.types import StringType, StructField, StructType,DoubleType,DateType,LongType

# Dataset Actifs fields

ID: str = "id"
BEDS: str = 'Beds'
BATHS: str = "Baths"
SURFACE: str = "Surface"
STREET: str = "street"
CITY: str = "city"
STATENAME: str = "stateName"
ZIPCODE: str= "ZipCode"
COUNTRY: str= "country"
POSTED: str= "Posted"
MONTHLY_RENT: str= "Monthly_rent"



ACTIFS_SCHEMA: StructType = StructType(
    [
        StructField(ID, StringType()),
        StructField(BEDS, DoubleType()),
        StructField(BATHS, DoubleType()),
        StructField(SURFACE, DoubleType()),
        StructField(STREET, StringType()),
        StructField(CITY, StringType()),
        StructField(STATENAME, StringType()),
        StructField(ZIPCODE, LongType()),
        StructField(COUNTRY, StringType()),
        StructField(POSTED, DateType()),
        StructField(MONTHLY_RENT, DoubleType())
    ]
)
