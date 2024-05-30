from pyspark.sql.functions import *
from pfe.context.context import spark
from pfe.data.rental_data.rental_data_schema import RENTAL_DATA_SCHEMA




class Transformation:
    def __init__(self) -> None:
        pass
    
    def _transform_rent(self, df: DataFrame) -> DataFrame:
        c = split("Monthly rent", "–")
        min = df.withColumn("min_rent", c.getItem(0))
        max = min.withColumn("max_rent", when(size(c) > 1, c.getItem(1)).otherwise(c.getItem(0)))
        max = max.withColumn("min_rent", col("min_rent").cast("string"))
        max = max.withColumn("min_rent", col("min_rent").substr(2, 10000))
        max = max.withColumn("min_rent", col("min_rent").cast("double"))
        max = max.withColumn("max_rent", col("max_rent").cast("string"))
        max = max.withColumn("max_rent", col("max_rent").substr(2, 10000))
        max = max.withColumn("max_rent", col("max_rent").cast("double"))
        df = max.withColumn("Monthly rent", (col("min_rent") + col("max_rent")) / 2)
        df = df.drop("min_rent", "max_rent")
        return df 
        
    def _transform_beds(self, df: DataFrame) -> DataFrame:
        b = split("Beds", "–")
        bed = df.withColumn("min_Beds", b.getItem(0))
        bed = bed.withColumn("max_Beds", when(size(b) > 1, b.getItem(1)).otherwise(b.getItem(0)))
        bed = bed.withColumn("min_Beds", col("min_Beds").cast("double"))
        bed = bed.withColumn("max_Beds", col("max_Beds").cast("double"))
        df = bed.withColumn("Beds", (col("min_Beds") + col("max_Beds")) / 2)
        df = df.drop("min_Beds", "max_Beds")
        return df
    
    def _transform_baths(self, df: DataFrame) -> DataFrame:
        b = split("Baths", "–")
        bath = df.withColumn("min_Baths", b.getItem(0))
        bath = bath.withColumn("max_Baths", when(size(b) > 1, b.getItem(1)).otherwise(b.getItem(0)))
        bath = bath.withColumn("min_Baths", col("min_Baths").cast("double"))
        bath = bath.withColumn("max_Baths", col("max_Baths").cast("double"))
        df = bath.withColumn("Baths", (col("min_Baths") + col("max_Baths")) / 2)
        df = df.drop("min_Baths", "max_Baths")
        return df
    
    def _transform_surface(self, df: DataFrame) -> DataFrame:
        b = split("Surface", "–")
        Surface = df.withColumn("min_Surface", b.getItem(0))
        Surface = Surface.withColumn("max_Surface", when(size(b) > 1, b.getItem(1)).otherwise(b.getItem(0)))
        Surface = Surface.withColumn("min_Surface", col("min_Surface").cast("double"))
        Surface = Surface.withColumn("max_Surface", col("max_Surface").cast("double"))
        df = Surface.withColumn("Surface",
                                ceil(((col("min_Surface") + col("max_Surface")) / 2) * 0.092903))  # transforme sqft to m²
        df = df.drop("min_Surface", "max_Surface")
        return df
    
    def _transform_address(self, df: DataFrame) -> DataFrame:
        a = split("address", ", ")
        street = df.withColumn("street", trim(a.getItem(0)))
        city = street.withColumn("city", trim(a.getItem(1)))
        state = city.withColumn("state", trim(a.getItem(2).substr(0, 3)))
        zipCode = state.withColumn("ZipCode", trim(a.getItem(2).substr(4, 7)))
        country = zipCode.withColumn("country", trim(a.getItem(3)))
        df = country.drop("address")
        return df
    
    def _transform_date(self, df: DataFrame) -> DataFrame:
        a = split("Posted", " ")
        df = df.withColumn("Posted", a.getItem(0))
        return df
    
    def _fix_null(self, df: DataFrame) -> DataFrame:
        median_value = df.approxQuantile("Surface", [0.5], 0.001)[0]
        df = df.fillna({"Surface": median_value})
        median_value = df.approxQuantile("Baths", [0.5], 0.001)[0]
        df = df.fillna({"Baths": median_value})
        df = df.na.drop()
        return df
    
    def _add_id(self, df: DataFrame) -> DataFrame:
        concatenated = concat(
            *[df["Beds"], df["Baths"], df["Surface"], df["street"], df["city"], df["state"], df["ZipCode"], df["country"],
            df["Monthly rent"]])
        df = df.withColumn("id", sha1(concatenated).cast("string"))
        return df
    
    def _transform_data(self, df: DataFrame) -> DataFrame:
        df = df.withColumn('Beds', regexp_replace('Beds', 'Studios', '0'))
        df = df.withColumn('Beds', regexp_replace('Beds', 'Studio', '0'))
        data = self._transform_rent(df)
        data = self._transform_beds(data)
        data = self._transform_baths(data)
        data = self._transform_surface(data)
        data = self._transform_address(data)
        data = self._transform_date(data)
        data = self._fix_null(data)
        data = self._add_id(data)
        new_order = ["id", "Beds", "Baths", "Surface", "street", "city", "state", "ZipCode", "country", "Posted",
                    "Monthly rent"]
        data = data.select(*new_order)
        data = data.withColumnRenamed("Monthly rent", "Monthly_rent")
        data = data.distinct()
        data = data.withColumn("Posted", to_date(data["Posted"], "yyyy-MM-dd"))
        return data
    
    def Transformer(self,df: DataFrame) -> DataFrame:
        df = self._transform_data(df)
        # Apply the new schema using withColumn and cast for each column
        data = df
        for field in RENTAL_DATA_SCHEMA.fields:
            data = data.withColumn(field.name, df[field.name].cast(field.dataType))
        return data
