import sys
from pfe.config.config import credentials
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pyspark

try:
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import SparkConf
    from pyspark.context import SparkContext

    # @params: [JOB_NAME, "ENTRYPOINT", "ENV"]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENTRYPOINT"])


    ## Access Datalake objects using PySpark
    # conf spark
    conf = (
        SparkConf()
        .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
        )
        .set("spark.hadoop.fs.s3a.access.key", credentials["aws_access_key_id"])
        .set("spark.hadoop.fs.s3a.secret.key", credentials["aws_secret_access_key"])
        .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    )
    sc = SparkContext(conf=conf)
    glueContext = GlueContext(sc)
    logger = glueContext.get_logger()
    spark = glueContext.spark_session.builder.getOrCreate()

    job = Job(glueContext)

except ImportError:
    import logging

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    logging.basicConfig(
        format="%(asctime)s\t%(module)s\t%(levelname)s\t%(message)s", level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    logger.warning("Package awsglue not found! Excepted if you run the code locally")

