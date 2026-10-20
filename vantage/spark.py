"""Build the local SparkSession for Vantage: Delta enabled, UTC, laptop-tuned.

One function so the pytest fixture, the parity test and any script all get an
identically configured session.
"""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# 16 GB Mac, shared with the running edit capture and VS Code. 4g leaves headroom
# for both plus macOS while giving the local driver enough for the batch job.
DRIVER_MEMORY = "4g"
# Tiny local data; Spark's default of 200 shuffle partitions just adds scheduler overhead.
SHUFFLE_PARTITIONS = "8"


def get_spark(app_name="vantage", master="local[*]", driver_memory=DRIVER_MEMORY):
    builder = (
        SparkSession.builder.appName(app_name).master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
    )
    # configure_spark_with_delta_pip adds the delta-spark jars matching the installed pip version
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
