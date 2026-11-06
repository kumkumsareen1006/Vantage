"""Build the local SparkSession for Vantage: Delta enabled, UTC, laptop-tuned.

One function so the pytest fixture, the parity test and any script all get an
identically configured session.
"""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from vantage import config

# PySpark picks the worker interpreter from PYSPARK_PYTHON (defaulting to whatever
# `python3` is on PATH), not from SparkConf -- so without the venv activated the
# worker is a different Python than the driver and Spark aborts on the mismatch.
# Pin both to this interpreter before any SparkContext is built.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 16 GB Mac, shared with the running edit capture and VS Code. 4g leaves headroom
# for both plus macOS while giving the local driver enough for the batch job.
DRIVER_MEMORY = "4g"
# Tiny local data; Spark's default of 200 shuffle partitions just adds scheduler overhead.
SHUFFLE_PARTITIONS = "8"
# Spark 3.5.9's Structured Streaming Kafka source. _2.12 matches the bundled
# scala-library; it pulls kafka-clients 3.4.1, which the 3.9.x broker speaks
# (docs/kafka.md). Added next to the Delta jars so one session can do both.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9"
# hadoop-azure carries the abfss:// (ABFS) filesystem + OAuth token provider. Pinned to
# 3.3.4 to match the hadoop-client jars pyspark 3.5.9 bundles (hadoop-client-*-3.3.4.jar),
# so the ABFS classes link against the same Hadoop the session already runs.
HADOOP_AZURE_PACKAGE = "org.apache.hadoop:hadoop-azure:3.3.4"
SP_FILE = Path.home() / ".config" / "vantage" / "azure_sp.json"


def _azure_oauth(builder, account, sp):
    # Client-credentials OAuth for the one storage account. The key names contain
    # "secret", so Spark's default redaction keeps the value out of the UI and logs.
    host = f"{account}.dfs.core.windows.net"
    return (builder
            .config(f"spark.hadoop.fs.azure.account.auth.type.{host}", "OAuth")
            .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{host}",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{host}", sp["client_id"])
            .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{host}", sp["client_secret"])
            .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{host}",
                    f"https://login.microsoftonline.com/{sp['tenant_id']}/oauth2/token"))


def get_spark(app_name="vantage", master="local[*]", driver_memory=DRIVER_MEMORY):
    builder = (
        SparkSession.builder.appName(app_name).master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        # bind to loopback so Spark doesn't depend on the machine's hostname/IP (changes between networks)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "localhost")
    )
    packages = [KAFKA_PACKAGE]
    if config.TARGET == "azure":
        with open(SP_FILE) as fh:
            sp = json.load(fh)
        builder = _azure_oauth(builder, config.AZURE["storage_account"], sp)
        packages.append(HADOOP_AZURE_PACKAGE)
    # configure_spark_with_delta_pip adds the delta-spark jars matching the installed pip version
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
