"""Build the local SparkSession for Vantage: Delta enabled, UTC, laptop-tuned.

One function so the pytest fixture, the parity test and any script all get an
identically configured session.
"""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from vantage import config


def on_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


# PySpark picks the worker interpreter from PYSPARK_PYTHON (defaulting to whatever
# `python3` is on PATH), not from SparkConf -- so without the venv activated the
# worker is a different Python than the driver and Spark aborts on the mismatch.
# Pin both to this interpreter before any SparkContext is built. Databricks sets its own.
if not on_databricks():
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
# Databricks secret scope holding the same three SP keys as SP_FILE (docs/databricks.md).
SECRET_SCOPE = "vantage"
SP_KEYS = ("client_id", "client_secret", "tenant_id")


def azure_oauth_conf(account, sp):
    # Client-credentials OAuth for the one storage account. The key names contain
    # "secret", so Spark's default redaction keeps the value out of the UI and logs.
    host = f"{account}.dfs.core.windows.net"
    return {
        f"fs.azure.account.auth.type.{host}": "OAuth",
        f"fs.azure.account.oauth.provider.type.{host}":
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{host}": sp["client_id"],
        f"fs.azure.account.oauth2.client.secret.{host}": sp["client_secret"],
        f"fs.azure.account.oauth2.client.endpoint.{host}":
            f"https://login.microsoftonline.com/{sp['tenant_id']}/oauth2/token",
    }


def _databricks_session():
    # The cluster already has a session with Delta built in; jars, master and driver
    # memory come from the cluster spec (databricks.yml), not from here.
    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    sp = {k: dbutils.secrets.get(SECRET_SCOPE, k) for k in SP_KEYS}
    for k, v in azure_oauth_conf(config.AZURE["storage_account"], sp).items():
        spark.conf.set(k, v)
    spark.conf.set("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # DBR turns deletion vectors on for new tables; keep them off so the local
    # delta-spark can keep reading and writing the same tables.
    spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")
    return spark


def release(spark):
    """Stop a local session. On Databricks the session belongs to the cluster, and
    stopping it ends the job."""
    if not on_databricks():
        spark.stop()


def get_spark(app_name="vantage", master="local[*]", driver_memory=DRIVER_MEMORY):
    if on_databricks():
        return _databricks_session()
    from delta import configure_spark_with_delta_pip

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
        for k, v in azure_oauth_conf(config.AZURE["storage_account"], sp).items():
            builder = builder.config(f"spark.hadoop.{k}", v)
        packages.append(HADOOP_AZURE_PACKAGE)
    # configure_spark_with_delta_pip adds the delta-spark jars matching the installed pip version
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
