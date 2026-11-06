"""ADLS round-trip, run as a subprocess by test_azure.py so the abfss jars load
in a fresh JVM. Writes a tiny Delta table to the lake container and reads it back,
then deletes it. Prints ROUNDTRIP_OK on success."""
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vantage import config
from vantage.spark import get_spark

config.set_target("azure")
spark = get_spark(app_name="vantage-azure-selftest")
try:
    path = config.lake_join(config._azure_uri("_selftest"), uuid.uuid4().hex)
    spark.range(5).write.format("delta").mode("overwrite").save(path)
    n = spark.read.format("delta").load(path).count()
    assert n == 5, n
    # clean up via the Hadoop FS so no test data lingers in the container
    sc = spark.sparkContext
    jpath = sc._jvm.org.apache.hadoop.fs.Path(path)
    jpath.getFileSystem(sc._jsc.hadoopConfiguration()).delete(jpath, True)
    print("ROUNDTRIP_OK")
finally:
    spark.stop()
