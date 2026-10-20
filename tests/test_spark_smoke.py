from delta.tables import DeltaTable
from pyspark.sql import Row


def test_delta_write_read_merge_timetravel(spark, tmp_path):
    path = str(tmp_path / "tbl")

    # version 0: two rows
    spark.createDataFrame([Row(id=1, v="a"), Row(id=2, v="b")]).write.format("delta").save(path)

    back = spark.read.format("delta").load(path)
    assert back.count() == 2
    assert {r["id"] for r in back.collect()} == {1, 2}

    # version 1: MERGE updates id=2 and inserts id=3
    updates = spark.createDataFrame([Row(id=2, v="B"), Row(id=3, v="c")])
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(updates.alias("u"), "t.id = u.id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

    after = {r["id"]: r["v"] for r in spark.read.format("delta").load(path).collect()}
    assert after == {1: "a", 2: "B", 3: "c"}

    # time travel back to version 0
    v0 = {r["id"]: r["v"] for r in
          spark.read.format("delta").option("versionAsOf", 0).load(path).collect()}
    assert v0 == {1: "a", 2: "b"}
