import gzip

from vantage.ingest.bronze import land_files


def write_gz(path, lines):
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def test_only_en_lines_land_with_right_provenance(spark, tmp_path):
    gz = tmp_path / "pageviews-20261015-010000.gz"  # ends 01:00 -> covers hour starting 00:00
    write_gz(gz, [
        "en Foo 10 0",    # keep
        "en.m Bar 5 0",   # keep
        "de Quux 9 0",    # drop: out of scope
        "fr.m X 1 0",     # drop
        "ena Y 1 0",      # drop: starts 'ena', not 'en '
        "en.mx Z 1 0",    # drop: starts 'en.mx', not 'en.m '
    ])
    bronze = str(tmp_path / "bronze")
    landed, skipped = land_files(spark, [gz], bronze)
    assert len(landed) == 1 and skipped == 0

    df = spark.read.format("delta").load(bronze)
    assert df.count() == 2
    assert {r["value"] for r in df.collect()} == {"en Foo 10 0", "en.m Bar 5 0"}

    prov = df.selectExpr(
        "source_file",
        "date_format(file_hour,'yyyy-MM-dd HH:mm:ss') as fh",
        "date_format(file_date,'yyyy-MM-dd') as fd",
    ).distinct().collect()
    assert len(prov) == 1
    assert prov[0]["source_file"] == "pageviews-20261015-010000.gz"
    assert prov[0]["fh"] == "2026-10-15 00:00:00"  # start of hour, not the file's 01:00 end
    assert prov[0]["fd"] == "2026-10-15"


def test_rerun_lands_nothing(spark, tmp_path):
    gz = tmp_path / "pageviews-20261015-010000.gz"
    write_gz(gz, ["en Foo 10 0", "en.m Bar 5 0"])
    bronze = str(tmp_path / "bronze")
    land_files(spark, [gz], bronze)
    before = spark.read.format("delta").load(bronze).count()

    landed, skipped = land_files(spark, [gz], bronze)  # same file again
    after = spark.read.format("delta").load(bronze).count()
    assert landed == [] and skipped == 1
    assert after == before


def test_already_landed_file_skipped_new_one_lands(spark, tmp_path):
    a = tmp_path / "pageviews-20261015-010000.gz"  # hour 00:00
    b = tmp_path / "pageviews-20261015-020000.gz"  # hour 01:00
    write_gz(a, ["en A 1 0"])
    write_gz(b, ["en B 2 0"])
    bronze = str(tmp_path / "bronze")
    land_files(spark, [a], bronze)

    landed, skipped = land_files(spark, [a, b], bronze)  # A skipped, B lands
    assert skipped == 1
    assert [n for n, _ in landed] == ["pageviews-20261015-020000.gz"]
    assert {r["value"] for r in spark.read.format("delta").load(bronze).collect()} == {"en A 1 0", "en B 2 0"}
