"""Day 18 streaming-metrics aggregation. Pure Python: no Spark, no broker -- it checks
how per-batch StreamingQueryProgress numbers roll up into one per-run row."""
from vantage.validate.stream_metrics import batch_record, kafka_lag, summarize


def test_batch_record_normalises_nan_and_none_rates():
    r = batch_record(100, float("nan"), None, 250)
    assert r == {"rows": 100, "input_rps": None, "processed_rps": None, "duration_ms": 250.0}


def test_summarize_sums_rows_and_means_finite_rates():
    # Spark reports NaN rates on the first batch (no prior trigger); they must not count.
    records = [
        batch_record(100, float("nan"), float("nan"), 200),  # first batch: rates NaN
        batch_record(300, 30.0, 150.0, 400),
        batch_record(200, 10.0, 50.0, 300),
    ]
    s = summarize(records)
    assert s["rows_in"] == 600
    assert s["batches"] == 3
    assert s["input_rows_per_sec"] == 20.0          # mean(30, 10), NaN dropped
    assert s["processed_rows_per_sec"] == 100.0      # mean(150, 50)
    assert s["batch_duration_ms"] == 300.0           # mean(200, 400, 300)


def test_summarize_ignores_empty_batches():
    # a continuous stream fires empty no-data batches between real ones
    s = summarize([batch_record(0, None, None, 10), batch_record(50, 5.0, 5.0, 20)])
    assert s["rows_in"] == 50 and s["batches"] == 1


def test_summarize_all_empty_is_zero_and_none():
    s = summarize([batch_record(0, None, None, None), batch_record(0, None, None, None)])
    assert s["rows_in"] == 0 and s["batches"] == 0
    assert s["input_rows_per_sec"] is None and s["batch_duration_ms"] is None


def test_kafka_lag_is_latest_minus_committed(monkeypatch):
    # latest (high) offsets per partition vs what Bronze committed (max landed + 1)
    import vantage.validate.stream_metrics as m
    monkeypatch.setattr(m, "kafka_watermarks", lambda b, t: {0: (0, 100), 1: (0, 50), 2: (0, 10)})
    out = kafka_lag("b", "t", bronze_committed={0: 90, 1: 50, 2: 0})
    assert out["per_partition"] == {0: 10, 1: 0, 2: 10}  # p1 fully caught up; p2 never consumed
    assert out["total"] == 20
