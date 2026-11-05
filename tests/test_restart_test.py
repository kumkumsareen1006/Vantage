from vantage.validate.restart import evaluate


def test_clean_with_upstream_dup_and_kill_race():
    # partition 0, offsets 0..5 contiguous. offset 3 replays id_c (upstream dup);
    # offset 5 is the hard-kill race: in Bronze, never logged.
    stats = {0: {"min_offset": 0, "max_offset": 5, "rows": 6, "distinct_offsets": 6}}
    bronze_offsets = {(0, o) for o in range(6)}
    bronze_ids = {"a", "b", "c", "d", "e"}          # 6 clean rows, 5 distinct (c twice)
    silver_ids = {"a", "b", "c", "d", "e"}
    delivery = [{"meta_id": m, "partition": 0, "offset": o} for m, o in
                [("a", 0), ("b", 1), ("c", 2), ("c", 3), ("d", 4)]]  # offset 5 not logged

    r = evaluate(bronze_stats=stats, bronze_offsets=bronze_offsets, bronze_total_rows=6,
                 bronze_malformed=0, bronze_ids=bronze_ids, silver_ids=silver_ids,
                 silver_rows=5, delivery_rows=delivery)

    assert r["check1_pass"] and r["check2_pass"] and r["check3_pass"] and r["all_pass"]
    assert r["lost_ids"] == [] and r["missing_from_bronze"] == []
    assert r["extra_ids"] == ["e"]                   # in Silver, not logged (the race)
    assert r["bronze_extra_offsets"] == [(0, 5)]
    assert r["log_upstream_dups"] == 1               # c logged twice
    assert r["bronze_upstream_dups"] == 1            # c in Bronze twice
    assert r["silver_removed"] == 1                  # Silver collapsed it


def test_gap_and_loss_fail():
    # Bronze missing offset 3 -> a gap; the producer logged offset 3 -> real loss
    stats = {0: {"min_offset": 0, "max_offset": 5, "rows": 5, "distinct_offsets": 5}}
    bronze_offsets = {(0, 0), (0, 1), (0, 2), (0, 4), (0, 5)}
    delivery = [{"meta_id": m, "partition": 0, "offset": o} for m, o in
                [("a", 0), ("b", 1), ("c", 2), ("x", 3), ("d", 4), ("e", 5)]]

    r = evaluate(bronze_stats=stats, bronze_offsets=bronze_offsets, bronze_total_rows=5,
                 bronze_malformed=0, bronze_ids={"a", "b", "c", "d", "e"},
                 silver_ids={"a", "b", "c", "d", "e"}, silver_rows=5, delivery_rows=delivery)

    assert not r["check1_pass"]                      # gap at offset 3
    assert not r["check2_pass"] and r["lost_ids"] == ["x"]
    assert r["missing_from_bronze"] == [(0, 3)]
    assert not r["all_pass"]


def test_silver_duplicate_fails_check3():
    stats = {0: {"min_offset": 0, "max_offset": 2, "rows": 3, "distinct_offsets": 3}}
    bronze_offsets = {(0, 0), (0, 1), (0, 2)}
    delivery = [{"meta_id": m, "partition": 0, "offset": o} for m, o in
                [("a", 0), ("b", 1), ("c", 2)]]

    r = evaluate(bronze_stats=stats, bronze_offsets=bronze_offsets, bronze_total_rows=3,
                 bronze_malformed=0, bronze_ids={"a", "b", "c"},
                 silver_ids={"a", "b", "c"}, silver_rows=4,  # 4 rows but 3 distinct -> dup
                 delivery_rows=delivery)

    assert r["check1_pass"] and r["check2_pass"]
    assert not r["check3_pass"] and not r["all_pass"]


def test_out_of_range_log_rows_ignored():
    # the producer is ahead of Bronze: offsets past bmax are out of range, not loss
    stats = {0: {"min_offset": 0, "max_offset": 1, "rows": 2, "distinct_offsets": 2}}
    bronze_offsets = {(0, 0), (0, 1)}
    delivery = [{"meta_id": m, "partition": 0, "offset": o} for m, o in
                [("a", 0), ("b", 1), ("c", 2), ("d", 3)]]  # 2,3 are beyond Bronze

    r = evaluate(bronze_stats=stats, bronze_offsets=bronze_offsets, bronze_total_rows=2,
                 bronze_malformed=0, bronze_ids={"a", "b"}, silver_ids={"a", "b"},
                 silver_rows=2, delivery_rows=delivery)

    assert r["log_rows_in_range"] == 2 and r["all_pass"]
    assert r["lost_ids"] == [] and r["bronze_extra_offsets"] == []
