"""The three restart-test checks, as pure functions over already-collected facts.

Kept free of Spark/Kafka so the logic is unit-testable: scripts/restart_test.py
gathers the counts and sets from Bronze, Silver and the producer's delivery log,
then calls evaluate() to decide pass/fail and tally the duplicates.

The delivery log is compared only up to the highest offset Bronze has per partition
(Bronze can trail the still-running producer). Within that range:
  Check 1  Bronze has every (partition, offset) once, contiguous from 0, no gaps.
  Check 2  every meta_id the producer logged is in Silver (nothing lost).
  Check 3  Silver meta_ids are unique.

Bronze offsets the log doesn't have are the hard-kill race: the producer delivered a
message to Kafka but was killed -9 before its delivery callback wrote the log line.
They are reported separately and are NOT loss -- the event is in Bronze, just unlogged.
"""


def evaluate(*, bronze_stats, bronze_offsets, bronze_total_rows, bronze_malformed,
             bronze_ids, silver_ids, silver_rows, delivery_rows):
    """
    bronze_stats:   {partition: {min_offset, max_offset, rows, distinct_offsets}}
    bronze_offsets: set of (partition, offset) in Bronze
    bronze_total_rows: all Bronze rows; bronze_malformed: those that fail parsing
    bronze_ids:     set of distinct clean meta_ids in Bronze
    silver_ids:     set of distinct meta_ids in Silver
    silver_rows:    total rows in Silver (to check uniqueness)
    delivery_rows:  list of {meta_id, partition, offset} from published.jsonl
    """
    bmax = {p: s["max_offset"] for p, s in bronze_stats.items()}
    bronze_clean_rows = bronze_total_rows - bronze_malformed

    # Check 1: per partition, from 0, contiguous, each offset exactly once
    check1 = {}
    for p, s in sorted(bronze_stats.items()):
        span = s["max_offset"] - s["min_offset"] + 1
        check1[p] = {"min": s["min_offset"], "max": s["max_offset"], "rows": s["rows"],
                     "from_zero": s["min_offset"] == 0,
                     "contiguous": s["rows"] == span,
                     "exactly_once": s["rows"] == s["distinct_offsets"]}
    check1_pass = all(c["from_zero"] and c["contiguous"] and c["exactly_once"]
                      for c in check1.values())

    # delivery log restricted to Bronze's consumed range (offset <= bmax[partition])
    in_range = [r for r in delivery_rows
                if r["partition"] in bmax and r["offset"] <= bmax[r["partition"]]]
    log_offsets = {(r["partition"], r["offset"]) for r in in_range}
    log_ids = {r["meta_id"] for r in in_range}

    bronze_extra_offsets = sorted(bronze_offsets - log_offsets)   # in Bronze, not logged (race)
    missing_from_bronze = sorted(log_offsets - bronze_offsets)    # logged, not in Bronze (loss)

    # Check 2: nothing the producer logged is missing from Silver
    lost_ids = sorted(log_ids - silver_ids)
    extra_ids = sorted(silver_ids - log_ids)                     # in Silver, not logged (race)
    check2_pass = len(lost_ids) == 0 and len(missing_from_bronze) == 0

    # Check 3: Silver meta_ids unique
    check3_pass = silver_rows == len(silver_ids)

    return {
        "check1": check1, "check1_pass": check1_pass,
        "check2_pass": check2_pass, "lost_ids": lost_ids, "extra_ids": extra_ids,
        "check3_pass": check3_pass,
        "all_pass": check1_pass and check2_pass and check3_pass,
        # counts
        "log_rows_in_range": len(in_range), "log_distinct_ids": len(log_ids),
        "bronze_total_rows": bronze_total_rows, "bronze_clean_rows": bronze_clean_rows,
        "bronze_malformed": bronze_malformed, "bronze_distinct_ids": len(bronze_ids),
        "silver_rows": silver_rows, "silver_distinct_ids": len(silver_ids),
        # upstream duplicates and what Silver removed
        "log_upstream_dups": len(in_range) - len(log_ids),
        "bronze_upstream_dups": bronze_clean_rows - len(bronze_ids),
        "silver_removed": bronze_clean_rows - silver_rows,
        # the hard-kill race, reported not counted as loss
        "bronze_extra_offsets": bronze_extra_offsets,
        "missing_from_bronze": missing_from_bronze,
    }
