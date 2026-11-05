"""Run the Kafka edit producer: the Day 13 replacement for capture_edits.py.

Streams enwiki recent-changes into the recentchange topic until Ctrl-C, which
flushes, saves the resume point and logs a summary. Needs the broker up
(docker compose up -d). Run from the project root:

    caffeinate -i python scripts/produce_edits.py
"""
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vantage import config
from vantage.ingest.producer import EditProducer
from vantage.observability import RunLogger, setup_logging

STREAM_DIR = config.ROOT / "data" / "stream"


def main():
    run_id = uuid.uuid4().hex
    logger = setup_logging(config.ROOT / "logs" / "producer.jsonl", name="vantage.producer")
    rlog = RunLogger(logger, run_id)

    producer = EditProducer(
        logger=rlog,
        state_file=STREAM_DIR / "_last_event_id.txt",
        delivery_log=STREAM_DIR / "published.jsonl",
    )
    producer.run()


if __name__ == "__main__":
    main()
