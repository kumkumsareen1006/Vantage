"""Structured JSON logging for the pipeline: one JSON object per line, to
logs/pipeline.jsonl and to stderr.

Every line carries the run_id, so a whole run can be grepped out of the file.
RunLogger is the only writer -- it stamps run_id onto each record -- and the
named logger does not propagate, so nothing else can land a non-JSON line in
the file.
"""
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path


def _utcnow_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class JsonFormatter(logging.Formatter):
    def format(self, record):
        data = getattr(record, "data", None)
        if data is None:                       # a record not routed through RunLogger
            data = {"event": record.getMessage()}
        return json.dumps({"ts": _utcnow_iso(), "level": record.levelname, **data}, default=str)


def setup_logging(logfile, name="vantage.pipeline", level=logging.INFO):
    logfile = Path(logfile)
    logfile.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    # replace handlers so a second setup (e.g. the next test) doesn't double-log
    for h in list(logger.handlers):
        logger.removeHandler(h)
        h.close()
    fmt = JsonFormatter()
    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


class RunLogger:
    """Wraps a logger and stamps run_id onto every line. None-valued fields are
    dropped so a line only carries the metrics it actually has."""

    def __init__(self, logger, run_id):
        self._log = logger
        self.run_id = run_id

    def _emit(self, level, event, fields):
        data = {"run_id": self.run_id, "event": event}
        data.update({k: v for k, v in fields.items() if v is not None})
        self._log.log(level, event, extra={"data": data})

    def event(self, event, **fields):
        self._emit(logging.INFO, event, fields)

    def stage(self, stage, status, **fields):
        level = logging.ERROR if status == "failed" else logging.INFO
        self._emit(level, f"stage_{status}", {"stage": stage, "status": status, **fields})
