"""Structured JSON logging.

Why not print()? Because at 3am you need to filter thousands of lines by
run_id and source_file. `print` gives you prose you have to read with your
eyes. JSON gives you records a machine can filter.

Every line carries run_id, so one pipeline run can be traced end to end.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from contextvars import ContextVar
from datetime import datetime, timezone

# ContextVar: a value scoped to the current execution context. Set it once at
# the start of a run and every log line picks it up without being passed it.
_run_id: ContextVar[str] = ContextVar("run_id", default="-")

# Attributes the stdlib logging module puts on every record. Anything NOT in
# this set was added by us via `extra=` and should appear in the JSON output.
_STANDARD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
    "message",
    "asctime",
}


def set_run_id(run_id: str) -> None:
    """Call once at the start of every pipeline run."""
    _run_id.set(run_id)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            "run_id": _run_id.get(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    """Return a logger that emits one JSON object per line to stdout."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
    return logger
