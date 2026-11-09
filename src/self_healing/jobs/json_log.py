"""JSON-lines logging for job output, so log-parser reads fields instead of free text."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    """Render a record as one JSON object. Pass data with extra={"fields": {...}}."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "job": record.name.rsplit(".", 1)[-1],
            "event": record.getMessage(),
            **getattr(record, "fields", {}),
        }
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    """Return a logger that writes JSON lines to stdout, which get_run_output captures."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
