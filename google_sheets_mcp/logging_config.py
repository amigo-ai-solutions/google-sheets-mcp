"""Structured JSON logging for Cloud Run / Cloud Logging."""

from __future__ import annotations

import json
import logging
import logging.config
import traceback
from datetime import datetime, timezone


class StructuredFormatter(logging.Formatter):
    """Emit one JSON object per log line, compatible with Google Cloud Logging."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "logger": record.name,
        }
        if record.exc_info and record.exc_info[1] is not None:
            entry["exception"] = "".join(traceback.format_exception(*record.exc_info))
        return json.dumps(entry, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Set up structured JSON logging for the entire application."""
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "json": {"()": StructuredFormatter},
            },
            "handlers": {
                "default": {
                    "class": "logging.StreamHandler",
                    "formatter": "json",
                    "stream": "ext://sys.stdout",
                },
            },
            "root": {
                "level": level.upper(),
                "handlers": ["default"],
            },
        }
    )
