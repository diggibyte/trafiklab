"""Structured logging configuration for HTQ2 GTFS pipeline.

Provides a consistent logger across all modules with structured
JSON output suitable for Databricks log analytics.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class StructuredFormatter(logging.Formatter):
    """JSON-formatted log output for machine-readable log analysis."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        # Include extra fields if passed via logger.info("msg", extra={"key": val})
        for key in ("run_id", "task", "table", "file_count", "duration_seconds",
                     "row_count", "status", "error"):
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)

        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Get a configured logger for HTQ2 pipeline modules.

    Args:
        name: Logger name (typically __name__ from calling module).
        level: Logging level (default: INFO).

    Returns:
        Configured logging.Logger instance.

    Example:
        >>> from htq2_gtfs.helpers.logging_config import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing started", extra={"task": "transform", "file_count": 3})
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False

    return logger
