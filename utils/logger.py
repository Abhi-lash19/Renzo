"""
Structured JSON logging — production-grade, zero external deps.

Log format:
{
  "timestamp": "...",
  "level": "INFO",
  "component": "PIPELINE",
  "event": "jobs_processed",
  "message": "...",
  "meta": {...}
}

Usage:
  from utils.logger import get_logger
  logger = get_logger(__name__)
  logger.info("msg", extra={"component": "DB", "event": "insert", "meta": {"count": 5}})
  logger.info("plain message")  # still works — component defaults to module name
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

LOG_DIR = "logs"
LOG_FILE = "app.log"
JSON_LOG_FILE = "app.json.log"
DEFAULT_LEVEL = "INFO"

os.makedirs(LOG_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# JSON Formatter
# ---------------------------------------------------------------------------

class StructuredJsonFormatter(logging.Formatter):
    """Produces one JSON object per log line."""

    def format(self, record: logging.LogRecord) -> str:
        entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "component": getattr(record, "component", record.name),
            "event": getattr(record, "event", "log"),
            "message": record.getMessage(),
        }

        # Attach structured meta if provided
        meta = getattr(record, "meta", None)
        if meta and isinstance(meta, dict):
            entry["meta"] = meta

        # Attach exception info when present
        if record.exc_info and record.exc_info[1]:
            entry["exception"] = self.formatException(record.exc_info)

        try:
            return json.dumps(entry, default=str, ensure_ascii=False)
        except Exception:
            return json.dumps({"level": "ERROR", "message": "log_serialization_failed"})


# ---------------------------------------------------------------------------
# Human-readable formatter (console / legacy file)
# ---------------------------------------------------------------------------

class ReadableFormatter(logging.Formatter):
    """Traditional pipe-delimited format, same as before."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s | %(levelname)s | %(name)s | %(message)s")


# ---------------------------------------------------------------------------
# Safe console handler (preserves existing Unicode crash protection)
# ---------------------------------------------------------------------------

class SafeConsoleHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            super().emit(record)
        except UnicodeEncodeError:
            try:
                # Fallback: strip non-ascii characters to avoid crash
                record.msg = str(record.msg).encode('ascii', 'ignore').decode('ascii')
                super().emit(record)
            except Exception:
                pass
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Level resolution
# ---------------------------------------------------------------------------

def _get_console_level() -> int:
    level_name = os.getenv("LOG_LEVEL", DEFAULT_LEVEL).upper()
    if level_name not in logging._nameToLevel:
        level_name = DEFAULT_LEVEL
    return logging._nameToLevel[level_name]


# ---------------------------------------------------------------------------
# Public API — fully backward compatible
# ---------------------------------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        # 1. File handler — human-readable (existing format preserved)
        file_handler = logging.FileHandler(f"{LOG_DIR}/{LOG_FILE}", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(ReadableFormatter())

        # 2. JSON file handler — structured logs for observability
        json_handler = logging.FileHandler(f"{LOG_DIR}/{JSON_LOG_FILE}", encoding="utf-8")
        json_handler.setLevel(logging.DEBUG)
        json_handler.setFormatter(StructuredJsonFormatter())

        # 3. Console handler — readable, respects LOG_LEVEL env
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass

        console_handler = SafeConsoleHandler(stream=sys.stdout)
        console_handler.setLevel(_get_console_level())
        console_handler.setFormatter(ReadableFormatter())

        logger.addHandler(file_handler)
        logger.addHandler(json_handler)
        logger.addHandler(console_handler)

    return logger