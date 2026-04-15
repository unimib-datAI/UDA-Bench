"""Simple run logger with uniform console + file events."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunLogger:
    def __init__(self, events_log_path: Path) -> None:
        self.events_log_path = events_log_path
        self.events_log_path.parent.mkdir(parents=True, exist_ok=True)

    def _emit(self, level: str, message: str, **extra) -> None:
        ts = now_iso()
        line = {"ts": ts, "level": level, "message": message, **extra}
        print(f"[{level}] {message}")
        with self.events_log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")

    def info(self, message: str, **extra) -> None:
        self._emit("INFO", message, **extra)

    def warn(self, message: str, **extra) -> None:
        self._emit("WARN", message, **extra)

    def error(self, message: str, **extra) -> None:
        self._emit("ERROR", message, **extra)
