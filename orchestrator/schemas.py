"""Shared schemas for meta-orchestrator runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class JobSpec:
    model: str
    dataset: str
    query_type: str = "all"
    mode: str = "run+eval"


@dataclass
class JobResult:
    model: str
    dataset: str
    query_type: str
    mode: str
    status: str
    return_code: int
    duration_sec: float
    command: list[str]
    summary_path: str | None = None
    macro_f1_mean: float | None = None
    stdout_tail: list[str] = field(default_factory=list)
    stderr_tail: list[str] = field(default_factory=list)
    raw_log_path: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
