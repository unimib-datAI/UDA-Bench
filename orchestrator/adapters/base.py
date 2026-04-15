"""Base adapter contract for model orchestrators."""

from __future__ import annotations

from typing import Protocol

from orchestrator.schemas import JobResult, JobSpec


class ModelAdapter(Protocol):
    name: str

    def execute(
        self,
        spec: JobSpec,
        rebuild: bool = False,
        rebuild_eval: bool = False,
        rebuild_extract: bool = False,
        rebuild_table: bool = False,
    ) -> JobResult:
        ...
