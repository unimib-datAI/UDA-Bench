"""DocETL adapter for root-level meta-orchestrator."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import os

from orchestrator.schemas import JobResult, JobSpec


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_python() -> str:
    """
    Priority:
    1) DOCETL_PYTHON env override
    2) repo local .venv-docetl python
    3) current interpreter
    """
    override = os.environ.get("DOCETL_PYTHON")
    if override and Path(override).exists():
        return override

    root = _repo_root()
    candidates = [
        root / ".venv-docetl" / "Scripts" / "python.exe",  # Windows
        root / ".venv-docetl" / "bin" / "python",  # Linux/macOS
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return sys.executable


def _summary_path(dataset: str, query_type: str) -> Path:
    eval_root = _repo_root() / "systems" / "DocETL" / "outputs" / dataset.lower() / "evaluation"
    if query_type == "all":
        return eval_root / "summary.json"
    return eval_root / f"summary_{query_type}.json"


class DocETLAdapter:
    name = "docetl"

    def execute(
        self,
        spec: JobSpec,
        rebuild: bool = False,
        rebuild_eval: bool = False,
        rebuild_extract: bool = False,
        rebuild_table: bool = False,
    ) -> JobResult:
        del rebuild_extract, rebuild_table  # not used by DocETL

        root = _repo_root()
        python_exe = _resolve_python()
        cmd = [python_exe, "systems/DocETL/orchestrator/main.py", "--dataset", spec.dataset]

        if spec.mode == "run":
            if rebuild:
                cmd.append("--rebuild")
        elif spec.mode == "eval":
            cmd.extend(["--eval-only", "--query-type", spec.query_type])
            if rebuild_eval:
                cmd.append("--rebuild-eval")
        elif spec.mode == "run+eval":
            cmd.extend(["--eval", "--query-type", spec.query_type])
            if rebuild:
                cmd.append("--rebuild")
            if rebuild_eval:
                cmd.append("--rebuild-eval")
        else:
            raise ValueError(f"Unsupported mode for docetl: {spec.mode}")

        started_at = datetime.now(timezone.utc).isoformat()
        t0 = time.time()
        proc = subprocess.run(
            cmd,
            cwd=str(root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        duration = time.time() - t0
        ended_at = datetime.now(timezone.utc).isoformat()

        stdout_lines = (proc.stdout or "").splitlines()
        stderr_lines = (proc.stderr or "").splitlines()

        summary_path = None
        macro = None
        if spec.mode in {"eval", "run+eval"}:
            sp = _summary_path(spec.dataset, spec.query_type)
            if sp.exists():
                summary_path = str(sp)
                try:
                    payload = json.loads(sp.read_text(encoding="utf-8"))
                    val = payload.get("macro_f1_mean")
                    if isinstance(val, (int, float)):
                        macro = float(val)
                except Exception:
                    pass

        status = "ok" if proc.returncode == 0 else "error"
        return JobResult(
            model=self.name,
            dataset=spec.dataset,
            query_type=spec.query_type,
            mode=spec.mode,
            status=status,
            return_code=proc.returncode,
            duration_sec=duration,
            command=cmd,
            summary_path=summary_path,
            macro_f1_mean=macro,
            stdout_tail=stdout_lines[-20:],
            stderr_tail=stderr_lines[-20:],
            started_at=started_at,
            ended_at=ended_at,
            error=None if proc.returncode == 0 else "DocETL orchestrator command failed",
        )
