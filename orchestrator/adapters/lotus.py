"""Lotus adapter for root-level meta-orchestrator."""

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
    1) LOTUS_PYTHON env override
    2) repo local .venv-lotus python
    3) current interpreter
    """
    override = os.environ.get("LOTUS_PYTHON")
    if override and Path(override).exists():
        return override

    root = _repo_root()
    candidates = [
        root / ".venv-lotus" / "Scripts" / "python.exe",  # Windows
        root / ".venv-lotus" / "bin" / "python",  # Linux/macOS
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return sys.executable


def _summary_path(dataset: str, query_type: str) -> Path:
    # Lotus might produce outputs in systems/lotus/results/ or similar
    # This needs to be adjusted based on how lotus structures its outputs
    eval_root = _repo_root() / "systems" / "lotus" / "outputs" / dataset.lower() / "evaluation"
    if query_type == "all":
        return eval_root / "summary.json"
    return eval_root / f"summary_{query_type}.json"


class LotusAdapter:
    name = "lotus"

    def execute(
        self,
        spec: JobSpec,
        rebuild: bool = False,
        rebuild_eval: bool = False,
        rebuild_extract: bool = False,
        rebuild_table: bool = False,
    ) -> JobResult:
        # Lotus is a single-query system, so this adapter needs to handle
        # collecting queries for the dataset/query_type and running them
        # For now, this is a placeholder - you need to implement query collection
        # and evaluation logic similar to DocETL/Evaporate
        
        root = _repo_root()
        python_exe = _resolve_python()
        
        # Placeholder: collect SQL queries for spec.dataset and spec.query_type
        # This needs to be implemented
        sql_queries = self._collect_queries(spec.dataset, spec.query_type)
        
        if not sql_queries:
            # Handle case with no queries
            pass
        
        sql_queries = [s[s.index("SELECT"):].strip() for s in sql_queries if "SELECT" in s]
        
        cmd = [python_exe, "systems/lotus/main.py", "--sql"] + sql_queries
        
        # Add output directory
        if root:
            output_dir = root / "systems" / "lotus" / "results" / spec.dataset / spec.query_type / str(int(time.time()))
            cmd.extend(["--out_dir", Path(str(output_dir))])
        
        # Lotus specific flags
        if rebuild:
            # Add any rebuild flags if lotus supports them
            pass
        
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

        # For evaluation, you need to run the evaluation script on the outputs
        # This is a placeholder
        summary_path = None
        macro = None
        if spec.mode in {"eval", "run+eval"}:
            # Run evaluation here or assume lotus produces summary.json
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
            error=None if proc.returncode == 0 else "Lotus command failed",
        )
    
    def _collect_queries(self, dataset: str, query_type: str) -> list[str]:
        # Implement logic to collect SQL queries from Query/dataset/query_type/
        # Based on DocETL's query_loader.py
        query_root = _repo_root() / "Query" / dataset
        
        if not query_root.exists():
            return []
        
        all_queries = []
        
        # Map query_type to directory names
        type_to_dirs = {
            "all": ["Agg", "Filter", "Select", "Mixed", "Join"],
            "agg": ["Agg"],
            "filter": ["Filter"], 
            "select": ["Select"],
            "mixed": ["Mixed"],
            "join": ["Join"],
        }
        
        dirs = type_to_dirs.get(query_type.lower(), [query_type.capitalize()])
        
        for dir_name in dirs:
            query_dir = query_root / dir_name
            if not query_dir.exists():
                continue
                
            for sql_file in sorted(query_dir.glob("*.sql")):
                with open(sql_file, "r", encoding="utf-8") as f:
                    content = f.read()
                
                queries = self._split_sql_queries(content)
                all_queries.extend(queries)
        
        return all_queries

    def _split_sql_queries(self, text: str) -> list[str]:
        chunks = [q.strip() for q in text.split(";")]
        return [q for q in chunks if q]