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
    """
    Prefer canonical output naming (lowercase dataset), fallback to exact dataset.
    This keeps backward compatibility with old folders.
    """
    roots = [
        _repo_root() / "systems" / "Lotus" / "outputs" / dataset.lower() / "evaluation",
        _repo_root() / "systems" / "Lotus" / "outputs" / dataset / "evaluation",
    ]
    summary_name = "summary.json" if query_type == "all" else f"summary_{query_type}.json"
    for r in roots:
        p = r / summary_name
        if p.exists():
            return p
    return roots[0] / summary_name


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
        root = _repo_root()
        python_exe = _resolve_python()
        
        sql_queries = self._collect_queries(spec.dataset, spec.query_type)
        
        if not sql_queries:
            return JobResult(
                model=self.name,
                dataset=spec.dataset,
                query_type=spec.query_type,
                mode=spec.mode,
                status="error",
                return_code=1,
                duration_sec=0,
                command=[],
                summary_path=None,
                macro_f1_mean=None,
                stdout_tail=[],
                stderr_tail=[],
                started_at=datetime.now(timezone.utc).isoformat(),
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="No queries found",
            )
        
        sql_queries = [s[s.index("SELECT"):].strip() for s in sql_queries if "SELECT" in s]
        
        started_at = datetime.now(timezone.utc).isoformat()
        t0 = time.time()
        
        macro_f1s = []
        all_stdout = []
        all_stderr = []
        overall_return_code = 0
        
        for i, sql in enumerate(sql_queries):
            print(f"[INFO] Executing query {i+1}/{len(sql_queries)}: {sql}")
            cmd = [python_exe, "systems/Lotus/main.py", "--sql", sql]
            
            output_dir = root / "systems" / "Lotus" / "results" / spec.dataset / "csv" / spec.query_type / f"query_{i+1}"
            output_dir.mkdir(parents=True, exist_ok=True)
            cmd.extend(["--out_dir", str(output_dir)])
            result_csv = output_dir / "results.csv"
            acc_file = output_dir / "acc_result" / "acc.json"
            
            if spec.mode in {"run", "run+eval"}:
                # Same resume semantics as DocETL/Evaporate: skip successful query outputs unless rebuild is requested.
                if not rebuild and result_csv.exists():
                    all_stdout.append(f"[INFO] skip run query_{i+1}: existing results.csv found")
                else:
                    if rebuild:
                        pass  # Add rebuild flags if supported
                    
                    proc = subprocess.run(
                        cmd,
                        cwd=str(root),
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                    )
                    
                    print(f"[INFO] Executing Done")
                    
                    all_stdout.extend(proc.stdout.splitlines())
                    all_stderr.extend(proc.stderr.splitlines())
                    
                    if proc.returncode != 0:
                        overall_return_code = proc.returncode
            
            if spec.mode in {"eval", "run+eval"}:
                # Keep evaluation incremental unless rebuild_eval is explicitly requested.
                if not rebuild_eval and acc_file.exists():
                    all_stdout.append(f"[INFO] skip eval query_{i+1}: existing acc.json found")
                    try:
                        with open(acc_file, "r", encoding="utf-8") as f:
                            acc = json.load(f)
                            f1 = acc.get("macro_f1") or acc.get("f1")
                            if isinstance(f1, (int, float)):
                                macro_f1s.append(float(f1))
                    except Exception:
                        pass
                    continue

                # Run evaluation for this query
                sql_file = root / f"temp_sql_{i}.json"
                with open(sql_file, "w", encoding="utf-8") as f:
                    json.dump({"sql": sql.replace("FROM finance", "FROM Finan").replace("FROM Finance", "FROM Finan")}, f)

                if result_csv.exists():
                    eval_cmd = [
                        python_exe, "-m", "evaluation.run_eval",
                        "--dataset", spec.dataset,
                        "--task", spec.query_type,
                        "--sql-file", str(sql_file),
                        "--result-csv", str(result_csv),
                        "--llm-provider", "none"
                    ]
                    
                    eval_proc = subprocess.run(
                        eval_cmd,
                        cwd=str(root)
                    )
                    
                    acc_file = result_csv.parent / "acc_result" / "acc.json"
                    if acc_file.exists():
                        try:
                            with open(acc_file, "r", encoding="utf-8") as f:
                                acc = json.load(f)
                                f1 = acc.get("macro_f1") or acc.get("f1")
                                if isinstance(f1, (int, float)):
                                    macro_f1s.append(float(f1))
                        except Exception:
                            pass
        
        duration = time.time() - t0
        ended_at = datetime.now(timezone.utc).isoformat()
        
        macro_f1_mean = sum(macro_f1s) / len(macro_f1s) if macro_f1s else None
        
        status = "ok" if overall_return_code == 0 else "error"
        
        return JobResult(
            model=self.name,
            dataset=spec.dataset,
            query_type=spec.query_type,
            mode=spec.mode,
            status=status,
            return_code=overall_return_code,
            duration_sec=duration,
            command=cmd,  # Last cmd
            summary_path=None,  # Not using summary.json
            macro_f1_mean=macro_f1_mean,
            stdout_tail=all_stdout[-20:],
            stderr_tail=all_stderr[-20:],
            started_at=started_at,
            ended_at=ended_at,
            error=None if overall_return_code == 0 else "Lotus execution failed",
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
