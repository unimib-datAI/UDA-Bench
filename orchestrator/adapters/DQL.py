"""DQL adapter for root-level meta-orchestrator."""

from __future__ import annotations

import json
import csv
import subprocess
import sys
import time
import re
from datetime import datetime, timezone
from pathlib import Path
import os

from orchestrator.schemas import JobResult, JobSpec


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_python() -> str:
    """
    Priority:
    1) DQL_PYTHON env override
    2) repo local .venv-DQL python
    3) current interpreter
    """
    override = os.environ.get("DQL_PYTHON")
    if override and Path(override).exists():
        return override

    root = _repo_root()
    candidates = [
        root / ".venv-DQL" / "Scripts" / "python.exe",  # Windows
        root / ".venv-DQL" / "bin" / "python",  # Linux/macOS
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
        _repo_root() / "systems" / "DQL" / "outputs" / dataset.lower() / "evaluation",
        _repo_root() / "systems" / "DQL" / "outputs" / dataset / "evaluation",
    ]
    summary_name = "summary.json" if query_type == "all" else f"summary_{query_type}.json"
    for r in roots:
        p = r / summary_name
        if p.exists():
            return p
    return roots[0] / summary_name


class DQLAdapter:
    name = "dql"

    def _dql_eval_summary_path(self, dataset: str, query_type: str) -> Path:
        root = _repo_root()
        eval_dir = root / "systems" / "DQL" / "results" / dataset / query_type / "evaluation"
        eval_dir.mkdir(parents=True, exist_ok=True)
        summary_name = "summary.json" if query_type == "all" else f"summary_{query_type}.json"
        return eval_dir / summary_name

    def _split_select_columns(self, sql: str) -> list[str]:
        """
        Minimal SELECT parser for adapter-side conversion.
        Works for common benchmark queries:
          SELECT col1, col2 FROM table
        """
        m = re.search(r"(?is)\bselect\b(.*?)\bfrom\b", sql or "")
        if not m:
            return []
        raw = m.group(1).strip()
        if not raw:
            return []
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        cols: list[str] = []
        for p in parts:
            # remove optional alias and table prefix
            p = re.sub(r"(?is)\s+as\s+\w+$", "", p).strip()
            if "." in p:
                p = p.split(".", 1)[1].strip()
            cols.append(p.strip("`\"[] "))
        return cols

    def _extract_from_table(self, sql: str) -> str | None:
        m = re.search(r"(?is)\bfrom\b\s+([a-zA-Z0-9_\.\"`\[\]]+)", sql or "")
        if not m:
            return None
        raw = m.group(1).strip().strip("`\"[]")
        # keep bare table name only
        return raw.split(".")[-1] if raw else None

    def _resolve_gt_csv(self, dataset: str, from_table: str | None) -> Path | None:
        root = _repo_root()
        gt_dir = root / "Query" / dataset
        if not gt_dir.exists():
            return None

        csv_files = sorted(gt_dir.glob("*.csv"))
        if not csv_files:
            return None

        if from_table:
            for p in csv_files:
                if p.stem.lower() == from_table.lower():
                    return p

        # Common DQL case: SQL uses "finance", dataset folder is "Finan" with Finan.csv.
        dataset_csv = gt_dir / f"{dataset}.csv"
        if dataset_csv.exists():
            return dataset_csv

        # Last fallback: single table dataset.
        if len(csv_files) == 1:
            return csv_files[0]
        return None

    def _align_sql_from_table(self, dataset: str, sql: str) -> str:
        table = self._extract_from_table(sql)
        gt_csv = self._resolve_gt_csv(dataset, table)
        if not table or not gt_csv:
            return sql
        target = gt_csv.stem
        if table.lower() == target.lower():
            return sql
        # Replace first FROM <table> occurrence only.
        pattern = re.compile(rf"(?is)(\bfrom\b\s+){re.escape(table)}(\b)")
        return pattern.sub(rf"\1{target}\2", sql, count=1)

    def _build_template_csv(
        self,
        dataset: str,
        sql: str,
        result_csv: Path,
    ) -> bool:
        """
        Build evaluator-compatible CSV with required columns + id keys.
        Values are left empty when DQL response is non-tabular narrative text.
        """
        cols = self._split_select_columns(sql)
        gt_csv = self._resolve_gt_csv(dataset, self._extract_from_table(sql))
        if not gt_csv:
            return False

        try:
            with gt_csv.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception:
            return False

        if not rows:
            return False

        id_values = []
        for i, r in enumerate(rows, start=1):
            rid = r.get("id")
            id_values.append(str(rid) if rid not in (None, "") else str(i))

        fieldnames = ["id"] + cols
        result_csv.parent.mkdir(parents=True, exist_ok=True)
        with result_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for rid in id_values:
                row = {"id": rid}
                for c in cols:
                    row[c] = ""
                writer.writerow(row)
        return True

    def _extract_rows_for_csv(self, payload: object) -> list[dict]:
        """
        Best-effort extraction of tabular rows from typical API response shapes.
        """
        if isinstance(payload, list) and all(isinstance(x, dict) for x in payload):
            return payload

        if isinstance(payload, dict):
            common_keys = ("rows", "data", "results", "items", "records", "result")
            for key in common_keys:
                candidate = payload.get(key)
                if isinstance(candidate, list) and all(isinstance(x, dict) for x in candidate):
                    return candidate
                if isinstance(candidate, dict):
                    rows = self._extract_rows_for_csv(candidate)
                    if rows:
                        return rows

        return []

    def _json_to_csv(self, results_json: Path, result_csv: Path, dataset: str, sql: str) -> bool:
        if not results_json.exists():
            # Still try template to keep evaluation flow consistent.
            return self._build_template_csv(dataset=dataset, sql=sql, result_csv=result_csv)

        try:
            payload = json.loads(results_json.read_text(encoding="utf-8"))
        except Exception:
            return self._build_template_csv(dataset=dataset, sql=sql, result_csv=result_csv)

        rows = self._extract_rows_for_csv(payload)
        if not rows:
            return self._build_template_csv(dataset=dataset, sql=sql, result_csv=result_csv)

        fieldnames: list[str] = []
        seen = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    fieldnames.append(key)

        result_csv.parent.mkdir(parents=True, exist_ok=True)
        with result_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return True

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
            
            if "finan" in spec.dataset.lower():
                user_id = "Finance"
            else:
                user_id = spec.dataset
            
            cmd = [python_exe, "systems/DQL/main.py", "--user-id", user_id, "--queries", sql]
            api_url = os.environ.get("DQL_API_URL")
            if api_url:
                cmd.extend(["--api-url", api_url])
            
            output_dir = root / "systems" / "DQL" / "results" / spec.dataset / "csv" / spec.query_type / f"query_{i+1}"
            output_dir.mkdir(parents=True, exist_ok=True)
            cmd.extend(["--out_dir", str(output_dir)])
            
            # Mode semantics aligned with other adapters:
            # - run: execute DQL only
            # - eval: skip DQL execution, evaluate existing artifacts only
            # - run+eval: execute then evaluate
            if spec.mode in {"run", "run+eval"}:
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
            else:
                all_stdout.append(f"[INFO] eval-only: skip DQL run for query_{i+1}, using existing artifacts")
            
            if spec.mode in {"eval", "run+eval"}:
                # Run evaluation for this query
                sql_file = output_dir / "sql.json"
                aligned_sql = self._align_sql_from_table(spec.dataset, sql)
                with open(sql_file, "w", encoding="utf-8") as f:
                    json.dump({"sql": aligned_sql}, f)
                
                result_csv = output_dir / "results.csv"
                if not result_csv.exists():
                    self._json_to_csv(
                        output_dir / "results.json",
                        result_csv,
                        dataset=spec.dataset,
                        sql=sql,
                    )

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
                    if eval_proc.returncode != 0:
                        overall_return_code = eval_proc.returncode
                else:
                    all_stderr.append(
                        f"[WARN] Missing results.csv for query_{i+1}; evaluation skipped (mode={spec.mode})"
                    )
                    overall_return_code = overall_return_code or 1

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
        summary_path = None
        if spec.mode in {"eval", "run+eval"}:
            sp = self._dql_eval_summary_path(spec.dataset, spec.query_type)
            payload = {
                "model": self.name,
                "dataset": spec.dataset,
                "query_type": spec.query_type,
                "mode": spec.mode,
                "queries_total": len(sql_queries),
                "queries_evaluated": len(macro_f1s),
                "macro_f1_mean": macro_f1_mean,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
            sp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            summary_path = str(sp)
        
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
            summary_path=summary_path,
            macro_f1_mean=macro_f1_mean,
            stdout_tail=all_stdout[-20:],
            stderr_tail=all_stderr[-20:],
            started_at=started_at,
            ended_at=ended_at,
            error=None if overall_return_code == 0 else "DQL execution failed",
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
