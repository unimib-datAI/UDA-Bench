import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from difflib import SequenceMatcher
import sqlglot
from sqlglot import exp
import pandas as pd

from query_loader import load_all_sql_queries
from utils import repo_root, dataset_real_name


TASK_MAP = {
    "agg": "Agg",
    "filter": "Filter",
    "select": "Select",
    "mixed": "Mixed",
    "join": "Join",
}
VALID_QUERY_TYPES = {"all", "agg", "filter", "select", "mixed", "join"}


def _query_index_from_id(query_id: str) -> int:
    try:
        return int(query_id.rsplit("_", 1)[-1])
    except Exception:
        return 1


def _safe_decode(data: bytes | None) -> str:
    if not data:
        return ""
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("utf-8", errors="replace")


def _load_table_name_map(root: Path, dataset_name: str) -> dict[str, str]:
    query_dir = root / "Query" / dataset_name
    gt_tables = [p.stem for p in sorted(query_dir.glob("*.csv"))]
    attr_files = sorted(query_dir.glob("*_attributes.json"))
    attr_tables: list[str] = []
    for af in attr_files:
        try:
            with open(af, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                attr_tables.extend(list(data.keys()))
        except Exception:
            continue

    mapping: dict[str, str] = {}
    if not gt_tables or not attr_tables:
        return mapping

    # Exact (case-insensitive) first.
    gt_by_lower = {t.lower(): t for t in gt_tables}
    for at in attr_tables:
        if at.lower() in gt_by_lower:
            mapping[at] = gt_by_lower[at.lower()]

    # Fallback: if one-table dataset, map everything to that table.
    if len(gt_tables) == 1:
        target = gt_tables[0]
        for at in attr_tables:
            mapping.setdefault(at, target)
        return mapping

    # Fallback by similarity.
    for at in attr_tables:
        if at in mapping:
            continue
        best = max(gt_tables, key=lambda gt: SequenceMatcher(None, at.lower(), gt.lower()).ratio())
        mapping[at] = best

    return mapping


def _rewrite_sql_table_names(sql: str, table_map: dict[str, str]) -> str:
    rewritten = sql
    for source, target in table_map.items():
        if source == target:
            continue
        pattern = re.compile(rf"(?<![\w.]){re.escape(source)}(?![\w.])", flags=re.IGNORECASE)
        rewritten = pattern.sub(target, rewritten)
    return rewritten


def _sanitize_sql_for_eval(sql: str) -> str:
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    cleaned = "\n".join(lines).strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    return cleaned


def _canonicalize_sql(sql: str) -> str:
    """Normalize SQL so evaluator parser receives a clean, single query."""
    try:
        expr = sqlglot.parse_one(sql, error_level="ignore")
    except Exception:
        expr = None
    if expr is None:
        return sql
    return expr.sql(dialect="duckdb")


def _load_numeric_columns(root: Path, dataset_name: str) -> set[str]:
    attr_path = root / "Query" / dataset_name / f"{dataset_name}_attributes.json"
    try:
        data = json.loads(attr_path.read_text(encoding="utf-8"))
    except Exception:
        return set()

    numeric: set[str] = set()
    if not isinstance(data, dict):
        return numeric
    for table, cols in data.items():
        if not isinstance(cols, dict):
            continue
        for col, meta in cols.items():
            if not isinstance(meta, dict):
                continue
            if str(meta.get("value_type", "")).lower() in {"int", "float", "number", "numeric"}:
                numeric.add(str(col).lower())
                numeric.add(f"{str(table).lower()}.{str(col).lower()}")
    return numeric


def _cast_numeric_agg_args(sql: str, numeric_columns: set[str]) -> str:
    """
    Ensure AVG/SUM run on numeric expressions even when CSV inference produced VARCHAR.
    """
    if not numeric_columns:
        return sql

    try:
        expr_root = sqlglot.parse_one(sql, error_level="ignore")
    except Exception:
        expr_root = None
    if expr_root is None:
        return sql

    def _col_key(c: exp.Column) -> tuple[str, str]:
        table = (c.table or "").lower()
        col = (c.name or "").lower()
        dotted = f"{table}.{col}" if table else col
        return col, dotted

    for node in expr_root.walk():
        if not isinstance(node, (exp.Avg, exp.Sum)):
            continue
        arg = node.this
        if not isinstance(arg, exp.Column):
            continue
        col, dotted = _col_key(arg)
        if col not in numeric_columns and dotted not in numeric_columns:
            continue
        casted = exp.Cast(this=arg.copy(), to=exp.DataType.build("DOUBLE"))
        node.set("this", casted)

    return expr_root.sql(dialect="duckdb")


def _dump_error_log(eval_root: Path, query_id: str, stdout: str, stderr: str) -> Path:
    logs_dir = eval_root / "_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"{query_id}.log"
    content = [
        f"QUERY: {query_id}",
        "",
        "=== STDOUT ===",
        stdout or "",
        "",
        "=== STDERR ===",
        stderr or "",
    ]
    log_path.write_text("\n".join(content), encoding="utf-8")
    return log_path


def _expected_columns_from_sql(sql_text: str) -> list[str]:
    try:
        expr = sqlglot.parse_one(sql_text, error_level="ignore")
    except Exception:
        expr = None
    if expr is None or not getattr(expr, "selects", None):
        return ["id"]

    cols: list[str] = []
    for item in expr.selects:
        alias = getattr(item, "alias_or_name", None)
        if alias:
            cols.append(str(alias))
            continue
        if hasattr(item, "name") and getattr(item, "name", None):
            cols.append(str(item.name))
            continue
        cols.append(item.sql(dialect="duckdb"))

    # Keep stable order and unique names
    dedup: list[str] = []
    seen = set()
    for c in cols:
        if c not in seen:
            dedup.append(c)
            seen.add(c)
    return dedup or ["id"]


def _prepare_result_csv_for_eval(
    csv_path: Path,
    temp_root: Path,
    query_id: str,
    expected_columns: list[str] | None = None,
) -> Path:
    """
    Prepare an evaluator-safe CSV.
    Avoid duplicated target names when evaluator maps filename -> id.
    """
    try:
        df = pd.read_csv(csv_path)
    except pd.errors.EmptyDataError:
        # Some DocETL runs can produce an empty file; keep evaluation running with empty headers.
        df = pd.DataFrame(columns=expected_columns or ["id"])
    cols = [str(c) for c in df.columns]
    col_set = set(cols)
    to_drop: set[str] = set()

    for col in cols:
        low = col.lower()
        if low in {"filename", "file_name"} and "id" in col_set:
            to_drop.add(col)
            continue

        if low.endswith(".filename") or low.endswith(".file_name"):
            prefix = col.rsplit(".", 1)[0]
            target_id = f"{prefix}.id"
            if target_id in col_set:
                to_drop.add(col)

    if to_drop:
        df = df.drop(columns=sorted(to_drop), errors="ignore")

    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    temp_root.mkdir(parents=True, exist_ok=True)
    safe_csv_path = temp_root / f"{query_id}.csv"
    df.to_csv(safe_csv_path, index=False)
    return safe_csv_path


def _normalize_query_type(query_type: str | None) -> str:
    qt = (query_type or "all").strip().lower()
    if qt not in VALID_QUERY_TYPES:
        raise ValueError(f"query_type non valido: {query_type}. Valori: {sorted(VALID_QUERY_TYPES)}")
    return qt


def run_evaluation(dataset_name: str, rebuild: bool = False, query_type: str = "all"):
    root = repo_root()
    all_queries = load_all_sql_queries(dataset_name)
    qt = _normalize_query_type(query_type)
    # Filter at loader-level to avoid touching any existing CSV/acc artifacts.
    queries = all_queries if qt == "all" else [q for q in all_queries if str(q.get("category", "")).lower() == qt]

    csv_root = root / "systems" / "DocETL" / "outputs" / dataset_real_name(dataset_name) / "csv"
    eval_root = root / "systems" / "DocETL" / "outputs" / dataset_real_name(dataset_name) / "evaluation"
    eval_root.mkdir(parents=True, exist_ok=True)
    sql_json_root = eval_root / "_sql_json"
    sql_json_root.mkdir(parents=True, exist_ok=True)
    temp_csv_root = eval_root / "_tmp_result_csv"
    temp_csv_root.mkdir(parents=True, exist_ok=True)
    attributes_file = root / "Query" / dataset_name / f"{dataset_name}_attributes.json"
    gt_dir = root / "Query" / dataset_name
    table_map = _load_table_name_map(root, dataset_name)
    numeric_columns = _load_numeric_columns(root, dataset_name)

    total = len(queries)
    ok = 0
    failed = 0
    skipped = 0
    macro_f1_values = []
    details = []

    for i, query_meta in enumerate(queries, start=1):
        query_id = query_meta["id"]
        print(f"[{i}/{total}] {query_id}")

        csv_path = csv_root / f"{query_id}.csv"
        if not csv_path.exists():
            failed += 1
            print("  ERROR -> CSV mancante")
            details.append({"query_id": query_id, "status": "missing_csv"})
            continue

        output_dir = eval_root / query_id
        acc_path = output_dir / "acc.json"
        if not rebuild and acc_path.exists():
            skipped += 1
            print("  SKIP -> evaluation gi\u00e0 presente")
            metric_payload = {}
            try:
                with open(acc_path, "r", encoding="utf-8") as f:
                    metric_payload = json.load(f)
                macro_f1 = metric_payload.get("macro_f1")
                if isinstance(macro_f1, (int, float)):
                    macro_f1_values.append(float(macro_f1))
            except Exception:
                metric_payload = {}
            details.append(
                {
                    "query_id": query_id,
                    "status": "skipped",
                    "acc_path": str(acc_path),
                    "metrics": metric_payload,
                }
            )
            continue

        task = TASK_MAP.get(query_meta["category"], "Select")
        sql_json_path = sql_json_root / f"{query_id}.sql.json"
        sql_text = _sanitize_sql_for_eval(query_meta["sql"])
        sql_text = _rewrite_sql_table_names(sql_text, table_map)
        sql_text = _canonicalize_sql(sql_text)
        sql_text = _cast_numeric_agg_args(sql_text, numeric_columns)
        expected_columns = _expected_columns_from_sql(sql_text)
        safe_csv_path = _prepare_result_csv_for_eval(
            csv_path, temp_csv_root, query_id, expected_columns=expected_columns
        )
        with open(sql_json_path, "w", encoding="utf-8") as f:
            json.dump({"sql": sql_text}, f, ensure_ascii=False, indent=2)

        cmd = [
            sys.executable,
            "-m",
            "evaluation.run_eval",
            "--dataset",
            dataset_name,
            "--task",
            task,
            "--sql-file",
            str(sql_json_path),
            "--query-id",
            "1",
            "--result-csv",
            str(safe_csv_path),
            "--output-dir",
            str(output_dir),
            "--attributes-file",
            str(attributes_file),
            "--gt-dir",
            str(gt_dir),
            "--llm-provider",
            "none",
        ]

        env = dict()
        env.update(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"

        result = subprocess.run(cmd, capture_output=True, text=False, cwd=str(root), env=env)
        stdout = _safe_decode(result.stdout)
        stderr = _safe_decode(result.stderr)
        if result.returncode != 0:
            failed += 1
            print("  ERROR -> evaluation fallita")
            log_path = _dump_error_log(eval_root, query_id, stdout, stderr)
            stderr_lines = (stderr or "").strip().splitlines()
            stdout_lines = (stdout or "").strip().splitlines()
            stderr_tail = stderr_lines[-12:]
            stdout_tail = stdout_lines[-12:]
            if stderr_tail:
                print("    STDERR:", " | ".join(stderr_tail))
            elif stdout_tail:
                print("    STDOUT:", " | ".join(stdout_tail))
            print(f"    LOG   : {log_path}")
            details.append(
                {
                    "query_id": query_id,
                    "status": "error",
                    "stdout": stdout,
                    "stderr": stderr,
                    "log_path": str(log_path),
                }
            )
            continue

        ok += 1
        print("  OK -> acc.json")

        metric_payload = {}
        if acc_path.exists():
            try:
                with open(acc_path, "r", encoding="utf-8") as f:
                    metric_payload = json.load(f)
                macro_f1 = metric_payload.get("macro_f1")
                if isinstance(macro_f1, (int, float)):
                    macro_f1_values.append(float(macro_f1))
            except Exception:
                metric_payload = {}

        details.append(
            {
                "query_id": query_id,
                "status": "ok",
                "acc_path": str(acc_path),
                "metrics": metric_payload,
            }
        )

    summary = {
        "dataset": dataset_name,
        "query_type": qt,
        "total": total,
        "ok": ok,
        "skip": skipped,
        "errors": failed,
        "macro_f1_mean": (sum(macro_f1_values) / len(macro_f1_values)) if macro_f1_values else None,
        "details": details,
    }

    # Keep backward compatibility for "all", and explicit files for subsets.
    summary_name = "summary.json" if qt == "all" else f"summary_{qt}.json"
    summary_path = eval_root / summary_name
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\n=== EVAL RIEPILOGO ===")
    print(f"Dataset      : {dataset_name}")
    print(f"Query type   : {qt}")
    print(f"Totali       : {total}")
    print(f"OK           : {ok}")
    print(f"Skip         : {skipped}")
    print(f"Errori       : {failed}")
    print(f"macro_f1 mean: {summary['macro_f1_mean']}")
    print(f"Summary file : {summary_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Nome cartella dataset, es. Finan")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Riesegue evaluation anche se acc.json Ã¨ giÃ  presente",
    )
    parser.add_argument(
        "--query-type",
        default="all",
        choices=sorted(VALID_QUERY_TYPES),
        help="Valuta solo una categoria di query (all, agg, filter, select, mixed, join)",
    )
    args = parser.parse_args()

    run_evaluation(args.dataset, rebuild=args.rebuild, query_type=args.query_type)

