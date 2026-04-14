import argparse
import json
import os
import re
import subprocess
import sys
from difflib import SequenceMatcher
from pathlib import Path

import duckdb
import pandas as pd

from query_loader import load_all_sql_queries
from utils import dataset_real_name, repo_root


COLUMN_ALIASES = {
    "total_Debt": "total_debt",
    "total_debt": "total_debt",
    "total_assets": "total_assets",
    "cash_reserves": "cash_reserves",
    "net_assets": "net_assets",
    "net_profit_or_loss": "net_profit_or_loss",
    "earnings_per_share": "earnings_per_share",
    "dividend_per_share": "dividend_per_share",
    "largest_shareholder": "largest_shareholder",
    "the_highest_ownership_stake": "the_highest_ownership_stake",
    "major_equity_changes": "major_equity_changes",
    "major_events": "major_events",
    "company_name": "company_name",
    "registered_office": "registered_office",
    "exchange_code": "exchange_code",
    "principal_activities": "principal_activities",
    "board_members": "board_members",
    "executive_profiles": "executive_profiles",
    "revenue": "revenue",
    "auditor": "auditor",
    "remuneration_policy": "remuneration_policy",
    "business_segments_num": "business_segments_num",
    "business_risks": "business_risks",
    "bussiness_sales": "bussiness_sales",
    "bussiness_profit": "bussiness_profit",
    "bussiness_cost": "bussiness_cost",
    "business_sales": "bussiness_sales",
    "business_profit": "bussiness_profit",
    "business_cost": "bussiness_cost",
}


def _safe_decode(data: bytes | None) -> str:
    if not data:
        return ""
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("utf-8", errors="replace")


def _run_cmd_stream(cmd: list[str], cwd: Path, env: dict | None = None) -> tuple[int, str]:
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="")
        lines.append(line)
    rc = proc.wait()
    return rc, "".join(lines)


def _sanitize_sql(sql: str) -> str:
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


def _normalize_text_value(value: str) -> str:
    if value is None:
        return ""
    s = str(value).replace("\r", " ").replace("\n", " ").strip()
    s = s.strip("\"' ")
    s = re.sub(r"\s+", " ", s).strip()
    if s.lower() in {"none", "null", "nan", "n/a", "not available"}:
        return ""
    return s


def _ensure_id_in_row_level_sql(sql: str, category: str) -> str:
    """
    For row-level queries (select/filter), ensure `id` is present in projection.
    This prevents evaluator key-matching on empty ids.
    """
    if category not in {"select", "filter"}:
        return sql

    m = re.search(r"^\s*select\s+(.*?)\s+from\s", sql, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return sql

    projection = m.group(1)
    if re.search(r"(^|,)\s*([A-Za-z_][A-Za-z0-9_]*\.)?id\s*(,|$)", projection, flags=re.IGNORECASE):
        return sql

    if re.match(r"^\s*distinct\b", projection, flags=re.IGNORECASE):
        new_projection = re.sub(r"^\s*distinct\b", "DISTINCT id,", projection, count=1, flags=re.IGNORECASE)
    else:
        new_projection = f"id, {projection}"

    start, end = m.span(1)
    return f"{sql[:start]}{new_projection}{sql[end:]}"


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

    gt_by_lower = {t.lower(): t for t in gt_tables}
    for at in attr_tables:
        if at.lower() in gt_by_lower:
            mapping[at] = gt_by_lower[at.lower()]

    if len(gt_tables) == 1:
        target = gt_tables[0]
        for at in attr_tables:
            mapping.setdefault(at, target)
        return mapping

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


def _rewrite_sql_column_aliases(sql: str) -> str:
    rewritten = sql
    for source, target in sorted(COLUMN_ALIASES.items(), key=lambda x: len(x[0]), reverse=True):
        if source == target:
            continue
        pattern = re.compile(rf"(?<![\w.]){re.escape(source)}(?![\w.])", flags=re.IGNORECASE)
        rewritten = pattern.sub(target, rewritten)
    return rewritten


def _parse_number(value: str):
    v = str(value).strip()
    if not v:
        return None
    v = re.sub(r"^\((.*)\)$", r"-\1", v)
    v = re.sub(r"[,$€£¥₹%]", "", v)
    v = v.replace(",", "")
    v = re.sub(r"\s+", "", v)
    v = re.sub(r"[^0-9eE+\-.]", "", v)
    if not v:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _build_table_json_from_dataset_csv(dataset_dir: Path, target_path: Path) -> Path:
    csv_files = sorted(
        [p for p in dataset_dir.glob("*.csv") if p.is_file()],
        key=lambda p: p.name.lower(),
    )
    if not csv_files:
        raise FileNotFoundError(
            f"Nessun CSV trovato in {dataset_dir}. "
            "Serve Data/<dataset>/table.json o almeno un CSV con ground truth."
        )

    by_doc: dict[str, dict[str, str]] = {}
    has_any_id = False

    for csv_path in csv_files:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        if df.empty:
            continue

        id_col = None
        for candidate in ("ID", "id", "doc_id"):
            if candidate in df.columns:
                id_col = candidate
                break
        if id_col is None:
            continue
        has_any_id = True

        cols = [c for c in df.columns if c != id_col]
        for _, row in df.iterrows():
            raw_id = str(row.get(id_col, "")).strip()
            if not raw_id:
                continue
            doc_key = raw_id if raw_id.lower().endswith(".txt") else f"{raw_id}.txt"
            if doc_key not in by_doc:
                by_doc[doc_key] = {}
            for c in cols:
                by_doc[doc_key][str(c)] = str(row.get(c, ""))

    if not has_any_id:
        # Final fallback: use first CSV and row index as doc id.
        df = pd.read_csv(csv_files[0], dtype=str, keep_default_na=False)
        cols = list(df.columns)
        for idx, row in df.iterrows():
            doc_key = f"{idx + 1}.txt"
            by_doc[doc_key] = {str(c): str(row.get(c, "")) for c in cols}

    if not by_doc:
        raise ValueError(f"Impossibile costruire table.json da {dataset_dir}")

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(json.dumps(by_doc, ensure_ascii=False, indent=2), encoding="utf-8")
    return target_path


def _load_numeric_columns(root: Path, dataset_name: str) -> set[str]:
    attr_path = root / "Query" / dataset_name / f"{dataset_name}_attributes.json"
    try:
        data = json.loads(attr_path.read_text(encoding="utf-8"))
    except Exception:
        return set()

    numeric: set[str] = set()
    if not isinstance(data, dict):
        return numeric
    for _, cols in data.items():
        if not isinstance(cols, dict):
            continue
        for col, meta in cols.items():
            if not isinstance(meta, dict):
                continue
            if str(meta.get("value_type", "")).lower() in {"int", "float", "number", "numeric"}:
                numeric.add(str(col).lower())
    return numeric


def _extract_run_string(stdout: str) -> str | None:
    m = re.search(r"RUN_STRING:\s*([^\s]+)", stdout)
    if m:
        return m.group(1).strip()
    m = re.search(r"run_string\s*=\s*'([^']+)'", stdout)
    if m:
        return m.group(1).strip()
    m = re.search(r"\b(dl[a-zA-Z0-9_]+)\b", stdout)
    if m:
        return m.group(1).strip()
    return None


def _run_evaporate_extract(
    dataset_name: str,
    out_root: Path,
    model: str,
    train_size: int,
    num_top_k_scripts: int,
    chunk_size: int,
    max_chunks_per_file: int,
    rebuild_extract: bool,
) -> tuple[str, Path]:
    root = repo_root()
    dataset_dir = root / "Data" / dataset_name
    txt_dir = dataset_dir / "txt"
    table_json = dataset_dir / "table.json"

    if not txt_dir.exists():
        raise FileNotFoundError(f"Cartella txt non trovata: {txt_dir}")
    if not table_json.exists():
        generated = out_root / "evaporate_work" / "generated_table.json"
        table_json = _build_table_json_from_dataset_csv(dataset_dir, generated)

    work_root = out_root / "evaporate_work"
    gi_path = work_root / "generative_indexes" / dataset_real_name(dataset_name)
    results_dumps = work_root / "results_dumps"
    latest_run = results_dumps / "latest_run.txt"

    if (
        not rebuild_extract
        and latest_run.exists()
        and gi_path.exists()
        and list(gi_path.glob("*_file2metadata.json"))
    ):
        run_prefix = latest_run.read_text(encoding="utf-8").strip()
        if run_prefix:
            return run_prefix, gi_path

    work_root.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(root / "systems" / "Evaporate" / "run_profiler.py"),
        "--data_lake",
        dataset_real_name(dataset_name),
        "--data_dir",
        str(txt_dir),
        "--base_data_dir",
        str(work_root),
        "--generative_index_path",
        str(gi_path),
        "--cache_dir",
        str(work_root / "cache" / dataset_real_name(dataset_name)),
        "--gold_extractions_file",
        str(table_json),
        "--train_size",
        str(train_size),
        "--num_top_k_scripts",
        str(num_top_k_scripts),
        "--chunk_size",
        str(chunk_size),
        "--max_chunks_per_file",
        str(max_chunks_per_file),
    ]

    if rebuild_extract:
        cmd.append("--overwrite_cache")

    env = dict(os.environ)
    env["EVAPORATE_MODEL"] = model
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    rc, out = _run_cmd_stream(cmd, cwd=root, env=env)
    if rc != 0:
        raise RuntimeError(
            "Evaporate extraction failed.\n"
            f"CMD: {' '.join(cmd)}\nOUTPUT:\n{out}"
        )

    run_prefix = _extract_run_string(out)
    if not run_prefix and latest_run.exists():
        run_prefix = latest_run.read_text(encoding="utf-8").strip()
    if not run_prefix:
        raise RuntimeError("Impossibile determinare run prefix Evaporate da stdout/latest_run.txt")

    return run_prefix, gi_path


def _build_evaporate_table(gi_path: Path, run_prefix: str, table_csv: Path, rebuild_table: bool) -> None:
    if table_csv.exists() and not rebuild_table:
        return

    root = repo_root()
    cmd = [
        sys.executable,
        str(root / "systems" / "Evaporate" / "uda_integration" / "build_evaporate_table_from_metadata.py"),
        "--input-dir",
        str(gi_path),
        "--output",
        str(table_csv),
        "--run-prefix",
        run_prefix,
    ]
    rc, out = _run_cmd_stream(cmd, cwd=root, env=dict(os.environ))
    if rc != 0:
        raise RuntimeError(
            "Build evaporate table failed.\n"
            f"CMD: {' '.join(cmd)}\nOUTPUT:\n{out}"
        )


def _prepare_table_dataframe(table_csv: Path, numeric_columns: set[str]) -> pd.DataFrame:
    df = pd.read_csv(table_csv, dtype=str, keep_default_na=False)
    df.columns = [str(c).strip() for c in df.columns]

    # Keep naming compatibility with SQL/evaluator expectations.
    for alias, target in COLUMN_ALIASES.items():
        if alias not in df.columns and target in df.columns:
            df[alias] = df[target]

    if "id" not in df.columns and "doc_id" in df.columns:
        df["id"] = df["doc_id"].astype(str).str.replace(r"\.txt$", "", regex=True)
    elif "id" in df.columns:
        df["id"] = df["id"].astype(str).str.replace(r"\.txt$", "", regex=True)

    for col in list(df.columns):
        if str(col).lower() in {"id", "doc_id", "file_id"}:
            continue
        df[col] = df[col].apply(_normalize_text_value)

    for col in list(df.columns):
        lower = col.lower()
        if lower in numeric_columns:
            parsed = df[col].apply(_parse_number)
            # Force numeric dtype so DuckDB infers DOUBLE (not VARCHAR).
            df[col] = pd.to_numeric(parsed, errors="coerce")

    return df


def _execute_sql(df: pd.DataFrame, sql: str, dataset_name: str, table_map: dict[str, str]) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    con.register("evaporate_table", df)

    aliases = {
        dataset_name,
        dataset_name.lower(),
        dataset_name.upper(),
        dataset_name.capitalize(),
    }
    aliases.update(table_map.keys())
    aliases.update(table_map.values())

    for alias in sorted(aliases):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias):
            continue
        try:
            con.execute(f'CREATE VIEW "{alias}" AS SELECT * FROM evaporate_table')
        except Exception:
            pass

    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def run_dataset(
    dataset_name: str,
    rebuild: bool = False,
    rebuild_extract: bool = False,
    rebuild_table: bool = False,
    model: str = "gemini-2.5-flash",
    train_size: int = 20,
    num_top_k_scripts: int = 2,
    chunk_size: int = 2000,
    max_chunks_per_file: int = 3,
):
    root = repo_root()
    out_root = root / "systems" / "Evaporate" / "outputs" / dataset_real_name(dataset_name)
    csv_root = out_root / "csv"
    log_root = out_root / "_logs"
    table_csv = out_root / "evaporate_full_table.csv"
    csv_root.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)

    run_prefix, gi_path = _run_evaporate_extract(
        dataset_name=dataset_name,
        out_root=out_root,
        model=model,
        train_size=train_size,
        num_top_k_scripts=num_top_k_scripts,
        chunk_size=chunk_size,
        max_chunks_per_file=max_chunks_per_file,
        rebuild_extract=rebuild_extract,
    )
    _build_evaporate_table(gi_path, run_prefix, table_csv, rebuild_table=rebuild_table)

    queries = load_all_sql_queries(dataset_name)
    table_map = _load_table_name_map(root, dataset_name)
    numeric_columns = _load_numeric_columns(root, dataset_name)
    df = _prepare_table_dataframe(table_csv, numeric_columns=numeric_columns)

    total = len(queries)
    ok = 0
    failed = 0
    skipped = 0

    for idx, query_meta in enumerate(queries, start=1):
        query_id = query_meta["id"]
        print(f"[{idx}/{total}] {query_id}")
        csv_path = csv_root / f"{query_id}.csv"

        if csv_path.exists() and not rebuild:
            skipped += 1
            print(f"  SKIP -> {csv_path.name} (gia presente)")
            continue

        sql = _sanitize_sql(query_meta["sql"])
        sql = _rewrite_sql_table_names(sql, table_map)
        sql = _ensure_id_in_row_level_sql(sql, query_meta["category"])

        try:
            res = _execute_sql(df, sql, dataset_name, table_map)
        except Exception as exc_first:
            sql_retry = _rewrite_sql_column_aliases(sql)
            try:
                res = _execute_sql(df, sql_retry, dataset_name, table_map)
            except Exception as exc_second:
                failed += 1
                log_path = log_root / f"{query_id}.log"
                log_path.write_text(
                    (
                        f"QUERY: {query_id}\n\n"
                        f"SQL_ORIG:\n{sql}\n\n"
                        f"FIRST_ERROR:\n{exc_first}\n\n"
                        f"SQL_RETRY:\n{sql_retry}\n\n"
                        f"SECOND_ERROR:\n{exc_second}\n"
                    ),
                    encoding="utf-8",
                )
                print(f"  ERROR -> {exc_second}")
                print(f"  LOG   -> {log_path}")
                continue

        res.to_csv(csv_path, index=False)
        ok += 1
        print(f"  OK -> {csv_path.name}")

    summary = {
        "dataset": dataset_name,
        "total": total,
        "ok": ok,
        "skip": skipped,
        "errors": failed,
        "run_prefix": run_prefix,
        "table_csv": str(table_csv),
    }
    summary_path = out_root / "run_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\n=== RIEPILOGO ===")
    print(f"Dataset: {dataset_name}")
    print(f"Totali : {total}")
    print(f"OK     : {ok}")
    print(f"Skip   : {skipped}")
    print(f"Errori : {failed}")
    print(f"Run ID : {run_prefix}")
    print(f"Table  : {table_csv}")
    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Nome dataset, es. Finan")
    parser.add_argument("--rebuild", action="store_true", help="Ricalcola CSV query anche se esistono")
    parser.add_argument(
        "--rebuild-extract",
        action="store_true",
        help="Riesegue Evaporate extraction (ignora latest run)",
    )
    parser.add_argument(
        "--rebuild-table",
        action="store_true",
        help="Ricostruisce evaporate_full_table.csv",
    )
    parser.add_argument("--model", default="gemini-2.5-flash")
    parser.add_argument("--train-size", type=int, default=20)
    parser.add_argument("--num-top-k-scripts", type=int, default=2)
    parser.add_argument("--chunk-size", type=int, default=2000)
    parser.add_argument("--max-chunks-per-file", type=int, default=3)
    args = parser.parse_args()

    run_dataset(
        dataset_name=args.dataset,
        rebuild=args.rebuild,
        rebuild_extract=args.rebuild_extract,
        rebuild_table=args.rebuild_table,
        model=args.model,
        train_size=args.train_size,
        num_top_k_scripts=args.num_top_k_scripts,
        chunk_size=args.chunk_size,
        max_chunks_per_file=args.max_chunks_per_file,
    )
