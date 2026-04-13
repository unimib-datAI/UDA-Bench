import json
import csv
from pathlib import Path
import re

import duckdb
import pandas as pd
import sqlglot
from sqlglot import exp


def json_to_csv(json_path: str, csv_path: str):
    json_file = Path(json_path)
    if not json_file.exists():
        raise FileNotFoundError(f"JSON output non trovato: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id"])
        return

    if isinstance(data, dict):
        data = [data]

    rows = [flatten_row(row) for row in data]
    fieldnames = sorted({k for row in rows for k in row.keys()})

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def json_to_query_csv(
    json_path: str,
    csv_path: str,
    query_sql: str,
    config: dict,
    attributes: dict | None = None,
):
    """
    Convert DocETL JSON output to final query result CSV by executing the SQL
    over the extracted rows. This keeps evaluation aligned with SQL semantics.
    """
    json_file = Path(json_path)
    if not json_file.exists():
        raise FileNotFoundError(f"JSON output non trovato: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        data = []

    rows = [flatten_row(row) for row in data if isinstance(row, dict)]
    df = pd.DataFrame(rows)

    # Guarantee stable columns for empty extractions.
    if df.empty:
        out_df = _empty_result_frame_from_sql(query_sql)
        out_df.to_csv(csv_path, index=False)
        return

    # Normalize names and values.
    df.columns = [str(c).strip() for c in df.columns]
    df = _normalize_identifier_columns(df)
    df = _coerce_columns_by_attributes(df, attributes)
    df = _coerce_probable_numeric_columns(df)

    # Determine canonical table name from config.
    table_names = list((config or {}).get("tables", {}).keys())
    target_table = table_names[0] if table_names else "data"
    sql_for_exec = _rewrite_sql_table_names(query_sql, target_table)
    sql_for_exec = _cast_numeric_columns_in_sql(sql_for_exec, attributes)
    sql_for_exec = _inject_id_columns_for_alignment(sql_for_exec)

    conn = duckdb.connect(database=":memory:")
    try:
        conn.register(target_table, df)
        out_df = conn.execute(sql_for_exec).fetchdf()
    finally:
        conn.close()

    out_df.to_csv(csv_path, index=False)


def flatten_row(row: dict) -> dict:
    flat = {}
    for k, v in row.items():
        if isinstance(v, list):
            flat[k] = " | ".join(map(str, v))
        elif isinstance(v, dict):
            flat[k] = json.dumps(v, ensure_ascii=False)
        else:
            flat[k] = v
    return flat


def _normalize_identifier_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only one id representation for compatibility with evaluator.
    """
    out = df.copy()
    cols_lower = {c.lower(): c for c in out.columns}

    filename_col = cols_lower.get("filename") or cols_lower.get("file_name")
    id_col = cols_lower.get("id")

    if id_col is None and filename_col is not None:
        out["id"] = out[filename_col].astype(str).str.replace(r"\.[^.]+$", "", regex=True)
        id_col = "id"

    if id_col is not None:
        out[id_col] = out[id_col].astype(str).str.strip()

    return out


def _coerce_probable_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert columns that are mostly numeric to numeric dtype.
    This is crucial for AVG/SUM/MIN/MAX SQL over extracted strings.
    """
    out = df.copy()
    for col in out.columns:
        series = out[col]
        if pd.api.types.is_numeric_dtype(series):
            continue

        # Keep obviously textual columns untouched.
        col_l = str(col).lower()
        if any(tok in col_l for tok in ["name", "office", "profile", "activities", "risk", "event"]):
            continue

        clean = _clean_numeric_series(series)
        numeric = pd.to_numeric(clean, errors="coerce")
        non_null = clean.notna().sum()
        numeric_non_null = numeric.notna().sum()
        if non_null == 0:
            continue

        # Conservative conversion: convert only when most values are numeric.
        if numeric_non_null / non_null >= 0.8:
            out[col] = numeric

    return out


def _coerce_columns_by_attributes(df: pd.DataFrame, attributes: dict | None) -> pd.DataFrame:
    """
    Coerce numeric columns based on benchmark attributes metadata.
    """
    if not attributes:
        return df

    out = df.copy()

    numeric_cols: set[str] = set()
    for table, cols in attributes.items():
        if not isinstance(cols, dict):
            continue
        for col, meta in cols.items():
            if not isinstance(meta, dict):
                continue
            vt = str(meta.get("value_type", "")).lower()
            if vt in {"int", "float", "number", "numeric"}:
                numeric_cols.add(_norm_col_key(str(col)))
                numeric_cols.add(_norm_col_key(f"{table}.{col}"))

    for col in out.columns:
        col_key = _norm_col_key(str(col))
        base_key = _norm_col_key(str(col).split(".")[-1])
        if col_key not in numeric_cols and base_key not in numeric_cols:
            continue
        clean = _clean_numeric_series(out[col])
        out[col] = pd.to_numeric(clean, errors="coerce")
    return out


def _norm_col_key(name: str) -> str:
    """
    Normalize column keys for cross-dataset naming mismatches:
    - case-insensitive
    - remove quotes/backticks/spaces
    """
    return str(name).strip().strip('"').strip("'").strip("`").replace(" ", "").lower()


def _clean_numeric_series(series: pd.Series) -> pd.Series:
    """
    Normalize financial-style numbers:
    - currency symbols ($, EUR, etc.)
    - thousands separators
    - parenthesis negatives: (123) -> -123
    - percent suffix
    """
    s = (
        series.astype(str)
        .str.strip()
        .replace({"": pd.NA, "None": pd.NA, "none": pd.NA, "nan": pd.NA, "null": pd.NA})
    )
    # (123.4) -> -123.4
    s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    # remove currency symbols/codes and grouping separators
    s = s.str.replace(r"[$€£¥₹]", "", regex=True)
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace("%", "", regex=False)
    # keep only characters useful for numeric parsing
    s = s.str.replace(r"[^0-9eE+\-\.]", "", regex=True)
    return s


def _cast_numeric_columns_in_sql(sql_text: str, attributes: dict | None) -> str:
    """
    Add CAST on numeric column refs inside comparisons and aggregates.
    This prevents varchar-vs-number issues during SQL post-processing.
    """
    if not attributes:
        return sql_text

    numeric_cols: set[str] = set()
    for table, cols in attributes.items():
        if not isinstance(cols, dict):
            continue
        for col, meta in cols.items():
            if not isinstance(meta, dict):
                continue
            vt = str(meta.get("value_type", "")).lower()
            if vt in {"int", "float", "number", "numeric"}:
                numeric_cols.add(str(col).lower())
                numeric_cols.add(f"{str(table).lower()}.{str(col).lower()}")

    try:
        expr_root = sqlglot.parse_one(sql_text, error_level="ignore")
    except Exception:
        expr_root = None
    if expr_root is None:
        return sql_text

    def _is_numeric_col(node: exp.Expression) -> bool:
        if not isinstance(node, exp.Column):
            return False
        col = (node.name or "").lower()
        dotted = f"{(node.table or '').lower()}.{col}" if node.table else col
        return col in numeric_cols or dotted in numeric_cols

    def _cast_if_numeric(node: exp.Expression) -> exp.Expression:
        if _is_numeric_col(node):
            return exp.Cast(this=node.copy(), to=exp.DataType.build("DOUBLE"))
        return node

    for node in expr_root.walk():
        if isinstance(node, (exp.Avg, exp.Sum, exp.Min, exp.Max)):
            node.set("this", _cast_if_numeric(node.this))
            continue

        if isinstance(node, (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ, exp.NEQ)):
            node.set("this", _cast_if_numeric(node.this))
            node.set("expression", _cast_if_numeric(node.expression))

    return expr_root.sql(dialect="duckdb")


def _rewrite_sql_table_names(sql_text: str, target_table: str) -> str:
    """
    Replace source table names from query SQL with the extracted table name.
    """
    try:
        expr = sqlglot.parse_one(sql_text, error_level="ignore")
    except Exception:
        expr = None
    if expr is None:
        return sql_text

    sources = []
    for table in expr.find_all(sqlglot.exp.Table):
        if table.name:
            sources.append(table.name)

    rewritten = sql_text
    for source in set(sources):
        if source.lower() == target_table.lower():
            continue
        pattern = re.compile(rf"(?<![\w.]){re.escape(source)}(?![\w.])", flags=re.IGNORECASE)
        rewritten = pattern.sub(target_table, rewritten)
    return rewritten


def _inject_id_columns_for_alignment(sql_text: str) -> str:
    """
    Mirror evaluator behavior: for non-aggregation queries, ensure id columns are
    present so row matching can align prediction and gold.
    - single-table select/filter: inject id
    - joins: inject {table}.id for each table
    - aggregations: untouched
    """
    try:
        expr = sqlglot.parse_one(sql_text, error_level="ignore")
    except Exception:
        expr = None
    if expr is None:
        return sql_text

    group_expr = expr.args.get("group")
    has_group = bool(group_expr and getattr(group_expr, "expressions", None))
    has_agg = any(
        isinstance(node, (sqlglot.exp.Avg, sqlglot.exp.Sum, sqlglot.exp.Min, sqlglot.exp.Max, sqlglot.exp.Count))
        for node in expr.walk()
    )
    if has_group or has_agg:
        return sql_text

    tables = []
    for table in expr.find_all(sqlglot.exp.Table):
        if table.name and table.name not in tables:
            tables.append(table.name)

    existing = set()
    for item in getattr(expr, "selects", []):
        alias_or_name = getattr(item, "alias_or_name", None)
        if alias_or_name:
            existing.add(str(alias_or_name).lower())
        try:
            existing.add(str(item.sql(dialect="duckdb")).lower())
        except Exception:
            pass

    if len(tables) > 1:
        for t in tables:
            alias = f"{t}.id"
            if alias.lower() in existing:
                continue
            expr = expr.select(sqlglot.exp.alias_(sqlglot.exp.column("id", table=t), alias, quoted=True))
    else:
        if "id" not in existing:
            expr = expr.select(sqlglot.exp.alias_(sqlglot.exp.column("id"), "id"))

    return expr.sql(dialect="duckdb")


def _empty_result_frame_from_sql(sql_text: str) -> pd.DataFrame:
    try:
        expr = sqlglot.parse_one(sql_text, error_level="ignore")
    except Exception:
        expr = None
    if expr is None or not getattr(expr, "selects", None):
        return pd.DataFrame(columns=["id"])

    cols = []
    for item in expr.selects:
        alias = getattr(item, "alias_or_name", None)
        if alias:
            cols.append(str(alias))
        else:
            cols.append(item.sql(dialect="duckdb"))
    # preserve order + uniqueness
    unique = []
    seen = set()
    for c in cols:
        if c not in seen:
            unique.append(c)
            seen.add(c)
    return pd.DataFrame(columns=unique or ["id"])
