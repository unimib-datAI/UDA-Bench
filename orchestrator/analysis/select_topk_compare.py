from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import mean

import pandas as pd
import sqlglot
from sqlglot import exp


@dataclass
class PerQuery:
    task: str
    model: str
    query_idx: int
    status: str
    macro_f1: float | None
    note: str = ""


@dataclass
class Global:
    task: str
    model: str
    completed: int
    total: int
    completion_rate: float
    macro_f1_mean: float | None
    pooled_precision: float | None = None
    pooled_recall: float | None = None
    pooled_f1: float | None = None
    total_pred_mass: float = 0.0
    total_gold_mass: float = 0.0
    total_tp_pred: float = 0.0
    total_tp_gold: float = 0.0


def _collect_lotus_from_benchmark_csv(dataset: str, task: str, total_queries: int) -> Global | None:
    csv_path = _repo_root() / "orchestrator" / "analysis" / "benchmark_results.csv"
    if not csv_path.exists():
        return None

    best_row: dict | None = None
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if str(row.get("model", "")).strip().lower() != "lotus":
                continue
            if str(row.get("dataset", "")).strip().lower() != dataset.lower():
                continue
            if str(row.get("query_type", "")).strip().lower() != task.lower():
                continue
            macro_raw = str(row.get("macro_f1_mean", "")).strip()
            if not macro_raw:
                continue
            if best_row is None or str(row.get("created_at_utc", "")) > str(best_row.get("created_at_utc", "")):
                best_row = row

    if best_row is None:
        return None
    try:
        macro = float(str(best_row.get("macro_f1_mean", "")).strip())
    except Exception:
        return None

    status = str(best_row.get("job_status", "")).strip().lower()
    completed = total_queries if status == "ok" else 0
    return Global(
        task=task.lower(),
        model="lotus",
        completed=completed,
        total=total_queries,
        completion_rate=(completed / total_queries) if total_queries else 0.0,
        macro_f1_mean=macro,
        pooled_precision=None,
        pooled_recall=None,
        pooled_f1=None,
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_dql_task_query_dir(root: Path, dataset: str, task: str, query_idx: int) -> Path:
    t = task.lower()
    candidates = [
        root / "systems" / "DQL" / "outputs" / dataset.lower() / t / "csv" / f"query_{query_idx}",
        root / "systems" / "DQL" / "results" / dataset / t / "csv" / f"query_{query_idx}",
    ]
    for c in candidates:
        if c.exists():
            return c
    return candidates[0]


def _resolve_task_sql_file(root: Path, dataset: str, task: str) -> Path:
    task_dir = root / "Query" / dataset / task.capitalize()
    if not task_dir.exists():
        raise SystemExit(f"Missing task dir: {task_dir}")
    t = task.lower()
    preferred = [
        task_dir / f"{t}_queries.sql",
        task_dir / f"{t}_queries_{dataset.lower()}.sql",
    ]
    for p in preferred:
        if p.exists():
            return p
    sqls = sorted(task_dir.glob("*.sql"))
    if not sqls:
        raise SystemExit(f"No SQL file found in {task_dir}")
    return sqls[0]


def _split_sql_file(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    chunks = [c.strip() for c in text.split(";")]
    return [c for c in chunks if c]


def _gt_table_names(dataset: str) -> list[str]:
    gt_dir = _repo_root() / "Query" / dataset
    if not gt_dir.exists():
        return []
    return sorted(p.stem for p in gt_dir.glob("*.csv"))


def _extract_numeric_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.extract(r"(\d+)", expand=False).astype(float)


def _filter_topk_rows(df: pd.DataFrame, k: int) -> pd.DataFrame:
    cols_lower = {str(c).lower(): str(c) for c in df.columns}
    preferred = ["id", "doc_id", "file_id", "filename", "file_name"]
    for key in preferred:
        if key in cols_lower:
            col = cols_lower[key]
            ids = _extract_numeric_series(df[col])
            mask = ids.notna() & (ids <= float(k))
            out = df.loc[mask].copy()
            if not out.empty:
                return out
    return df.head(k).copy()


def _with_where_topk(sql_text: str, k: int) -> str:
    expr = sqlglot.parse_one(sql_text, error_level="ignore")
    if expr is None:
        return sql_text
    cond = exp.LTE(this=exp.column("id"), expression=exp.Literal.number(k))
    where = expr.args.get("where")
    if where is None:
        expr.set("where", exp.Where(this=cond))
    else:
        expr.set("where", exp.Where(this=exp.and_(where.this, cond)))
    return expr.sql(dialect="duckdb")


def _normalize_eval_sql_numeric_agg(sql_text: str) -> str:
    """
    Evaluation guardrail:
    DuckDB fails on AVG/SUM over VARCHAR columns (common in Finan CSV fields).
    We only patch evaluation SQL, not source system outputs.
    """
    def _safe_cast_agg(match: re.Match) -> str:
        fn = match.group("fn")
        arg = (match.group("arg") or "").strip()
        low = arg.lower()
        if "cast(" in low or "try_cast(" in low:
            return f"{fn}({arg})"
        if fn.lower() == "count" and arg == "*":
            return f"{fn}({arg})"
        if fn.lower() in {"avg", "sum"}:
            return f"{fn}(TRY_CAST({arg} AS DOUBLE))"
        return f"{fn}({arg})"

    agg_pat = re.compile(
        r"(?is)\b(?P<fn>avg|sum|count)\s*\(\s*(?P<arg>(?:[^()]|\([^()]*\))+)\s*\)"
    )
    return agg_pat.sub(_safe_cast_agg, sql_text or "")


def _align_sql_tables_to_gt(sql_text: str, gt_tables: list[str]) -> str:
    """
    Best-effort table-name alignment for evaluation:
    - exact match
    - case-insensitive match
    - if only one GT table exists, map missing table names to it
    """
    expr = sqlglot.parse_one(sql_text, error_level="ignore")
    if expr is None or not gt_tables:
        return sql_text

    gt_exact = set(gt_tables)
    gt_lower = {t.lower(): t for t in gt_tables}
    single_gt = gt_tables[0] if len(gt_tables) == 1 else None

    changed = False
    for t in expr.find_all(exp.Table):
        name = t.name
        if not name:
            continue
        if name in gt_exact:
            continue
        if name.lower() in gt_lower:
            t.set("this", exp.to_identifier(gt_lower[name.lower()]))
            changed = True
            continue
        if single_gt is not None:
            t.set("this", exp.to_identifier(single_gt))
            changed = True

    if not changed:
        return sql_text
    return expr.sql(dialect="duckdb")


def _safe_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _run_eval(
    root: Path,
    dataset: str,
    task: str,
    sql_json: Path,
    result_csv: Path,
    out_dir: Path,
) -> tuple[int, str, str]:
    cmd = [
        sys.executable,
        "-m",
        "evaluation.run_eval",
        "--dataset",
        dataset,
        "--task",
        task,
        "--sql-file",
        str(sql_json),
        "--query-id",
        "1",
        "--result-csv",
        str(result_csv),
        "--output-dir",
        str(out_dir),
        "--attributes-file",
        str(root / "Query" / dataset / f"{dataset}_attributes.json"),
        "--gt-dir",
        str(root / "Query" / dataset),
        "--llm-provider",
        "none",
    ]
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    p = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True, encoding="utf-8", errors="replace", env=env)
    return p.returncode, p.stdout, p.stderr


def _evaluate_model_topk(
    model: str,
    dataset: str,
    task: str,
    queries: list[str],
    sql_stem: str,
    csv_fetcher,
    topk: int,
) -> tuple[list[PerQuery], Global]:
    root = _repo_root()
    gt_tables = _gt_table_names(dataset)
    task_l = task.lower()
    base_tmp = root / "orchestrator" / "analysis" / "_tmp_topk" / f"{model}_{dataset.lower()}_{task_l}_{topk}"
    sql_dir = base_tmp / "sql"
    csv_dir = base_tmp / "csv"
    eval_dir = base_tmp / "eval"
    for d in (sql_dir, csv_dir, eval_dir):
        d.mkdir(parents=True, exist_ok=True)

    rows: list[PerQuery] = []
    macro_vals: list[float] = []
    total_pred_mass = 0.0
    total_gold_mass = 0.0
    total_tp_pred = 0.0
    total_tp_gold = 0.0

    for i, sql_text in enumerate(queries, start=1):
        src_csv = csv_fetcher(i)
        if src_csv is None or not src_csv.exists():
            rows.append(PerQuery(task=task_l, model=model, query_idx=i, status="missing_csv", macro_f1=None, note="missing source csv"))
            continue

        try:
            df = pd.read_csv(src_csv)
        except Exception as e:
            rows.append(PerQuery(task=task_l, model=model, query_idx=i, status="bad_csv", macro_f1=None, note=str(e)))
            continue

        df_small = _filter_topk_rows(df, topk)
        if df_small.empty:
            rows.append(PerQuery(task=task_l, model=model, query_idx=i, status="empty_after_filter", macro_f1=None))
            continue

        # evaluator alignment for row-level tasks expects id often
        low_map = {str(c).lower(): str(c) for c in df_small.columns}
        if "id" not in low_map:
            for cand in ("doc_id", "file_id", "filename", "file_name"):
                if cand in low_map:
                    ids = _extract_numeric_series(df_small[low_map[cand]]).fillna(pd.Series(range(1, len(df_small) + 1)))
                    df_small.insert(0, "id", ids.astype(int).astype(str))
                    break
            if "id" not in {str(c).lower(): str(c) for c in df_small.columns}:
                df_small.insert(0, "id", pd.Series(range(1, len(df_small) + 1)).astype(str))

        dst_csv = csv_dir / f"query_{i}.csv"
        df_small.to_csv(dst_csv, index=False)

        aligned_sql = _align_sql_tables_to_gt(sql_text, gt_tables)
        aligned_sql = _normalize_eval_sql_numeric_agg(aligned_sql)
        sql_mod = _with_where_topk(aligned_sql, topk)
        sql_json = sql_dir / f"query_{i}.json"
        sql_json.write_text(json.dumps({"sql": sql_mod}, ensure_ascii=False, indent=2), encoding="utf-8")

        out_q = eval_dir / f"query_{i}"
        out_q.mkdir(parents=True, exist_ok=True)
        rc, out, err = _run_eval(root, dataset, task_l, sql_json, dst_csv, out_q)
        # Persist subprocess logs for easier debugging in case of eval errors.
        (out_q / "stdout.log").write_text(out or "", encoding="utf-8")
        (out_q / "stderr.log").write_text(err or "", encoding="utf-8")
        if rc != 0:
            err_text = (err or "").strip()
            out_text = (out or "").strip()
            note = err_text[-240:] if err_text else out_text[-240:]
            rows.append(PerQuery(task=task_l, model=model, query_idx=i, status="eval_error", macro_f1=None, note=note))
            continue

        acc = _safe_json(out_q / "acc.json")
        if not isinstance(acc, dict):
            rows.append(PerQuery(task=task_l, model=model, query_idx=i, status="no_acc", macro_f1=None))
            continue

        cols = acc.get("columns", {})
        row_info = acc.get("rows", {})
        mass_added = False
        if isinstance(cols, dict) and isinstance(row_info, dict):
            len_pred = row_info.get("len_pred")
            len_gold = row_info.get("len_gold")
            if isinstance(len_pred, (int, float)) and isinstance(len_gold, (int, float)):
                for c_payload in cols.values():
                    if not isinstance(c_payload, dict):
                        continue
                    p = c_payload.get("precision")
                    r = c_payload.get("recall")
                    if isinstance(p, (int, float)) and isinstance(r, (int, float)):
                        total_pred_mass += float(len_pred)
                        total_gold_mass += float(len_gold)
                        total_tp_pred += float(p) * float(len_pred)
                        total_tp_gold += float(r) * float(len_gold)
                        mass_added = True

        # Fallback: some acc.json variants may not expose per-column/row masses
        # consistently; keep pooled metrics defined using unit weight per query.
        if not mass_added:
            mp = acc.get("macro_precision")
            mr = acc.get("macro_recall")
            if isinstance(mp, (int, float)) and isinstance(mr, (int, float)):
                total_pred_mass += 1.0
                total_gold_mass += 1.0
                total_tp_pred += float(mp)
                total_tp_gold += float(mr)

        f1 = acc.get("macro_f1")
        if isinstance(f1, (int, float)):
            macro_vals.append(float(f1))
            rows.append(PerQuery(task=task_l, model=model, query_idx=i, status="ok", macro_f1=float(f1)))
        else:
            rows.append(PerQuery(task=task_l, model=model, query_idx=i, status="no_macro_f1", macro_f1=None))

    completed = sum(1 for r in rows if r.status == "ok")
    pooled_precision = (total_tp_pred / total_pred_mass) if total_pred_mass > 0 else None
    pooled_recall = (total_tp_gold / total_gold_mass) if total_gold_mass > 0 else None
    pooled_f1 = None
    if pooled_precision is not None and pooled_recall is not None:
        denom = pooled_precision + pooled_recall
        pooled_f1 = 0.0 if denom == 0 else (2 * pooled_precision * pooled_recall / denom)
    gl = Global(
        task=task_l,
        model=model,
        completed=completed,
        total=len(queries),
        completion_rate=(completed / len(queries)) if queries else 0.0,
        macro_f1_mean=mean(macro_vals) if macro_vals else None,
        pooled_precision=pooled_precision,
        pooled_recall=pooled_recall,
        pooled_f1=pooled_f1,
        total_pred_mass=total_pred_mass,
        total_gold_mass=total_gold_mass,
        total_tp_pred=total_tp_pred,
        total_tp_gold=total_tp_gold,
    )
    return rows, gl


def _build_overall_rows(global_rows: list[Global], tasks: list[str]) -> list[Global]:
    out: list[Global] = []
    models = sorted({g.model for g in global_rows})
    task_set = {t.lower() for t in tasks}
    for m in models:
        rows = [g for g in global_rows if g.model == m and g.task in task_set]
        if not rows:
            continue
        completed = sum(g.completed for g in rows)
        total = sum(g.total for g in rows)
        completion_rate = (completed / total) if total else 0.0

        tp_pred = sum(g.total_tp_pred for g in rows)
        pred_mass = sum(g.total_pred_mass for g in rows)
        tp_gold = sum(g.total_tp_gold for g in rows)
        gold_mass = sum(g.total_gold_mass for g in rows)

        pooled_precision = (tp_pred / pred_mass) if pred_mass > 0 else None
        pooled_recall = (tp_gold / gold_mass) if gold_mass > 0 else None
        pooled_f1 = None
        if pooled_precision is not None and pooled_recall is not None:
            denom = pooled_precision + pooled_recall
            pooled_f1 = 0.0 if denom == 0 else (2 * pooled_precision * pooled_recall / denom)

        macro_vals = [float(g.macro_f1_mean) for g in rows if isinstance(g.macro_f1_mean, (int, float))]
        macro_f1_mean = mean(macro_vals) if macro_vals else None

        out.append(
            Global(
                task="overall",
                model=m,
                completed=completed,
                total=total,
                completion_rate=completion_rate,
                macro_f1_mean=macro_f1_mean,
                pooled_precision=pooled_precision,
                pooled_recall=pooled_recall,
                pooled_f1=pooled_f1,
                total_pred_mass=pred_mass,
                total_gold_mass=gold_mass,
                total_tp_pred=tp_pred,
                total_tp_gold=tp_gold,
            )
        )
    return out


def _render_task_block(task: str, topk: int, per_query: list[PerQuery], global_rows: list[Global], include_lotus: bool) -> str:
    t = task.lower()
    model_order = ["docetl", "evaporate", "dql"]
    task_per_query = [r for r in per_query if r.task == t]
    task_global_rows = [g for g in global_rows if g.task == t]
    by_model = {m: {r.query_idx: r for r in task_per_query if r.model == m} for m in model_order}
    total_q = max((r.query_idx for r in task_per_query), default=0)

    def fmt(v: float | None) -> str:
        return "n/a" if v is None else f"{v:.4f}"

    # per-query table
    q_rows = []
    for i in range(1, total_q + 1):
        d = by_model["docetl"].get(i)
        e = by_model["evaporate"].get(i)
        q = by_model["dql"].get(i)
        q_rows.append(
            "<tr>"
            f"<td>{i}</td>"
            f"<td>{fmt(d.macro_f1 if d else None)}</td><td>{html.escape(d.status if d else 'n/a')}</td>"
            f"<td>{fmt(e.macro_f1 if e else None)}</td><td>{html.escape(e.status if e else 'n/a')}</td>"
            f"<td>{fmt(q.macro_f1 if q else None)}</td><td>{html.escape(q.status if q else 'n/a')}</td>"
            "</tr>"
        )

    # global table
    g_rows = []
    for g in task_global_rows:
        g_rows.append(
            "<tr>"
            f"<td>{html.escape(g.model)}</td>"
            f"<td>{g.completed}/{g.total}</td>"
            f"<td>{g.completion_rate*100:.1f}%</td>"
            f"<td>{fmt(g.pooled_f1)}</td>"
            f"<td>{fmt(g.macro_f1_mean)}</td>"
            "</tr>"
        )

    # simple inline svg for pooled/global F1 (true total F1)
    width = 800
    height = 280
    pad_l, pad_r, pad_t, pad_b = 70, 20, 30, 40
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    vals = [(g.model, (g.pooled_f1 or 0.0), g.pooled_f1 is not None) for g in task_global_rows]
    n = len(vals)
    bw = min(80, (plot_w / max(n, 1)) * 0.5)
    svg = [f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}">']
    svg.append('<rect width="100%" height="100%" fill="#fff"/>')
    for t in range(0, 11, 2):
        yv = t / 10
        y = pad_t + plot_h * (1 - yv)
        svg.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{pad_l+plot_w}" y2="{y:.1f}" stroke="#e5e7eb"/>')
        svg.append(f'<text x="{pad_l-8}" y="{y+4:.1f}" text-anchor="end" font-size="10">{yv:.1f}</text>')
    colors = {"docetl": "#2E86AB", "evaporate": "#F18F01", "dql": "#C73E1D", "lotus": "#2A9D8F"}
    for i, (m, v, has_value) in enumerate(vals):
        cx = pad_l + (i + 0.5) * (plot_w / max(n, 1))
        h = max(0.0, min(1.0, v)) * plot_h
        x = cx - bw / 2
        y = pad_t + plot_h - h
        opacity = "1.0" if has_value else "0.25"
        svg.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{h:.1f}" fill="{colors.get(m,"#64748b")}" fill-opacity="{opacity}"/>')
        svg.append(f'<text x="{cx:.1f}" y="{height-16}" text-anchor="middle" font-size="11">{html.escape(m)}</text>')
        lbl = f"{v:.3f}" if has_value else "n/a"
        svg.append(f'<text x="{cx:.1f}" y="{y-6:.1f}" text-anchor="middle" font-size="11">{lbl}</text>')
    svg.append(f'<text x="{width/2:.1f}" y="18" text-anchor="middle" font-size="14" font-weight="700">Pooled (True Global) F1 on first {topk} docs ({task.upper()})</text>')
    svg.append("</svg>")
    svg_chart = "\n".join(svg)

    lotus_note = (
        f'<div class="muted">Lotus is included from benchmark CSV global metrics (no per-query top-{topk} artifacts in this pipeline).</div>'
        if include_lotus
        else '<div class="muted">Lotus excluded: only models with local per-query artifacts are shown for fair top-k comparison.</div>'
    )

    return f"""
  <div class="card">
    <h2>{task.upper()} summary</h2>
    <table>
      <thead><tr><th>Model</th><th>Completed</th><th>Completion</th><th>Pooled Global F1</th><th>Macro F1 Mean</th></tr></thead>
      <tbody>{''.join(g_rows)}</tbody>
    </table>
    <p class="muted">Pooled Global F1 aggregates all evaluated cells across all completed queries; this is the true total F1 for this top-{topk} run.</p>
  </div>
  <div class="card">
    <h2>{task.upper()} chart</h2>
    {svg_chart}
  </div>
  <div class="card">
    <h2>{task.upper()} per-query details</h2>
    <table>
      <thead>
        <tr>
          <th>Query</th>
          <th>DocETL F1</th><th>DocETL status</th>
          <th>Evaporate F1</th><th>Evaporate status</th>
          <th>DQL F1</th><th>DQL status</th>
        </tr>
      </thead>
      <tbody>{''.join(q_rows)}</tbody>
    </table>
    <p class="muted">Lotus is shown only in global row for {task.upper()} ({'enabled' if include_lotus else 'disabled'}).</p>
  </div>
"""


def _render_overall_block(topk: int, overall_rows: list[Global], tasks: list[str]) -> str:
    tasks_label = " + ".join(t.upper() for t in tasks)
    def fmt(v: float | None) -> str:
        return "n/a" if v is None else f"{v:.4f}"

    g_rows = []
    for g in overall_rows:
        g_rows.append(
            "<tr>"
            f"<td>{html.escape(g.model)}</td>"
            f"<td>{g.completed}/{g.total}</td>"
            f"<td>{g.completion_rate*100:.1f}%</td>"
            f"<td>{fmt(g.pooled_f1)}</td>"
            f"<td>{fmt(g.macro_f1_mean)}</td>"
            "</tr>"
        )

    width = 800
    height = 280
    pad_l, pad_r, pad_t, pad_b = 70, 20, 30, 40
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    vals = [(g.model, (g.pooled_f1 or 0.0), g.pooled_f1 is not None) for g in overall_rows]
    n = len(vals)
    bw = min(80, (plot_w / max(n, 1)) * 0.5)
    svg = [f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}">']
    svg.append('<rect width="100%" height="100%" fill="#fff"/>')
    for t in range(0, 11, 2):
        yv = t / 10
        y = pad_t + plot_h * (1 - yv)
        svg.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{pad_l+plot_w}" y2="{y:.1f}" stroke="#e5e7eb"/>')
        svg.append(f'<text x="{pad_l-8}" y="{y+4:.1f}" text-anchor="end" font-size="10">{yv:.1f}</text>')
    colors = {"docetl": "#2E86AB", "evaporate": "#F18F01", "dql": "#C73E1D", "lotus": "#2A9D8F"}
    for i, (m, v, has_value) in enumerate(vals):
        cx = pad_l + (i + 0.5) * (plot_w / max(n, 1))
        h = max(0.0, min(1.0, v)) * plot_h
        x = cx - bw / 2
        y = pad_t + plot_h - h
        opacity = "1.0" if has_value else "0.25"
        svg.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{h:.1f}" fill="{colors.get(m,"#64748b")}" fill-opacity="{opacity}"/>')
        svg.append(f'<text x="{cx:.1f}" y="{height-16}" text-anchor="middle" font-size="11">{html.escape(m)}</text>')
        lbl = f"{v:.3f}" if has_value else "n/a"
        svg.append(f'<text x="{cx:.1f}" y="{y-6:.1f}" text-anchor="middle" font-size="11">{lbl}</text>')
    svg.append(f'<text x="{width/2:.1f}" y="18" text-anchor="middle" font-size="14" font-weight="700">Pooled (True Global) F1 on first {topk} docs ({tasks_label})</text>')
    svg.append("</svg>")
    svg_chart = "\n".join(svg)

    return f"""
  <div class="card">
    <h2>Overall summary ({tasks_label})</h2>
    <table>
      <thead><tr><th>Model</th><th>Completed</th><th>Completion</th><th>Pooled Global F1</th><th>Macro F1 Mean</th></tr></thead>
      <tbody>{''.join(g_rows)}</tbody>
    </table>
    <p class="muted">Overall pooled F1 merges {tasks_label} by summing true-positive masses and denominator masses across all selected task families.</p>
  </div>
  <div class="card">
    <h2>Overall chart ({tasks_label})</h2>
    {svg_chart}
  </div>
"""


def _render_html(
    dataset: str,
    tasks: list[str],
    topk: int,
    per_query: list[PerQuery],
    global_rows: list[Global],
    out_path: Path,
    include_lotus: bool,
) -> str:
    overall_rows = _build_overall_rows(global_rows, tasks)
    overall_block = _render_overall_block(topk, overall_rows, tasks) if overall_rows else ""
    task_blocks = "".join(_render_task_block(t, topk, per_query, global_rows, include_lotus) for t in tasks)

    lotus_note = (
        f'<div class="muted">Lotus is included from benchmark CSV as global metric per task (no per-query top-{topk} artifacts in this pipeline).</div>'
        if include_lotus
        else '<div class="muted">Lotus excluded: only models with local per-query artifacts are shown.</div>'
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Top-{topk} Multi-task comparison</title>
<style>
body{{font-family:Segoe UI,Arial,sans-serif;background:#f6f8fb;color:#0f172a;margin:0}}
.wrap{{max-width:1200px;margin:20px auto;padding:0 14px}}
.card{{background:#fff;border:1px solid #dbe3ec;border-radius:12px;padding:14px;margin-bottom:14px}}
h1{{margin:0 0 6px;font-size:24px}} h2{{margin:0 0 10px;font-size:17px}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th,td{{border-bottom:1px solid #e2e8f0;padding:7px;text-align:left;white-space:nowrap}}
th{{background:#f1f5f9}}
.muted{{color:#64748b;font-size:12px}}
</style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <h1>Comparison on first {topk} documents</h1>
    <div class="muted">Dataset: {html.escape(dataset)} | Tasks: {html.escape(', '.join(t.upper() for t in tasks))} | Generated from local artifacts and re-evaluation on top-{topk} rows where CSV outputs exist.</div>
    <div class="muted">Output file: {html.escape(str(out_path))}</div>
    {lotus_note}
  </div>
  {overall_block}
  {task_blocks}
</div>
</body>
</html>"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare tasks on first-k docs across DocETL/Evaporate/DQL")
    parser.add_argument("--dataset", default="Finan")
    parser.add_argument("--tasks", default="select,agg,mixed", help="Comma-separated tasks, e.g. select,agg,mixed")
    parser.add_argument("--topk", type=int, default=6)
    parser.add_argument(
        "--include-lotus-from-benchmark",
        action="store_true",
        help="Include Lotus global metrics from benchmark_results.csv (no per-query top-k artifacts).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Single HTML output path (default: orchestrator/analysis/select_top6_compare.html)",
    )
    args = parser.parse_args()

    root = _repo_root()
    tasks = [t.strip().lower() for t in args.tasks.split(",") if t.strip()]
    if not tasks:
        raise SystemExit("No tasks provided")

    per_query: list[PerQuery] = []
    global_rows: list[Global] = []

    for task in tasks:
        sql_path = _resolve_task_sql_file(root, args.dataset, task)
        queries = _split_sql_file(sql_path)
        if not queries:
            raise SystemExit(f"No queries found in {sql_path}")
        sql_stem = sql_path.stem

        def docetl_csv(i: int, _task=task, _stem=sql_stem) -> Path:
            return root / "systems" / "DocETL" / "outputs" / args.dataset.lower() / "csv" / f"{_task}_{_stem}_{i}.csv"

        def evaporate_csv(i: int, _task=task, _stem=sql_stem) -> Path:
            return root / "systems" / "Evaporate" / "outputs" / args.dataset.lower() / "csv" / f"{_task}_{_stem}_{i}.csv"

        def dql_csv(i: int, _task=task, _stem=sql_stem) -> Path:
            flat = root / "systems" / "DQL" / "outputs" / args.dataset.lower() / "csv" / f"{_task}_{_stem}_{i}.csv"
            if flat.exists():
                return flat
            return _resolve_dql_task_query_dir(root, args.dataset, _task, i) / "results.csv"

        rows, g = _evaluate_model_topk("docetl", args.dataset, task, queries, sql_stem, docetl_csv, args.topk)
        per_query.extend(rows)
        global_rows.append(g)

        rows, g = _evaluate_model_topk("evaporate", args.dataset, task, queries, sql_stem, evaporate_csv, args.topk)
        per_query.extend(rows)
        global_rows.append(g)

        rows, g = _evaluate_model_topk("dql", args.dataset, task, queries, sql_stem, dql_csv, args.topk)
        per_query.extend(rows)
        global_rows.append(g)

        if args.include_lotus_from_benchmark:
            lotus = _collect_lotus_from_benchmark_csv(args.dataset, task, len(queries))
            if lotus is not None:
                global_rows.append(lotus)

    out = Path(args.output) if args.output else (root / "orchestrator" / "analysis" / "select_top6_compare.html")
    out.parent.mkdir(parents=True, exist_ok=True)
    report_html = _render_html(
        args.dataset,
        tasks,
        args.topk,
        per_query,
        global_rows,
        out,
        include_lotus=args.include_lotus_from_benchmark,
    )
    out.write_text(report_html, encoding="utf-8")

    print(f"Single-file report generated: {out}")
    for g in global_rows:
        print(
            f"{g.task}/{g.model}: completed={g.completed}/{g.total}, "
            f"completion={g.completion_rate:.1%}, pooled_f1={g.pooled_f1}, macro_f1_mean={g.macro_f1_mean}"
        )
    # Print first eval_error note per model for quick diagnosis.
    for task in tasks:
        for model in ("docetl", "evaporate", "dql"):
            first_err = next((r for r in per_query if r.task == task and r.model == model and r.status == "eval_error"), None)
            if first_err and first_err.note:
                print(f"{task}/{model} first_eval_error: {first_err.note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
