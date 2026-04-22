from __future__ import annotations

import argparse
import csv
import html
import json
import re
from dataclasses import dataclass
from pathlib import Path
from statistics import mean


@dataclass
class QueryMetric:
    model: str
    query_index: int
    has_acc: bool
    macro_f1: float | None
    n_columns: int


@dataclass
class GlobalMetric:
    model: str
    expected_queries: int
    completed_queries: int
    completion_rate: float
    query_macro_mean: float | None
    global_column_mean: float | None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _safe_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _count_select_queries(query_sql_path: Path) -> int:
    if not query_sql_path.exists():
        return 0
    text = query_sql_path.read_text(encoding="utf-8")
    chunks = [c.strip() for c in text.split(";")]
    return sum(1 for c in chunks if c)


def _query_index_from_name(name: str, fallback: int) -> int:
    m = re.search(r"_(\d+)$", name)
    if m:
        return int(m.group(1))
    return fallback


def _iter_select_eval_dirs(base_eval_dir: Path) -> list[tuple[int, Path]]:
    dirs = sorted(
        [p for p in base_eval_dir.iterdir() if p.is_dir() and p.name.startswith("select_select_queries_")]
    )
    out: list[tuple[int, Path]] = []
    for idx, p in enumerate(dirs, start=1):
        out.append((_query_index_from_name(p.name, idx), p))
    return out


def _collect_from_eval_dirs(model: str, base_eval_dir: Path, expected_queries: int) -> tuple[list[QueryMetric], GlobalMetric]:
    query_metrics: list[QueryMetric] = []
    macro_vals: list[float] = []
    all_col_f1_vals: list[float] = []

    for q_idx, q_dir in _iter_select_eval_dirs(base_eval_dir):
        acc_path = q_dir / "acc.json"
        payload = _safe_json(acc_path) if acc_path.exists() else None
        if payload is None:
            query_metrics.append(
                QueryMetric(model=model, query_index=q_idx, has_acc=False, macro_f1=None, n_columns=0)
            )
            continue

        macro_f1 = payload.get("macro_f1")
        columns = payload.get("columns", {}) if isinstance(payload.get("columns", {}), dict) else {}
        n_cols = len(columns)
        if isinstance(macro_f1, (int, float)):
            macro_vals.append(float(macro_f1))
        for _, c_payload in columns.items():
            if isinstance(c_payload, dict):
                f1 = c_payload.get("f1")
                if isinstance(f1, (int, float)):
                    all_col_f1_vals.append(float(f1))

        query_metrics.append(
            QueryMetric(
                model=model,
                query_index=q_idx,
                has_acc=isinstance(macro_f1, (int, float)),
                macro_f1=float(macro_f1) if isinstance(macro_f1, (int, float)) else None,
                n_columns=n_cols,
            )
        )

    existing = {m.query_index for m in query_metrics}
    for i in range(1, expected_queries + 1):
        if i not in existing:
            query_metrics.append(QueryMetric(model=model, query_index=i, has_acc=False, macro_f1=None, n_columns=0))

    query_metrics.sort(key=lambda x: x.query_index)
    completed = sum(1 for m in query_metrics if m.has_acc)
    gm = GlobalMetric(
        model=model,
        expected_queries=expected_queries,
        completed_queries=completed,
        completion_rate=(completed / expected_queries) if expected_queries else 0.0,
        query_macro_mean=mean(macro_vals) if macro_vals else None,
        global_column_mean=mean(all_col_f1_vals) if all_col_f1_vals else None,
    )
    return query_metrics, gm


def _collect_dql(
    model: str,
    dql_flat_csv_root: Path,
    dql_eval_root: Path,
    expected_queries: int,
) -> tuple[list[QueryMetric], GlobalMetric]:
    query_metrics: list[QueryMetric] = []
    macro_vals: list[float] = []
    all_col_f1_vals: list[float] = []
    produced_outputs = 0

    for i in range(1, expected_queries + 1):
        csv_name = f"select_select_queries_{i}.csv"
        has_output = (dql_flat_csv_root / csv_name).exists()
        if has_output:
            produced_outputs += 1
        acc_path = dql_eval_root / f"select_select_queries_{i}" / "acc.json"
        payload = _safe_json(acc_path) if acc_path.exists() else None

        macro_f1 = None
        n_cols = 0
        has_acc = False
        if isinstance(payload, dict):
            mf = payload.get("macro_f1")
            cols = payload.get("columns", {})
            if has_output and isinstance(mf, (int, float)):
                macro_f1 = float(mf)
                has_acc = True
                macro_vals.append(float(mf))
            if has_output and isinstance(cols, dict):
                n_cols = len(cols)
                for _, c_payload in cols.items():
                    if isinstance(c_payload, dict):
                        f1 = c_payload.get("f1")
                        if isinstance(f1, (int, float)):
                            all_col_f1_vals.append(float(f1))

        query_metrics.append(
            QueryMetric(
                model=model,
                query_index=i,
                has_acc=has_acc,
                macro_f1=macro_f1,
                n_columns=n_cols if n_cols else (1 if has_output else 0),
            )
        )

    completed = produced_outputs
    gm = GlobalMetric(
        model=model,
        expected_queries=expected_queries,
        completed_queries=completed,
        completion_rate=(completed / expected_queries) if expected_queries else 0.0,
        query_macro_mean=mean(macro_vals) if macro_vals else None,
        global_column_mean=mean(all_col_f1_vals) if all_col_f1_vals else None,
    )
    return query_metrics, gm


def _collect_lotus_from_benchmark_csv(dataset: str, expected_queries: int) -> GlobalMetric | None:
    """
    Lotus metrics are sourced from orchestrator/analysis/benchmark_results.csv
    (as provided by collaborator), because per-query lotus artifacts are not
    wired into this report pipeline yet.
    """
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
            if str(row.get("query_type", "")).strip().lower() != "select":
                continue
            macro_raw = str(row.get("macro_f1_mean", "")).strip()
            if not macro_raw:
                continue
            # prefer newest record (ISO timestamp sorts lexicographically)
            if best_row is None or str(row.get("created_at_utc", "")) > str(best_row.get("created_at_utc", "")):
                best_row = row

    if best_row is None:
        return None

    try:
        macro = float(str(best_row.get("macro_f1_mean", "")).strip())
    except Exception:
        return None

    status = str(best_row.get("job_status", "")).strip().lower()
    # If status is ok we assume SELECT run completed; otherwise unknown/partial.
    if status == "ok":
        completed = expected_queries
        completion = 1.0
    else:
        completed = 0
        completion = 0.0

    return GlobalMetric(
        model="lotus",
        expected_queries=expected_queries,
        completed_queries=completed,
        completion_rate=completion,
        query_macro_mean=macro,
        global_column_mean=None,
    )


def _resolve_dql_select_csv_root(root: Path, dataset: str) -> Path:
    candidates = [
        root / "systems" / "DQL" / "outputs" / dataset.lower() / "csv",
        root / "systems" / "DQL" / "outputs" / dataset.lower() / "select" / "csv",
        root / "systems" / "DQL" / "results" / dataset / "select" / "csv",
    ]
    for c in candidates:
        if c.exists():
            return c
    return candidates[0]


def _fmt(v: float | None, pct: bool = False) -> str:
    if v is None:
        return "n/a"
    if pct:
        return f"{v*100:.1f}%"
    return f"{v:.4f}"


def _svg_grouped_per_query(per_query: list[QueryMetric], expected_queries: int) -> str:
    width = 1200
    height = 360
    pad_l = 60
    pad_r = 30
    pad_t = 35
    pad_b = 45
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    models = ["docetl", "evaporate", "dql"]
    colors = {"docetl": "#2E86AB", "evaporate": "#F18F01", "dql": "#C73E1D"}

    # index query metrics
    idx: dict[tuple[str, int], QueryMetric] = {(m.model, m.query_index): m for m in per_query}
    group_w = plot_w / max(expected_queries, 1)
    bar_w = max(3, group_w * 0.22)
    centers = [pad_l + group_w * (i - 0.5) for i in range(1, expected_queries + 1)]

    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="360" role="img" aria-label="Per-query macro F1 chart">',
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
    ]

    # y grid and labels
    for t in range(0, 11):
        yv = t / 10
        y = pad_t + plot_h * (1 - yv)
        stroke = "#e5e7eb" if t not in (0, 10) else "#9ca3af"
        parts.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{pad_l+plot_w}" y2="{y:.1f}" stroke="{stroke}" stroke-width="1"/>')
        if t % 2 == 0:
            parts.append(f'<text x="{pad_l-8}" y="{y+4:.1f}" text-anchor="end" font-size="11" fill="#374151">{yv:.1f}</text>')

    # bars
    for qi in range(1, expected_queries + 1):
        cx = centers[qi - 1]
        for mi, model in enumerate(models):
            metric = idx.get((model, qi))
            v = metric.macro_f1 if metric and metric.macro_f1 is not None else 0.0
            h = max(0, min(1, v)) * plot_h
            x = cx + (mi - 1) * (bar_w + 1) - bar_w / 2
            y = pad_t + (plot_h - h)
            alpha = "1.0" if metric and metric.has_acc else "0.28"
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" '
                f'fill="{colors[model]}" fill-opacity="{alpha}"/>'
            )

    # x labels
    for qi in range(1, expected_queries + 1):
        cx = centers[qi - 1]
        parts.append(f'<text x="{cx:.1f}" y="{height-18}" text-anchor="middle" font-size="10" fill="#374151">{qi}</text>')

    # title
    parts.append('<text x="600" y="20" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">Per-query Macro F1 (SELECT)</text>')
    parts.append('<text x="600" y="338" text-anchor="middle" font-size="11" fill="#374151">Query Index</text>')

    # legend
    lx = width - 300
    ly = 28
    for i, m in enumerate(models):
        yy = ly + i * 18
        parts.append(f'<rect x="{lx}" y="{yy-9}" width="12" height="12" fill="{colors[m]}"/>')
        parts.append(f'<text x="{lx+18}" y="{yy+1}" font-size="11" fill="#111827">{m}</text>')
    parts.append('<text x="80" y="20" font-size="11" fill="#6b7280">faded bars = missing evaluation</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def _svg_global(global_rows: list[GlobalMetric]) -> str:
    width = 900
    height = 340
    pad_l = 70
    pad_r = 30
    pad_t = 35
    pad_b = 48
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    metrics = ["query_macro_mean", "global_column_mean", "completion_rate"]
    metric_colors = {
        "query_macro_mean": "#2E86AB",
        "global_column_mean": "#6A4C93",
        "completion_rate": "#2A9D8F",
    }
    metric_labels = {
        "query_macro_mean": "macro_f1_mean (queries)",
        "global_column_mean": "column_f1_mean (all columns)",
        "completion_rate": "completion_rate",
    }

    n_models = len(global_rows)
    group_w = plot_w / max(n_models, 1)
    bar_w = min(36, group_w * 0.22)
    centers = [pad_l + group_w * (i + 0.5) for i in range(n_models)]

    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="340" role="img" aria-label="Global metrics chart">',
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
    ]

    for t in range(0, 11):
        yv = t / 10
        y = pad_t + plot_h * (1 - yv)
        stroke = "#e5e7eb" if t not in (0, 10) else "#9ca3af"
        parts.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{pad_l+plot_w}" y2="{y:.1f}" stroke="{stroke}" stroke-width="1"/>')
        if t % 2 == 0:
            parts.append(f'<text x="{pad_l-8}" y="{y+4:.1f}" text-anchor="end" font-size="11" fill="#374151">{yv:.1f}</text>')

    for i, row in enumerate(global_rows):
        cx = centers[i]
        vals = {
            "query_macro_mean": row.query_macro_mean if row.query_macro_mean is not None else 0.0,
            "global_column_mean": row.global_column_mean if row.global_column_mean is not None else 0.0,
            "completion_rate": row.completion_rate,
        }
        for mi, m in enumerate(metrics):
            v = max(0.0, min(1.0, vals[m]))
            h = v * plot_h
            x = cx + (mi - 1) * (bar_w + 4) - bar_w / 2
            y = pad_t + (plot_h - h)
            alpha = "1.0" if not (m == "global_column_mean" and row.global_column_mean is None) else "0.25"
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" '
                f'fill="{metric_colors[m]}" fill-opacity="{alpha}"/>'
            )

        parts.append(f'<text x="{cx:.1f}" y="{height-20}" text-anchor="middle" font-size="11" fill="#374151">{row.model}</text>')

    parts.append('<text x="450" y="20" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">Global Metrics (SELECT)</text>')
    parts.append('<text x="450" y="326" text-anchor="middle" font-size="11" fill="#374151">Model</text>')

    lx = width - 260
    ly = 26
    for i, m in enumerate(metrics):
        yy = ly + i * 18
        parts.append(f'<rect x="{lx}" y="{yy-9}" width="12" height="12" fill="{metric_colors[m]}"/>')
        parts.append(f'<text x="{lx+18}" y="{yy+1}" font-size="11" fill="#111827">{metric_labels[m]}</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def _table_global(global_rows: list[GlobalMetric]) -> str:
    rows = []
    for g in global_rows:
        rows.append(
            "<tr>"
            f"<td>{html.escape(g.model)}</td>"
            f"<td>{g.completed_queries}/{g.expected_queries}</td>"
            f"<td>{_fmt(g.completion_rate, pct=True)}</td>"
            f"<td>{_fmt(g.query_macro_mean)}</td>"
            f"<td>{_fmt(g.global_column_mean)}</td>"
            "</tr>"
        )
    return (
        "<table>"
        "<thead><tr>"
        "<th>Model</th><th>Coverage</th><th>Completion Rate</th><th>macro_f1_mean (queries)</th><th>column_f1_mean (all columns)</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _table_per_query(per_query: list[QueryMetric], expected_queries: int) -> str:
    by_model = {m: {r.query_index: r for r in per_query if r.model == m} for m in ["docetl", "evaporate", "dql"]}
    rows = []
    for i in range(1, expected_queries + 1):
        d = by_model["docetl"].get(i)
        e = by_model["evaporate"].get(i)
        q = by_model["dql"].get(i)
        rows.append(
            "<tr>"
            f"<td>{i}</td>"
            f"<td>{_fmt(d.macro_f1 if d else None)}</td>"
            f"<td>{_fmt(e.macro_f1 if e else None)}</td>"
            f"<td>{_fmt(q.macro_f1 if q else None)}</td>"
            "</tr>"
        )
    return (
        "<table>"
        "<thead><tr>"
        "<th>Query</th><th>DocETL macro_f1</th><th>Evaporate macro_f1</th><th>DQL macro_f1</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _render_html(dataset: str, expected_queries: int, global_rows: list[GlobalMetric], per_query: list[QueryMetric]) -> str:
    svg_per_query = _svg_grouped_per_query(per_query, expected_queries)
    svg_global = _svg_global(global_rows)
    tbl_global = _table_global(global_rows)
    tbl_per_query = _table_per_query(per_query, expected_queries)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SELECT Evaluation Report - {html.escape(dataset)}</title>
  <style>
    :root {{
      --bg: #f7f8fb;
      --panel: #ffffff;
      --ink: #0f172a;
      --muted: #475569;
      --line: #dbe2ea;
      --accent: #0ea5e9;
    }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
      background: radial-gradient(1200px 500px at 10% -10%, #d9f1ff 0%, var(--bg) 60%), var(--bg);
      color: var(--ink);
    }}
    .wrap {{
      max-width: 1200px;
      margin: 24px auto;
      padding: 0 16px 24px;
    }}
    .hero {{
      background: linear-gradient(120deg, #0b4f6c 0%, #145a7a 40%, #1b7ea8 100%);
      color: #fff;
      border-radius: 14px;
      padding: 18px 20px;
      box-shadow: 0 8px 20px rgba(11,79,108,.25);
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 26px;
      letter-spacing: .2px;
    }}
    .hero p {{ margin: 2px 0; color: #dbefff; }}
    .grid {{
      margin-top: 16px;
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px 14px 10px;
      box-shadow: 0 6px 14px rgba(15,23,42,.06);
    }}
    .card h2 {{
      margin: 0 0 10px;
      font-size: 17px;
      color: #0b3b52;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 8px 7px;
      text-align: left;
      white-space: nowrap;
    }}
    th {{
      background: #f2f7fb;
      color: #12364a;
      font-weight: 700;
    }}
    .muted {{ color: var(--muted); font-size: 12px; }}
    .foot {{
      margin-top: 6px;
      color: #64748b;
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>SELECT Evaluation Report</h1>
      <p><strong>Dataset:</strong> {html.escape(dataset)} | <strong>Scope:</strong> DocETL, Evaporate, DQL, Lotus | <strong>Queries:</strong> {expected_queries}</p>
      <p>Global metrics are aligned to evaluation semantics: <em>macro_f1_mean (queries)</em> and <em>column_f1_mean (all columns)</em>.</p>
      <p><strong>How to read them:</strong> <em>macro_f1_mean (queries)</em> gives equal weight to each query (query-level view), while <em>column_f1_mean (all columns)</em> gives equal weight to each extracted column across all queries (attribute-level view).</p>
      <p><strong>Interpretation:</strong> if query-level score is high but column-level score is lower, the system is usually good on average per query but struggles on specific fields; if column-level is higher, some larger queries may be dragging down the query-average despite good per-field extraction.</p>
    </section>

    <section class="grid">
      <article class="card">
        <h2>Global Metrics Summary</h2>
        {tbl_global}
        <p class="foot">If a metric is "n/a", evaluation artifacts were not available for that model/query set.</p>
      </article>

      <article class="card">
        <h2>Global Metrics Chart</h2>
        {svg_global}
      </article>

      <article class="card">
        <h2>Per-query Macro F1 Chart</h2>
        {svg_per_query}
      </article>

      <article class="card">
        <h2>Per-query Metrics Table</h2>
        {tbl_per_query}
        <p class="muted">Values are per-query <code>macro_f1</code> from <code>acc.json</code>. Lotus is included only in global chart/table from benchmark CSV metrics.</p>
      </article>
    </section>
  </div>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Build one-file human-readable SELECT evaluation report")
    parser.add_argument("--dataset", default="Finan", help="Dataset name (default: Finan)")
    parser.add_argument(
        "--output",
        default=None,
        help="Output HTML path (default: orchestrator/analysis/select_report.html)",
    )
    args = parser.parse_args()

    root = _repo_root()
    out_path = Path(args.output) if args.output else root / "orchestrator" / "analysis" / "select_report.html"
    expected = _count_select_queries(root / "Query" / args.dataset / "Select" / "select_queries.sql")
    if expected == 0:
        raise SystemExit("No SELECT queries found.")

    all_query_rows: list[QueryMetric] = []
    global_rows: list[GlobalMetric] = []

    doc_eval = root / "systems" / "DocETL" / "outputs" / args.dataset.lower() / "evaluation"
    q_rows, g = _collect_from_eval_dirs("docetl", doc_eval, expected)
    all_query_rows.extend(q_rows)
    global_rows.append(g)

    eva_eval = root / "systems" / "Evaporate" / "outputs" / args.dataset.lower() / "evaluation"
    q_rows, g = _collect_from_eval_dirs("evaporate", eva_eval, expected)
    all_query_rows.extend(q_rows)
    global_rows.append(g)

    dql_csv = _resolve_dql_select_csv_root(root, args.dataset)
    dql_eval = root / "systems" / "DQL" / "outputs" / args.dataset.lower() / "evaluation"
    q_rows, g = _collect_dql("dql", dql_csv, dql_eval, expected)
    all_query_rows.extend(q_rows)
    global_rows.append(g)

    lotus_global = _collect_lotus_from_benchmark_csv(args.dataset, expected)
    if lotus_global is not None:
        global_rows.append(lotus_global)

    html_text = _render_html(args.dataset, expected, global_rows, all_query_rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_text, encoding="utf-8")

    print("One-file report generated:")
    print(f"- {out_path}")
    for g in global_rows:
        print(
            f"- {g.model}: completed={g.completed_queries}/{g.expected_queries}, "
            f"completion={g.completion_rate:.2%}, query_macro_mean={g.query_macro_mean}, "
            f"global_column_mean={g.global_column_mean}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
