from __future__ import annotations

import argparse
import html
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean


KNOWN_TASKS = ("select", "agg", "filter", "mixed", "join")
MODEL_ORDER = ("docetl", "dql", "evaporate", "lotus", "quest")
COLORS = {
    "docetl": "#2E86AB",
    "dql": "#C73E1D",
    "evaporate": "#F18F01",
    "lotus": "#2A9D8F",
    "quest": "#6D5DF6",
}


@dataclass
class Metric:
    model: str
    task: str
    query_index: int
    name: str
    acc_path: Path
    macro_precision: float | None
    macro_recall: float | None
    macro_f1: float | None
    column_f1_mean: float | None
    n_columns: int
    len_gold: int | None
    len_pred: int | None
    matched_rows: int | None

    @property
    def is_empty_empty(self) -> bool:
        return self.len_gold == 0 and self.len_pred == 0

    @property
    def adjusted_macro_f1(self) -> float | None:
        if self.is_empty_empty:
            return 1.0
        return self.macro_f1

    @property
    def adjusted_column_f1_mean(self) -> float | None:
        if self.is_empty_empty:
            return 1.0
        return self.column_f1_mean


@dataclass
class Summary:
    model: str
    task: str
    completed: int
    expected: int
    completion_rate: float
    macro_f1_mean: float | None
    adjusted_macro_f1_mean: float | None
    column_f1_mean: float | None
    adjusted_column_f1_mean: float | None
    macro_precision_mean: float | None
    macro_recall_mean: float | None
    empty_empty_count: int
    matched_rows: int
    len_pred: int
    len_gold: int


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def safe_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def as_float(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def as_int(value) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def split_sql_count(path: Path) -> int:
    if not path.exists():
        return 0
    chunks = [c.strip() for c in path.read_text(encoding="utf-8").split(";")]
    return sum(1 for c in chunks if c)


def query_counts(dataset: str) -> dict[str, int]:
    q_root = repo_root() / "Query" / dataset
    out: dict[str, int] = {}
    if not q_root.exists():
        return out
    for task in KNOWN_TASKS:
        task_dir = q_root / task.capitalize()
        if not task_dir.exists():
            continue
        out[task] = sum(split_sql_count(p) for p in task_dir.glob("*.sql"))
    return out


def model_sort_key(model: str) -> tuple[int, str]:
    low = model.lower()
    if low in MODEL_ORDER:
        return (MODEL_ORDER.index(low), low)
    return (len(MODEL_ORDER), low)


def task_from_name(name: str) -> str | None:
    low = name.lower()
    for task in KNOWN_TASKS:
        if low == task or low.startswith(f"{task}_"):
            return task
    return None


def query_index_from_name(name: str, fallback: int) -> int:
    match = re.search(r"(?:query_|_)(\d+)$", name.lower())
    return int(match.group(1)) if match else fallback


def metric_from_acc(model: str, task: str, query_index: int, name: str, acc_path: Path) -> Metric | None:
    payload = safe_json(acc_path)
    if not isinstance(payload, dict):
        return None

    columns = payload.get("columns", {})
    col_f1_vals: list[float] = []
    if isinstance(columns, dict):
        for col_payload in columns.values():
            if isinstance(col_payload, dict):
                f1 = as_float(col_payload.get("f1"))
                if f1 is not None:
                    col_f1_vals.append(f1)

    rows = payload.get("rows", {})
    if not isinstance(rows, dict):
        rows = {}

    return Metric(
        model=model.lower(),
        task=task.lower(),
        query_index=query_index,
        name=name,
        acc_path=acc_path,
        macro_precision=as_float(payload.get("macro_precision")),
        macro_recall=as_float(payload.get("macro_recall")),
        macro_f1=as_float(payload.get("macro_f1")),
        column_f1_mean=mean(col_f1_vals) if col_f1_vals else None,
        n_columns=len(columns) if isinstance(columns, dict) else 0,
        len_gold=as_int(rows.get("len_gold")),
        len_pred=as_int(rows.get("len_pred")),
        matched_rows=as_int(rows.get("matched_rows")),
    )


def has_acc_artifacts(path: Path) -> bool:
    return path.exists() and any(path.glob("*/acc.json"))


def evaluation_roots(model: str, output_root: Path, variant: str) -> list[Path]:
    if variant == "llm":
        candidates = [
            output_root / f"outputs_llm_{model}",
            output_root / f"evaluation_llm_{model}",
        ]
        # Backward-compatible names from earlier ad-hoc LLM evaluation runs.
        candidates.extend(sorted(output_root.glob("evaluation_llm_*")))
        seen: set[Path] = set()
        roots: list[Path] = []
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            if has_acc_artifacts(candidate):
                roots.append(candidate)
                break
        return roots
    return [output_root / "evaluation"]


def collect_flat_outputs(model: str, output_root: Path, variant: str = "standard") -> list[Metric]:
    metrics: list[Metric] = []
    for eval_root in evaluation_roots(model, output_root, variant):
        if not eval_root.exists():
            continue
        for fallback, q_dir in enumerate(sorted(p for p in eval_root.iterdir() if p.is_dir()), start=1):
            task = task_from_name(q_dir.name)
            if task is None:
                continue
            acc_path = q_dir / "acc.json"
            if not acc_path.exists():
                continue
            metric = metric_from_acc(model, task, query_index_from_name(q_dir.name, fallback), q_dir.name, acc_path)
            if metric is not None:
                metrics.append(metric)
    return metrics


def collect_lotus_outputs(output_root: Path) -> list[Metric]:
    metrics: list[Metric] = []
    csv_root = output_root / "csv"
    if not csv_root.exists():
        return metrics
    query_dirs = sorted(
        [p for p in csv_root.iterdir() if p.is_dir() and re.match(r"query_\d+$", p.name.lower())],
        key=lambda p: query_index_from_name(p.name, 0),
    )
    for fallback, q_dir in enumerate(query_dirs, start=1):
        acc_path = q_dir / "acc_result" / "acc.json"
        if not acc_path.exists():
            continue
        metric = metric_from_acc("lotus", "legacy", query_index_from_name(q_dir.name, fallback), q_dir.name, acc_path)
        if metric is not None:
            metrics.append(metric)
    return metrics


def discover_metrics(dataset: str, variant: str = "standard") -> list[Metric]:
    root = repo_root()
    dataset_key = dataset.lower()
    metrics: list[Metric] = []
    systems_root = root / "systems"
    for system_dir in sorted([p for p in systems_root.iterdir() if p.is_dir()], key=lambda p: model_sort_key(p.name)):
        output_root = system_dir / "outputs" / dataset_key
        if not output_root.exists():
            continue
        model = system_dir.name.lower()
        if model == "lotus" and variant == "standard":
            metrics.extend(collect_lotus_outputs(output_root))
        metrics.extend(collect_flat_outputs(model, output_root, variant=variant))
    return metrics


def summarize(metrics: list[Metric], model: str, task: str, expected: int) -> Summary:
    macro_f1_vals = [m.macro_f1 for m in metrics if m.macro_f1 is not None]
    adjusted_macro_f1_vals = [m.adjusted_macro_f1 for m in metrics if m.adjusted_macro_f1 is not None]
    col_f1_vals = [m.column_f1_mean for m in metrics if m.column_f1_mean is not None]
    adjusted_col_f1_vals = [m.adjusted_column_f1_mean for m in metrics if m.adjusted_column_f1_mean is not None]
    prec_vals = [m.macro_precision for m in metrics if m.macro_precision is not None]
    rec_vals = [m.macro_recall for m in metrics if m.macro_recall is not None]
    empty_empty_count = sum(1 for m in metrics if m.is_empty_empty)
    matched = sum(m.matched_rows or 0 for m in metrics)
    len_pred = sum(m.len_pred or 0 for m in metrics)
    len_gold = sum(m.len_gold or 0 for m in metrics)
    return Summary(
        model=model,
        task=task,
        completed=len(metrics),
        expected=expected,
        completion_rate=(len(metrics) / expected) if expected else 0.0,
        macro_f1_mean=mean(macro_f1_vals) if macro_f1_vals else None,
        adjusted_macro_f1_mean=mean(adjusted_macro_f1_vals) if adjusted_macro_f1_vals else None,
        column_f1_mean=mean(col_f1_vals) if col_f1_vals else None,
        adjusted_column_f1_mean=mean(adjusted_col_f1_vals) if adjusted_col_f1_vals else None,
        macro_precision_mean=mean(prec_vals) if prec_vals else None,
        macro_recall_mean=mean(rec_vals) if rec_vals else None,
        empty_empty_count=empty_empty_count,
        matched_rows=matched,
        len_pred=len_pred,
        len_gold=len_gold,
    )


def build_summaries(metrics: list[Metric], expected_by_task: dict[str, int]) -> tuple[list[Summary], list[Summary]]:
    known_total = sum(expected_by_task.values())
    by_model_task: dict[tuple[str, str], list[Metric]] = {}
    by_model: dict[str, list[Metric]] = {}
    for metric in metrics:
        by_model_task.setdefault((metric.model, metric.task), []).append(metric)
        by_model.setdefault(metric.model, []).append(metric)

    task_summaries: list[Summary] = []
    for (model, task), group in sorted(by_model_task.items(), key=lambda item: (item[0][1], model_sort_key(item[0][0]))):
        if task == "legacy":
            continue
        expected = expected_by_task.get(task, len(group))
        task_summaries.append(summarize(group, model, task, expected))

    global_summaries: list[Summary] = []
    for model, group in sorted(by_model.items(), key=lambda item: model_sort_key(item[0])):
        tasks = sorted({m.task for m in group})
        if "legacy" in tasks and known_total:
            expected = known_total
        else:
            expected = sum(expected_by_task.get(t, sum(1 for m in group if m.task == t)) for t in tasks)
        global_summaries.append(summarize(group, model, "all detected", expected))
    return global_summaries, task_summaries


def fmt_num(value: float | None, digits: int = 4) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def esc(value) -> str:
    return html.escape(str(value))


def render_table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body = "\n".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def render_bar_chart(title: str, rows: list[tuple[str, float | None, str]]) -> str:
    width, height = 860, 300
    left, right, top, bottom = 68, 20, 34, 52
    plot_w = width - left - right
    plot_h = height - top - bottom
    usable = [(label, value, color) for label, value, color in rows]
    n = max(1, len(usable))
    slot = plot_w / n
    bar_w = min(72, slot * 0.42)
    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" role="img" aria-label="{esc(title)}">',
        '<rect width="100%" height="100%" fill="#fff"/>',
    ]
    for i in range(6):
        y = top + plot_h - (plot_h * i / 5)
        val = i / 5
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" stroke="#e5e7eb"/>')
        parts.append(f'<text x="{left-8}" y="{y+4:.1f}" text-anchor="end" font-size="10" fill="#475569">{val:.1f}</text>')
    for idx, (label, value, color) in enumerate(usable):
        x = left + idx * slot + (slot - bar_w) / 2
        if value is None:
            bar_h = 0
            y = top + plot_h
            text = "n/a"
            opacity = 0.25
        else:
            clamped = max(0.0, min(1.0, value))
            bar_h = plot_h * clamped
            y = top + plot_h - bar_h
            text = f"{value:.3f}"
            opacity = 1.0
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" fill="{color}" fill-opacity="{opacity}"/>')
        parts.append(f'<text x="{x+bar_w/2:.1f}" y="{y-6:.1f}" text-anchor="middle" font-size="11" fill="#0f172a">{esc(text)}</text>')
        parts.append(f'<text x="{x+bar_w/2:.1f}" y="{height-22}" text-anchor="middle" font-size="11" fill="#334155">{esc(label)}</text>')
    parts.append(f'<text x="{width/2}" y="20" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">{esc(title)}</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def render_grouped_bar_chart(title: str, rows: list[tuple[str, float | None, float | None, str]]) -> str:
    width, height = 860, 320
    left, right, top, bottom = 68, 20, 34, 62
    plot_w = width - left - right
    plot_h = height - top - bottom
    n = max(1, len(rows))
    slot = plot_w / n
    bar_w = min(32, slot * 0.18)
    gap = 6
    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" role="img" aria-label="{esc(title)}">',
        '<rect width="100%" height="100%" fill="#fff"/>',
    ]
    for i in range(6):
        y = top + plot_h - (plot_h * i / 5)
        val = i / 5
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" stroke="#e5e7eb"/>')
        parts.append(f'<text x="{left-8}" y="{y+4:.1f}" text-anchor="end" font-size="10" fill="#475569">{val:.1f}</text>')
    for idx, (label, raw, adjusted, color) in enumerate(rows):
        group_w = bar_w * 2 + gap
        x0 = left + idx * slot + (slot - group_w) / 2
        for offset, value, fill, tag in (
            (0, raw, "#94a3b8", "raw"),
            (bar_w + gap, adjusted, color, "adj"),
        ):
            x = x0 + offset
            if value is None:
                bar_h = 0
                y = top + plot_h
                text = "n/a"
                opacity = 0.25
            else:
                clamped = max(0.0, min(1.0, value))
                bar_h = plot_h * clamped
                y = top + plot_h - bar_h
                text = f"{value:.3f}"
                opacity = 1.0
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" fill="{fill}" fill-opacity="{opacity}"/>'
            )
            parts.append(
                f'<text x="{x+bar_w/2:.1f}" y="{y-5:.1f}" text-anchor="middle" font-size="10" fill="#0f172a">{esc(text)}</text>'
            )
            parts.append(
                f'<text x="{x+bar_w/2:.1f}" y="{height-38}" text-anchor="middle" font-size="9" fill="#64748b">{tag}</text>'
            )
        parts.append(f'<text x="{x0+group_w/2:.1f}" y="{height-20}" text-anchor="middle" font-size="11" fill="#334155">{esc(label)}</text>')
    parts.append(f'<text x="{width/2}" y="20" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">{esc(title)}</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def render_html(
    dataset: str,
    metrics: list[Metric],
    global_summaries: list[Summary],
    task_summaries: list[Summary],
    variant: str = "standard",
) -> str:
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    models = sorted({m.model for m in metrics}, key=model_sort_key)
    report_metrics = [m for m in metrics if m.task != "legacy"]
    tasks = sorted({m.task for m in report_metrics}, key=lambda t: (KNOWN_TASKS.index(t) if t in KNOWN_TASKS else 99, t))
    title = "Finance Outputs LLM Evaluation Report" if variant == "llm" else "Finance Outputs Evaluation Report"
    artifact_note = (
        "Generated from LLM-assisted evaluation artifacts under per-model outputs_llm*/evaluation_llm* folders."
        if variant == "llm"
        else f"Generated from local <code>acc.json</code> artifacts under <code>systems/*/outputs/{esc(dataset.lower())}</code>."
    )
    use_adjusted = variant == "llm"
    metric_label = "Adjusted Macro F1" if use_adjusted else "Macro F1"

    if use_adjusted:
        overall_headers = [
            "Model",
            "Scope",
            "Coverage",
            "Completion",
            "Raw Macro F1",
            "Adjusted Macro F1",
            "Delta",
            "Empty/Empty",
            "Matched Rows",
        ]
        overall_rows = [
            [
                esc(s.model),
                esc(s.task),
                f"{s.completed}/{s.expected}",
                fmt_pct(s.completion_rate),
                fmt_num(s.macro_f1_mean),
                fmt_num(s.adjusted_macro_f1_mean),
                fmt_num(
                    (s.adjusted_macro_f1_mean - s.macro_f1_mean)
                    if s.adjusted_macro_f1_mean is not None and s.macro_f1_mean is not None
                    else None
                ),
                esc(s.empty_empty_count),
                esc(s.matched_rows),
            ]
            for s in global_summaries
        ]
        task_headers = [
            "Task",
            "Model",
            "Coverage",
            "Completion",
            "Raw Macro F1",
            "Adjusted Macro F1",
            "Delta",
            "Empty/Empty",
            "Precision",
            "Recall",
        ]
        task_rows = [
            [
                esc(s.task),
                esc(s.model),
                f"{s.completed}/{s.expected}",
                fmt_pct(s.completion_rate),
                fmt_num(s.macro_f1_mean),
                fmt_num(s.adjusted_macro_f1_mean),
                fmt_num(
                    (s.adjusted_macro_f1_mean - s.macro_f1_mean)
                    if s.adjusted_macro_f1_mean is not None and s.macro_f1_mean is not None
                    else None
                ),
                esc(s.empty_empty_count),
                fmt_num(s.macro_precision_mean),
                fmt_num(s.macro_recall_mean),
            ]
            for s in task_summaries
        ]
    else:
        overall_headers = [
            "Model",
            "Scope",
            "Coverage",
            "Completion",
            "Macro F1",
            "Column F1",
            "Precision",
            "Recall",
            "Matched Rows",
        ]
        overall_rows = [
            [
                esc(s.model),
                esc(s.task),
                f"{s.completed}/{s.expected}",
                fmt_pct(s.completion_rate),
                fmt_num(s.macro_f1_mean),
                fmt_num(s.column_f1_mean),
                fmt_num(s.macro_precision_mean),
                fmt_num(s.macro_recall_mean),
                esc(s.matched_rows),
            ]
            for s in global_summaries
        ]
        task_headers = ["Task", "Model", "Coverage", "Completion", "Macro F1", "Column F1", "Precision", "Recall"]
        task_rows = [
            [
                esc(s.task),
                esc(s.model),
                f"{s.completed}/{s.expected}",
                fmt_pct(s.completion_rate),
                fmt_num(s.macro_f1_mean),
                fmt_num(s.column_f1_mean),
                fmt_num(s.macro_precision_mean),
                fmt_num(s.macro_recall_mean),
            ]
            for s in task_summaries
        ]

    if use_adjusted:
        detail_headers = [
            "Task",
            "Query",
            "Model",
            "Raw Macro F1",
            "Adjusted Macro F1",
            "Empty/Empty",
            "Columns",
            "Matched",
            "Pred Rows",
            "Gold Rows",
            "Artifact",
        ]
        detail_rows = [
            [
                esc(m.task),
                esc(m.query_index),
                esc(m.model),
                fmt_num(m.macro_f1),
                fmt_num(m.adjusted_macro_f1),
                esc("yes" if m.is_empty_empty else "no"),
                esc(m.n_columns),
                esc(m.matched_rows if m.matched_rows is not None else "n/a"),
                esc(m.len_pred if m.len_pred is not None else "n/a"),
                esc(m.len_gold if m.len_gold is not None else "n/a"),
                esc(m.acc_path.relative_to(repo_root())),
            ]
            for m in sorted(report_metrics, key=lambda x: (x.task, x.query_index, model_sort_key(x.model)))
        ]
    else:
        detail_headers = [
            "Task",
            "Query",
            "Model",
            "Macro F1",
            "Column F1",
            "Columns",
            "Matched",
            "Pred Rows",
            "Gold Rows",
            "Artifact",
        ]
        detail_rows = [
            [
                esc(m.task),
                esc(m.query_index),
                esc(m.model),
                fmt_num(m.macro_f1),
                fmt_num(m.column_f1_mean),
                esc(m.n_columns),
                esc(m.matched_rows if m.matched_rows is not None else "n/a"),
                esc(m.len_pred if m.len_pred is not None else "n/a"),
                esc(m.len_gold if m.len_gold is not None else "n/a"),
                esc(m.acc_path.relative_to(repo_root())),
            ]
            for m in sorted(report_metrics, key=lambda x: (x.task, x.query_index, model_sort_key(x.model)))
        ]

    task_sections: list[str] = []
    for task in tasks:
        summaries = [s for s in task_summaries if s.task == task]
        chart_rows = [
            (
                s.model,
                s.adjusted_macro_f1_mean if use_adjusted else s.macro_f1_mean,
                COLORS.get(s.model, "#64748b"),
            )
            for s in summaries
        ]
        section_headers = (
            ["Model", "Coverage", "Completion", "Raw Macro F1", "Adjusted Macro F1", "Delta", "Empty/Empty"]
            if use_adjusted
            else ["Model", "Coverage", "Completion", "Macro F1", "Column F1", "Precision", "Recall"]
        )
        section_rows = (
            [
                [
                    esc(s.model),
                    f"{s.completed}/{s.expected}",
                    fmt_pct(s.completion_rate),
                    fmt_num(s.macro_f1_mean),
                    fmt_num(s.adjusted_macro_f1_mean),
                    fmt_num(
                        (s.adjusted_macro_f1_mean - s.macro_f1_mean)
                        if s.adjusted_macro_f1_mean is not None and s.macro_f1_mean is not None
                        else None
                    ),
                    esc(s.empty_empty_count),
                ]
                for s in summaries
            ]
            if use_adjusted
            else [
                [
                    esc(s.model),
                    f"{s.completed}/{s.expected}",
                    fmt_pct(s.completion_rate),
                    fmt_num(s.macro_f1_mean),
                    fmt_num(s.column_f1_mean),
                    fmt_num(s.macro_precision_mean),
                    fmt_num(s.macro_recall_mean),
                ]
                for s in summaries
            ]
        )
        task_sections.append(
            f"""
      <article class="card">
        <h2>{esc(task.upper())} Summary</h2>
        {render_table(section_headers, section_rows)}
      </article>
      <article class="card">
        <h2>{esc(task.upper())} Chart</h2>
        {render_bar_chart(f"{metric_label} by model ({task})", chart_rows)}
      </article>
"""
        )

    overall_chart_rows = [
        (
            s.model,
            s.adjusted_macro_f1_mean if use_adjusted else s.macro_f1_mean,
            COLORS.get(s.model, "#64748b"),
        )
        for s in global_summaries
    ]
    raw_vs_adjusted_chart = ""
    if use_adjusted:
        raw_vs_adjusted_chart = f"""
      <article class="card">
        <h2>Raw vs Adjusted</h2>
        {render_grouped_bar_chart("Raw vs adjusted Macro F1 by model", [
            (s.model, s.macro_f1_mean, s.adjusted_macro_f1_mean, COLORS.get(s.model, "#64748b"))
            for s in global_summaries
        ])}
        <p class="foot">Adjusted Macro F1 counts queries with empty gold and empty prediction as F1 = 1.0. Raw Macro F1 is the evaluator output stored in each <code>acc.json</code>.</p>
      </article>
"""
    empty_result_note = (
        '<p class="foot">Empty-result correction: when both the gold query result and the system output are empty, '
        "the adjusted score counts the query as perfectly correct (F1 = 1.0). Raw artifacts are not modified.</p>"
        if use_adjusted
        else ""
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{esc(title)}</title>
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
    .wrap {{ max-width: 1240px; margin: 24px auto; padding: 0 16px 24px; }}
    .hero {{
      background: linear-gradient(120deg, #0b4f6c 0%, #145a7a 40%, #1b7ea8 100%);
      color: #fff;
      border-radius: 14px;
      padding: 18px 20px;
      box-shadow: 0 8px 20px rgba(11,79,108,.25);
    }}
    .hero h1 {{ margin: 0 0 8px; font-size: 26px; letter-spacing: .2px; }}
    .hero p {{ margin: 2px 0; color: #dbefff; }}
    .grid {{ margin-top: 16px; display: grid; grid-template-columns: 1fr; gap: 14px; }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px 14px 10px;
      box-shadow: 0 6px 14px rgba(15,23,42,.06);
      overflow-x: auto;
    }}
    .card h2 {{ margin: 0 0 10px; font-size: 17px; color: #0b3b52; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 8px 7px; text-align: left; white-space: nowrap; }}
    th {{ background: #f2f7fb; color: #12364a; font-weight: 700; }}
    .muted {{ color: var(--muted); font-size: 12px; }}
    .foot {{ margin-top: 6px; color: #64748b; font-size: 12px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>{esc(title)}</h1>
      <p><strong>Dataset:</strong> {esc(dataset)} | <strong>Models:</strong> {esc(", ".join(models) if models else "none")} | <strong>Tasks:</strong> {esc(", ".join(tasks) if tasks else "none")}</p>
      <p>{artifact_note}</p>
      <p><strong>Generated at:</strong> {esc(generated)}</p>
    </section>

    <section class="grid">
      <article class="card">
        <h2>Overall Metrics Summary</h2>
        {render_table(overall_headers, overall_rows)}
        <p class="foot">Coverage uses expected query counts from <code>Query/{esc(dataset)}</code>. Lotus has 60 available outputs and is shown only in this global view against the full Finance denominator.</p>
        {empty_result_note}
      </article>

      <article class="card">
        <h2>Overall Chart</h2>
        {render_bar_chart(f"{metric_label} by model (all detected outputs)", overall_chart_rows)}
      </article>

      {raw_vs_adjusted_chart}

      <article class="card">
        <h2>Task Metrics Summary</h2>
        {render_table(task_headers, task_rows)}
      </article>

      {"".join(task_sections)}

      <article class="card">
        <h2>Per-query Details</h2>
        {render_table(detail_headers, detail_rows)}
      </article>
    </section>
  </div>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate an HTML report from local Finance output artifacts.")
    parser.add_argument("--dataset", default="Finan")
    parser.add_argument("--variant", choices=["standard", "llm"], default="standard")
    parser.add_argument("--output")
    args = parser.parse_args()

    metrics = discover_metrics(args.dataset, variant=args.variant)
    expected = query_counts(args.dataset)
    global_summaries, task_summaries = build_summaries(metrics, expected)
    html_text = render_html(args.dataset, metrics, global_summaries, task_summaries, variant=args.variant)

    default_name = "finance_outputs_report_llm.html" if args.variant == "llm" else "finance_outputs_report.html"
    output = Path(args.output) if args.output else Path(__file__).with_name(default_name)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html_text, encoding="utf-8")
    print(f"Wrote {output}")
    print(f"Detected {len(metrics)} acc.json artifacts across {len({m.model for m in metrics})} model(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
