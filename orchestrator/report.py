"""Aggregate run summaries into a single CSV report."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _split_csv_values(raw: str | None) -> set[str] | None:
    if not raw:
        return None
    vals = {x.strip().lower() for x in raw.split(",") if x.strip()}
    return vals or None


def _iter_summary_files(runs_dir: Path) -> list[Path]:
    files: list[Path] = []
    for run_dir in sorted(runs_dir.iterdir()):
        if not run_dir.is_dir():
            continue
        metrics_summary = run_dir / "metrics" / "summary.json"
        root_summary = run_dir / "summary.json"
        if metrics_summary.exists():
            files.append(metrics_summary)
        elif root_summary.exists():
            files.append(root_summary)
    return files


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _matches(value: str | None, allowed: set[str] | None) -> bool:
    if allowed is None:
        return True
    return (value or "").lower() in allowed


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a CSV report across orchestrator runs")
    parser.add_argument("--run-id", help="Comma-separated run ids to include")
    parser.add_argument("--model", help="Comma-separated models to include")
    parser.add_argument("--dataset", help="Comma-separated datasets to include")
    parser.add_argument("--query-type", help="Comma-separated query types to include")
    parser.add_argument(
        "--out",
        default="orchestrator/reports/runs_report.csv",
        help="Output CSV path",
    )
    args = parser.parse_args()

    runs_dir = _repo_root() / "orchestrator" / "runs"
    if not runs_dir.exists():
        raise FileNotFoundError(f"Runs directory not found: {runs_dir}")

    run_filter = _split_csv_values(args.run_id)
    model_filter = _split_csv_values(args.model)
    dataset_filter = _split_csv_values(args.dataset)
    qtype_filter = _split_csv_values(args.query_type)

    rows: list[dict] = []
    for summary_file in _iter_summary_files(runs_dir):
        payload = _load_json(summary_file)
        run_id = payload.get("run_id")
        if not _matches(run_id, run_filter):
            continue

        for job in payload.get("jobs", []):
            model = job.get("model")
            dataset = job.get("dataset")
            query_type = job.get("query_type")
            if not _matches(model, model_filter):
                continue
            if not _matches(dataset, dataset_filter):
                continue
            if not _matches(query_type, qtype_filter):
                continue

            rows.append(
                {
                    "run_id": run_id,
                    "started_at": payload.get("started_at"),
                    "ended_at": payload.get("ended_at"),
                    "mode": payload.get("mode"),
                    "model": model,
                    "dataset": dataset,
                    "query_type": query_type,
                    "status": job.get("status"),
                    "return_code": job.get("return_code"),
                    "duration_sec": job.get("duration_sec"),
                    "macro_f1_mean": job.get("macro_f1_mean"),
                    "summary_path": job.get("summary_path"),
                    "job_log": job.get("raw_log_path"),
                }
            )

    out_path = (_repo_root() / args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run_id",
        "started_at",
        "ended_at",
        "mode",
        "model",
        "dataset",
        "query_type",
        "status",
        "return_code",
        "duration_sec",
        "macro_f1_mean",
        "summary_path",
        "job_log",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Rows: {len(rows)}")
    print(f"Report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
