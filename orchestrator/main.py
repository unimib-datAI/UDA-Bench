"""Root-level meta-orchestrator entrypoint.

Supports matrix execution across:
- models (single, list, all)
- datasets (single, list, all)
- query types (single, list, all)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from itertools import product
from pathlib import Path

# Allow execution both as module and as script path.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator.logger import RunLogger, now_iso
from orchestrator.registry import build_registry
from orchestrator.schemas import JobResult, JobSpec, write_json

VALID_QUERY_TYPES = {"all", "agg", "filter", "select", "mixed", "join"}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _discover_datasets() -> list[str]:
    query_root = _repo_root() / "Query"
    if not query_root.exists():
        return []
    return sorted([p.name for p in query_root.iterdir() if p.is_dir() and not p.name.startswith("__")])


def _parse_selector(value: str, universe: list[str], label: str) -> list[str]:
    raw = (value or "all").strip()
    if raw.lower() == "all":
        return list(universe)
    if raw.lower().startswith("list:"):
        items = [x.strip() for x in raw[5:].split(",") if x.strip()]
    else:
        items = [raw]

    unknown = [x for x in items if x not in universe]
    if unknown:
        raise ValueError(f"{label} non validi: {unknown}. Valori supportati: {universe}")
    return items


def _parse_query_types(value: str) -> list[str]:
    raw = (value or "all").strip().lower()
    if raw == "all":
        return ["all"]
    if raw.startswith("list:"):
        items = [x.strip().lower() for x in raw[5:].split(",") if x.strip()]
    else:
        items = [raw]
    unknown = [x for x in items if x not in VALID_QUERY_TYPES]
    if unknown:
        raise ValueError(f"query-type non validi: {unknown}. Valori: {sorted(VALID_QUERY_TYPES)}")
    return items


def _make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")


def _write_raw_job_log(run_dir: Path, result: JobResult) -> str:
    safe_name = f"{result.model}__{result.dataset}__{result.query_type}.log"
    log_path = run_dir / "raw_logs" / safe_name
    log_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        f"MODEL: {result.model}",
        f"DATASET: {result.dataset}",
        f"QUERY_TYPE: {result.query_type}",
        f"MODE: {result.mode}",
        f"STATUS: {result.status}",
        f"RETURN_CODE: {result.return_code}",
        f"DURATION_SEC: {result.duration_sec:.3f}",
        f"COMMAND: {' '.join(result.command)}",
        "",
        "=== STDOUT (tail) ===",
        *(result.stdout_tail or []),
        "",
        "=== STDERR (tail) ===",
        *(result.stderr_tail or []),
    ]
    log_path.write_text("\n".join(lines), encoding="utf-8")
    return str(log_path)


def _aggregate_summary(results: list[JobResult]) -> dict:
    ok = sum(1 for r in results if r.status == "ok")
    err = sum(1 for r in results if r.status != "ok")
    macro_vals = [r.macro_f1_mean for r in results if isinstance(r.macro_f1_mean, (int, float))]
    return {
        "total_jobs": len(results),
        "ok_jobs": ok,
        "error_jobs": err,
        "macro_f1_mean_over_jobs": (sum(macro_vals) / len(macro_vals)) if macro_vals else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Root meta-orchestrator for UDA-Bench systems")
    parser.add_argument("--model", default="all", help="docetl | evaporate | all | list:docetl,evaporate")
    parser.add_argument("--dataset", default="all", help="Finan | Player | ... | all | list:Finan,Player")
    parser.add_argument(
        "--query-type",
        default="all",
        help="all | agg | filter | select | mixed | join | list:select,filter",
    )
    parser.add_argument(
        "--mode",
        default="run+eval",
        choices=["run", "eval", "run+eval"],
        help="run only, eval only, or run followed by eval",
    )
    parser.add_argument("--rebuild", action="store_true", help="Rebuild pipeline outputs where supported")
    parser.add_argument("--rebuild-eval", action="store_true", help="Rebuild evaluation outputs")
    parser.add_argument("--rebuild-extract", action="store_true", help="Evaporate only: rebuild extraction")
    parser.add_argument("--rebuild-table", action="store_true", help="Evaporate only: rebuild full table")
    parser.add_argument("--run-id", default=None, help="Optional custom run id")
    parser.add_argument("--list", action="store_true", help="List available models/datasets and exit")
    args = parser.parse_args()

    registry = build_registry()
    model_universe = sorted(registry.keys())
    dataset_universe = _discover_datasets()
    if args.list:
        print("Available models :", ", ".join(model_universe))
        print("Available datasets:", ", ".join(dataset_universe))
        print("Query types      :", ", ".join(sorted(VALID_QUERY_TYPES)))
        return 0

    models = _parse_selector(args.model, model_universe, "model")
    datasets = _parse_selector(args.dataset, dataset_universe, "dataset")
    query_types = _parse_query_types(args.query_type)

    run_id = args.run_id or _make_run_id()
    run_dir = _repo_root() / "orchestrator" / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    logger = RunLogger(run_dir / "events.log")
    logger.info(f"Starting run {run_id}")

    manifest = {
        "run_id": run_id,
        "started_at": now_iso(),
        "mode": args.mode,
        "models": models,
        "datasets": datasets,
        "query_types": query_types,
        "flags": {
            "rebuild": args.rebuild,
            "rebuild_eval": args.rebuild_eval,
            "rebuild_extract": args.rebuild_extract,
            "rebuild_table": args.rebuild_table,
        },
    }
    write_json(run_dir / "run_manifest.json", manifest)

    job_specs: list[JobSpec] = [
        JobSpec(model=m, dataset=d, query_type=qt, mode=args.mode)
        for (m, d, qt) in product(models, datasets, query_types)
    ]
    logger.info(f"Planned {len(job_specs)} jobs")

    results: list[JobResult] = []
    for idx, spec in enumerate(job_specs, start=1):
        logger.info(
            f"[{idx}/{len(job_specs)}] {spec.model} | {spec.dataset} | {spec.query_type} | {spec.mode}"
        )
        adapter = registry[spec.model]
        result = adapter.execute(
            spec=spec,
            rebuild=args.rebuild,
            rebuild_eval=args.rebuild_eval,
            rebuild_extract=args.rebuild_extract,
            rebuild_table=args.rebuild_table,
        )
        result.raw_log_path = _write_raw_job_log(run_dir, result)
        results.append(result)

        write_json(
            run_dir / "jobs" / f"{spec.model}__{spec.dataset}__{spec.query_type}.json",
            result.to_dict(),
        )

        if result.status == "ok":
            logger.info(
                f"OK {spec.model}/{spec.dataset}/{spec.query_type} "
                f"(macro_f1={result.macro_f1_mean})"
            )
        else:
            logger.error(
                f"ERROR {spec.model}/{spec.dataset}/{spec.query_type} "
                f"(return_code={result.return_code})"
            )

    summary = {
        "run_id": run_id,
        "started_at": manifest["started_at"],
        "ended_at": now_iso(),
        "mode": args.mode,
        "selection": {
            "models": models,
            "datasets": datasets,
            "query_types": query_types,
        },
        "aggregate": _aggregate_summary(results),
        "jobs": [r.to_dict() for r in results],
    }
    write_json(run_dir / "summary.json", summary)

    agg = summary["aggregate"]
    logger.info(
        "Run completed: "
        f"total={agg['total_jobs']} ok={agg['ok_jobs']} err={agg['error_jobs']} "
        f"macro_f1_mean_over_jobs={agg['macro_f1_mean_over_jobs']}"
    )
    logger.info(f"Run folder: {run_dir}")
    return 0 if agg["error_jobs"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
