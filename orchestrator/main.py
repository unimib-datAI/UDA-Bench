"""Root-level meta-orchestrator entrypoint.

Supports matrix execution across:
- models (single, list, all)
- datasets (single, list, all)
- query types (single, list, all)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from shutil import copy2

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
    datasets: list[str] = []
    for p in query_root.iterdir():
        if not p.is_dir() or p.name.startswith("__"):
            continue
        # Keep only real dataset folders containing SQL queries.
        if list(p.rglob("*.sql")):
            datasets.append(p.name)
    return sorted(datasets)


def _build_case_map(universe: list[str]) -> dict[str, str]:
    return {u.lower(): u for u in universe}


def _parse_selector(value: str, universe: list[str], label: str) -> list[str]:
    raw = (value or "all").strip()
    case_map = _build_case_map(universe)
    if raw.lower() == "all":
        return list(universe)
    if raw.lower().startswith("list:"):
        items = [x.strip() for x in raw[5:].split(",") if x.strip()]
    else:
        items = [raw]

    normalized: list[str] = []
    unknown: list[str] = []
    for item in items:
        key = item.lower()
        if key not in case_map:
            unknown.append(item)
            continue
        normalized.append(case_map[key])

    if unknown:
        raise ValueError(f"{label} non validi: {unknown}. Valori supportati: {universe}")
    # Preserve user order while removing duplicates.
    dedup: list[str] = []
    seen = set()
    for item in normalized:
        if item in seen:
            continue
        seen.add(item)
        dedup.append(item)
    return dedup


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
    # "all" is mutually exclusive to avoid accidental duplicate job sets.
    if "all" in items and len(items) > 1:
        raise ValueError("query-type 'all' non puo essere combinato con altri valori")
    dedup: list[str] = []
    seen = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        dedup.append(item)
    return dedup


def _make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")


def _split_sql_queries(text: str) -> list[str]:
    chunks = [q.strip() for q in text.split(";")]
    return [q for q in chunks if q]


def _prepare_run_dirs(run_dir: Path) -> dict[str, Path]:
    paths = {
        "root": run_dir,
        "manifest": run_dir / "manifest",
        "queries": run_dir / "queries",
        "outputs": run_dir / "outputs",
        "metrics": run_dir / "metrics",
        "jobs": run_dir / "jobs",
        "logs": run_dir / "logs",
        "raw_logs": run_dir / "logs" / "raw",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


def _snapshot_queries_for_dataset(
    dataset: str,
    query_types: list[str],
    queries_dir: Path,
) -> list[dict]:
    query_root = _repo_root() / "Query" / dataset
    if not query_root.exists():
        return []

    selected_types = set(query_types)
    include_all = "all" in selected_types

    snapshot_rows: list[dict] = []
    for sql_file in sorted(query_root.rglob("*.sql")):
        category = sql_file.parent.name.lower()
        if not include_all and category not in selected_types:
            continue

        relative_path = sql_file.relative_to(query_root)
        snapshot_file = queries_dir / dataset / relative_path
        snapshot_file.parent.mkdir(parents=True, exist_ok=True)
        copy2(sql_file, snapshot_file)

        content = sql_file.read_text(encoding="utf-8")
        for idx, sql_text in enumerate(_split_sql_queries(content), start=1):
            snapshot_rows.append(
                {
                    "dataset": dataset,
                    "query_type": category,
                    "query_id": f"{category}_{sql_file.stem}_{idx}",
                    "source_file": str(sql_file),
                    "snapshot_file": str(snapshot_file),
                    "sql": sql_text,
                }
            )
    return snapshot_rows


def _write_query_snapshot_index(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")


def _query_prefixes(query_type: str) -> list[str] | None:
    qt = (query_type or "all").lower()
    if qt == "all":
        return None
    return [f"{qt}_"]


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    copy2(src, dst)


def _copy_tree_filtered(src_dir: Path, dst_dir: Path, prefixes: list[str] | None = None) -> int:
    if not src_dir.exists():
        return 0
    copied = 0
    for p in src_dir.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(src_dir)
        if prefixes is not None and not any(rel.name.startswith(pref) for pref in prefixes):
            continue
        _copy_file(p, dst_dir / rel)
        copied += 1
    return copied


def _copy_eval_dirs_filtered(eval_src: Path, eval_dst: Path, prefixes: list[str] | None = None) -> int:
    if not eval_src.exists():
        return 0
    copied = 0
    for p in eval_src.iterdir():
        if not p.is_dir():
            continue
        name = p.name
        if name.startswith("_"):
            continue
        if prefixes is not None and not any(name.startswith(pref) for pref in prefixes):
            continue
        copied += _copy_tree_filtered(p, eval_dst / name, prefixes=None)
    return copied


def _materialize_system_outputs(run_outputs_dir: Path, result: JobResult) -> dict:
    root = _repo_root()
    model = result.model.lower()
    dataset = result.dataset
    query_type = (result.query_type or "all").lower()
    prefixes = _query_prefixes(query_type)

    if model == "docetl":
        sys_out_root = root / "systems" / "DocETL" / "outputs" / dataset.lower()
    elif model == "evaporate":
        sys_out_root = root / "systems" / "Evaporate" / "outputs" / dataset.lower()
    elif model == "lotus":
        sys_out_root = root / "systems" / "Lotus" / "results" / dataset.lower()
    elif model == "quest":
        sys_out_root = root / "systems" / "quest" / "results" / dataset.lower()
    else:
        return {"copied_files": 0, "details": {"reason": "unsupported model"}}

    target_root = run_outputs_dir / model / dataset / query_type
    target_root.mkdir(parents=True, exist_ok=True)

    copied = 0
    details: dict[str, int | str] = {}

    # Query-level artifacts.
    copied_csv = _copy_tree_filtered(sys_out_root / "csv", target_root / "csv", prefixes=prefixes)
    copied += copied_csv
    details["csv_files"] = copied_csv

    if model == "docetl":
        copied_yaml = _copy_tree_filtered(sys_out_root / "yaml", target_root / "yaml", prefixes=prefixes)
        copied_json = _copy_tree_filtered(sys_out_root / "json", target_root / "json", prefixes=prefixes)
        copied += copied_yaml + copied_json
        details["yaml_files"] = copied_yaml
        details["json_files"] = copied_json
    elif model == "evaporate":
        full_table = sys_out_root / "evaporate_full_table.csv"
        if full_table.exists():
            _copy_file(full_table, target_root / "evaporate_full_table.csv")
            copied += 1
            details["full_table"] = "copied"

    # Evaluation artifacts.
    eval_src = sys_out_root / "evaluation"
    eval_dst = target_root / "evaluation"

    summary_name = "summary.json" if query_type == "all" else f"summary_{query_type}.json"
    summary_src = eval_src / summary_name
    if summary_src.exists():
        _copy_file(summary_src, eval_dst / summary_name)
        copied += 1
        details["summary"] = summary_name

    copied_eval_query_dirs = _copy_eval_dirs_filtered(eval_src, eval_dst, prefixes=prefixes)
    copied += copied_eval_query_dirs
    details["eval_query_dir_files"] = copied_eval_query_dirs

    # Optional evaluation debug folders.
    copied_eval_logs = _copy_tree_filtered(eval_src / "_logs", eval_dst / "_logs", prefixes=prefixes)
    copied_eval_tmp = _copy_tree_filtered(eval_src / "_tmp_result_csv", eval_dst / "_tmp_result_csv", prefixes=prefixes)
    copied_eval_sql = _copy_tree_filtered(eval_src / "_sql_json", eval_dst / "_sql_json", prefixes=prefixes)
    copied += copied_eval_logs + copied_eval_tmp + copied_eval_sql
    details["eval_logs_files"] = copied_eval_logs
    details["eval_tmp_csv_files"] = copied_eval_tmp
    details["eval_sql_json_files"] = copied_eval_sql

    return {
        "copied_files": copied,
        "source_root": str(sys_out_root),
        "target_root": str(target_root),
        "details": details,
    }


def _write_raw_job_log(raw_logs_dir: Path, result: JobResult) -> str:
    safe_name = f"{result.model}__{result.dataset}__{result.query_type}.log"
    log_path = raw_logs_dir / safe_name
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
    if not datasets:
        raise ValueError("Nessun dataset trovato in Query/")

    run_id = args.run_id or _make_run_id()
    run_dir = _repo_root() / "orchestrator" / "runs" / run_id
    run_paths = _prepare_run_dirs(run_dir)
    
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    os.environ["ORCHESTRATOR_RUN_DIR"] = str(run_dir)
    logger = RunLogger(run_paths["logs"] / "events.log")
    logger.info(f"Starting run {run_id}")

    snapshot_rows: list[dict] = []
    for dataset in datasets:
        snapshot_rows.extend(
            _snapshot_queries_for_dataset(
                dataset=dataset,
                query_types=query_types,
                queries_dir=run_paths["queries"],
            )
        )
    _write_query_snapshot_index(run_paths["queries"] / "queries_index.jsonl", snapshot_rows)
    logger.info(f"Snapshotted {len(snapshot_rows)} queries into {run_paths['queries']}")

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
        "artifacts": {
            "queries_snapshot_dir": str(run_paths["queries"]),
            "queries_index_jsonl": str(run_paths["queries"] / "queries_index.jsonl"),
            "jobs_dir": str(run_paths["jobs"]),
            "metrics_dir": str(run_paths["metrics"]),
            "logs_dir": str(run_paths["logs"]),
        },
    }
    write_json(run_paths["manifest"] / "run_manifest.json", manifest)

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
        result.raw_log_path = _write_raw_job_log(run_paths["raw_logs"], result)

        materialize_info = _materialize_system_outputs(run_paths["outputs"], result)
        logger.info(
            f"Materialized outputs for {spec.model}/{spec.dataset}/{spec.query_type}: "
            f"{materialize_info.get('copied_files', 0)} files"
        )

        results.append(result)

        job_payload = result.to_dict()
        job_payload["materialized_outputs"] = materialize_info
        write_json(run_paths["jobs"] / f"{spec.model}__{spec.dataset}__{spec.query_type}.json", job_payload)

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
    write_json(run_paths["metrics"] / "summary.json", summary)
    # Backward compatibility with earlier run layout.
    write_json(run_paths["root"] / "summary.json", summary)

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
