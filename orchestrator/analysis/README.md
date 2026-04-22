## Orchestrator Analysis

This folder contains report scripts and benchmark snapshots used for result presentation.

## Scope

- Systems: `DocETL`, `Evaporate`, `DQL`
- Dataset: `Finan`
- Official final page: `SELECT` (`Query/Finan/Select/select_queries.sql`) via `select_eval_report.py`
- Top-k multi-task page: supports `SELECT`, `AGG`, `MIXED` via `select_topk_compare.py`

### Main script

- `select_eval_report.py`: generates a single HTML report for SELECT comparison

### Optional helper

- `select_topk_compare.py`: top-k focused comparison utility (supports multiple tasks in one HTML, e.g. `select,agg,mixed`)
- top-k outputs (for example `select_top6_compare.html`) are exploratory artifacts and are not required deliverables
- default behavior excludes Lotus to keep the top-k comparison fair across models with per-query artifacts (`DocETL`, `Evaporate`, `DQL`)

### Inputs used

- model evaluation artifacts under `systems/*/outputs/...` (DQL included; legacy DQL `results/...` is still readable as fallback)
- benchmark snapshot `benchmark_results.csv` (includes Lotus global metrics)

### Generate final report

```powershell
.\.venv-DQL\Scripts\python.exe orchestrator/analysis/select_eval_report.py --dataset Finan --output orchestrator/analysis/select_report.html
```

### Generate fair top-k report (DocETL/Evaporate/DQL)

```powershell
py -3.11 orchestrator/analysis/select_topk_compare.py --dataset Finan --tasks select,agg,mixed --topk 6 --output orchestrator/analysis/select_top6_compare.html
```

If you explicitly want Lotus global numbers from benchmark CSV:

```powershell
py -3.11 orchestrator/analysis/select_topk_compare.py --dataset Finan --tasks select,agg,mixed --topk 6 --include-lotus-from-benchmark --output orchestrator/analysis/select_top6_compare.html
```

### Open report

```powershell
start .\orchestrator\analysis\select_report.html
```

### Notes

- `select_report.html` is the single deliverable report page.
- temporary folders/files (for example `_tmp_topk/`) are not required for commit.
- if you need only one official output in this folder, keep `select_report.html` and regenerate top-k pages on demand.

## Metrics used

- `macro_f1` (per query): average of F1 over selected columns in that query.
- `query_macro_mean` (per model): average of per-query `macro_f1`.
- `global_column_mean` (per model): average of all column-level F1 across all SELECT queries.
- `completion_rate`: `completed_queries / expected_queries`.

Top-k evaluation note:
- `select_topk_compare.py` normalizes evaluation SQL for numeric aggregations on text-like columns:
  - `AVG(col)` -> `AVG(TRY_CAST(col AS DOUBLE))`
  - `SUM(col)` -> `SUM(TRY_CAST(col AS DOUBLE))`
- This avoids DuckDB binder errors on datasets where numeric values are stored as strings.

## Why two global F1 views

- `query_macro_mean` gives equal weight to each query.
- `global_column_mean` gives equal weight to each output column.
- If queries have different numbers of selected columns, the two values can differ.

## Current DQL limitation

- DQL outputs are currently JSON narrative outputs (`results.json`) and do not always include evaluation artifacts (`acc.json`).
- Without `acc.json` (or equivalent column-aligned CSV evaluated by `evaluation.run_eval`), F1 cannot be computed reliably.
- In that case, report uses `completion_rate` and flags DQL as "not fully evaluated yet".

