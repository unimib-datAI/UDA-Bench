## Orchestrator Analysis

This folder contains report scripts and benchmark snapshots used for result presentation.

### Main script

- `select_eval_report.py`: generates a single HTML report for SELECT comparison

### Optional helper

- `select_topk_compare.py`: top-k focused comparison utility

### Inputs used

- model evaluation artifacts under `systems/*/outputs/...` and `systems/DQL/results/...`
- benchmark snapshot `benchmark_results.csv` (includes Lotus global metrics)

### Generate final report

```powershell
.\.venv-DQL\Scripts\python.exe orchestrator/analysis/select_eval_report.py --dataset Finan --output orchestrator/analysis/select_report.html
```

### Open report

```powershell
start .\orchestrator\analysis\select_report.html
```

### Notes

- `select_report.html` is the single deliverable report page.
- temporary folders/files (for example `_tmp_topk/`) are not required for commit.

