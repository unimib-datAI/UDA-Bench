## DQL Integration Notes

This folder contains the DQL runner used by the root orchestrator.

- Entry script: `systems/DQL/main.py`
- Runtime dependency for the standalone client: `requests` (this folder does not currently ship a dedicated requirements file)
- Outputs (canonical, flat like DocETL/Evaporate):
  - query CSVs: `systems/DQL/outputs/<dataset>/csv/<query_name>.csv`
  - evaluation: `systems/DQL/outputs/<dataset>/evaluation/<query_name>/...`
- Internal per-query runtime folders are generated for execution/debug:
  - `systems/DQL/outputs/<dataset>/_runtime/<query_type>/query_<n>/...`
- Legacy paths still readable for backward compatibility:
  - `systems/DQL/outputs/<dataset>/<query_type>/csv/query_<n>/...`
  - `systems/DQL/results/<Dataset>/<query_type>/...`

### Standalone run (DQL API)

```powershell
.\.venv-DQL\Scripts\python.exe systems/DQL/main.py --user-id Finance --queries "SELECT earnings_per_share FROM finance" --api-url http://127.0.0.1:8000/api/v2/chat --out_dir systems/DQL/outputs/finan/_runtime/select/query_1
```

If `--api-url` is omitted, the client uses `DQL_API_URL` from the root `.env`/environment, then falls back to `http://127.0.0.1:9000/api/answer`.

### Recommended: run via root orchestrator

```powershell
# run only
.\.venv-DQL\Scripts\python.exe orchestrator/main.py --model dql --dataset Finan --query-type select --mode run

# eval only (uses existing artifacts)
.\.venv-DQL\Scripts\python.exe orchestrator/main.py --model dql --dataset Finan --query-type select --mode eval

# run + eval
.\.venv-DQL\Scripts\python.exe orchestrator/main.py --model dql --dataset Finan --query-type select --mode run+eval
```

### Evaluation compatibility

DQL can return non-tabular JSON. The orchestrator DQL adapter handles this by:

1. extracting rows from narrative JSON via LLM as primary path
2. creating evaluator-compatible CSV shape (`id` + requested columns), with template fallback when needed
3. running the shared `evaluation.run_eval` pipeline

This keeps DQL inside the same unified orchestration/evaluation flow used by other models.

