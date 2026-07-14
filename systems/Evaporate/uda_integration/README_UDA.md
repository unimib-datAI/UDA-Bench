# Evaporate × UDA integration utilities

This folder contains the two low-level utilities that connect the original Evaporate metadata artifacts to UDA-Bench SQL queries. For normal batch execution and evaluation, use `systems/Evaporate/orchestrator/main.py` as documented in the parent README.

Commands below are run from the repository root.

## Build a consolidated table

`build_evaporate_table_from_metadata.py` reads final `*_file2metadata.json` artifacts and writes one document-level CSV. It must not use `*_all_extractions.json`, which belongs to the training/sample phase.

```powershell
python systems/Evaporate/uda_integration/build_evaporate_table_from_metadata.py `
  --input-dir systems/Evaporate/data/finance/generative_indexes/finance `
  --run-prefix <RUN_PREFIX> `
  --output systems/Evaporate/data/finance/results_dumps/evaporate_full_table_from_metadata.csv
```

Arguments:

- `--input-dir`: directory containing the metadata JSON files (required)
- `--output`: destination CSV (required)
- `--run-prefix`: optional prefix used to select one run

The generated CSV contains `doc_id` followed by the union of discovered attributes.

## Execute one projection query

`run_uda_select_queries_on_evaporate.py` selects a 1-based query from a semicolon-separated SQL file and projects its columns from the consolidated CSV.

```powershell
python systems/Evaporate/uda_integration/run_uda_select_queries_on_evaporate.py `
  --input-table systems/Evaporate/data/finance/results_dumps/evaporate_full_table_from_metadata.csv `
  --query-file Query/Finan/Select/select_queries.sql `
  --query-id 1 `
  --output systems/Evaporate/data/finance/results_dumps/select_q1_result.csv
```

The utility supports simple `SELECT column[, ...] FROM table` projections only. It rejects `WHERE`, aggregation/grouping, `ORDER BY`, `LIMIT`, joins and mixed queries. Use the parent orchestrator for the full benchmark workflow.

## Data flow

```text
run_profiler.py
  -> *_file2metadata.json
  -> build_evaporate_table_from_metadata.py
  -> consolidated CSV
  -> run_uda_select_queries_on_evaporate.py
  -> one query-result CSV
```

Evaporate is schema-driven: missing or empty attributes in the extracted metadata remain missing or empty in downstream query results.
