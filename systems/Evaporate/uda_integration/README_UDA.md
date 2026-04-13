# Evaporate × UDA Integration

This folder contains only the scripts used to connect the original Evaporate pipeline with UDA-Benchmark queries and outputs.

## Folder purpose

The original Evaporate code remains in `systems/Evaporate/`.

This folder contains only the integration layer needed to:

1. run Evaporate on the dataset
2. build a document-level table from Evaporate outputs
3. execute UDA `SELECT` queries on that table

---

## Original Evaporate files

These are part of the original Evaporate pipeline and should be considered the core system:

- `run_profiler.py`
- `profiler.py`
- `utils.py`
- `profiler_utils.py`
- `evaluate_synthetic.py`
- `evaluate_profiler.py`
- `schema_identification.py`
- `configs.py`
- `prompts.py`

---

## Integration scripts kept here

### 1. `build_evaporate_table_from_metadata.py`

Builds a full CSV table from Evaporate final metadata files:

- input: `*_file2metadata.json`
- output: `evaporate_full_table_from_metadata.csv`

Important:
Do **not** use `*_all_extractions.json` for the final table, because those files are tied to the training/sample phase.
The correct files for full-dataset reconstruction are `*_file2metadata.json`.

Example:

```bash
python uda_integration/build_evaporate_table_from_metadata.py --input-dir data/finance/generative_indexes/finance --run-prefix dlfinance_d04032026_fs100_ts5_k1_cs2000_rt0_b0_c0_ub1_m1 --output data/finance/results_dumps/evaporate_full_table_from_metadata.csv
```

## 2. run_uda_select_queries_on_evaporate.py

Reads a multi-query SQL file from UDA-Benchmark, selects one query by index, and runs it on the Evaporate full table.

Current support:

* SELECT col1 FROM finance
* SELECT col1, col2, ... FROM finance

Not yet supported:

* WHERE
* AGG
* FILTER
* MIXED
* ORDER BY
* LIMIT

Example:
```
python uda_integration/run_uda_select_queries_on_evaporate.py --input-table data/finance/results_dumps/evaporate_full_table_from_metadata.csv --query-file ..\..\Query\Finan\Select\select_queries.sql --query-id 1 --output data/finance/results_dumps/select_q1_result.csv
```

## Recommended workflow
Step 1 — run Evaporate

From systems/Evaporate/:
```
python run_profiler.py
```
This produces Evaporate outputs such as:

* *_all_extractions.json
* *_file2metadata.json
* *_all_metrics.json

For the final UDA table, use only:

* *_file2metadata.json

Step 2 — build the full table
```
python uda_integration/build_evaporate_table_from_metadata.py --input-dir data/finance/generative_indexes/finance --run-prefix <RUN_PREFIX> --output data/finance/results_dumps/evaporate_full_table_from_metadata.csv
```
Step 3 — execute one UDA SELECT query
```
python uda_integration/run_uda_select_queries_on_evaporate.py --input-table data/finance/results_dumps/evaporate_full_table_from_metadata.csv --query-file ..\..\Query\Finan\Select\select_queries.sql --query-id <ID> --output data/finance/results_dumps/select_q<ID>_result.csv
```
## Important observations

* Evaporate is schema-driven, not query-driven.
* Queries are applied after extraction, on the reconstructed table.
* Some attributes may be completely empty even if the pipeline runs correctly.
* This is an extraction-quality issue, not necessarily a bug in the wrappers.
* In the current finance run, some attributes are missing entirely, for example:
    - bussiness_cost
    - environmental_compliance

So some UDA queries may fail because the required column is not available.

## Scratch folder

Older exploratory/debug scripts were moved to:

systems/Evaporate/scratch/

These are not part of the final intended workflow.

## Current final pipeline

run_profiler.py

→ *_file2metadata.json

→ build_evaporate_table_from_metadata.py

→ evaporate_full_table_from_metadata.csv

→ run_uda_select_queries_on_evaporate.py

→ query result CSV
'@ | Set-Content systems\Evaporate\uda_integration\README_UDA.md