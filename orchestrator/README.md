## Meta-Orchestrator (Root Level)

Questo layer orchestra run cross-modello e cross-dataset da un unico entrypoint:

- DocETL
- Evaporate
- Lotus
- Quest
- DQL

Combinazioni supportate:

- modello (`single`, `list`, `all`)
- dataset (`single`, `list`, `all`)
- tipo query (`all`, `agg`, `filter`, `select`, `mixed`, `join`)
- modalità (`run`, `eval`, `run+eval`)

## Prerequisiti

- Repo UDA-Bench completa
- Python disponibile nell'ambiente host
- Virtualenv dei modelli (consigliato):
  - `.venv-docetl`
  - `.venv-evaporate`
  - `.venv-lotus`
  - `.venv-quest`
  - `.venv-DQL`

Il meta-orchestrator cerca automaticamente gli interpreti Python dei modelli:

- DocETL: `.venv-docetl/Scripts/python.exe` (o `bin/python`)
- Evaporate: `.venv-evaporate/Scripts/python.exe` (o `bin/python`)
- Lotus: `.venv-lotus/Scripts/python.exe` (o `bin/python`)
- Quest: `.venv-quest/Scripts/python.exe` (o `bin/python`)
- DQL: `.venv-DQL/Scripts/python.exe` (o `bin/python`)

Override manuale opzionale:

```powershell
$env:DOCETL_PYTHON="C:\\path\\to\\docetl\\python.exe"
$env:EVAPORATE_PYTHON="C:\\path\\to\\evaporate\\python.exe"
$env:LOTUS_PYTHON="C:\\path\\to\\lotus\\python.exe"
$env:QUEST_PYTHON="C:\\path\\to\\quest\\python.exe"
$env:DQL_PYTHON="C:\\path\\to\\dql\\python.exe"
# opzionale: endpoint API DQL
$env:DQL_API_URL="http://127.0.0.1:8000/api/v2/chat"
```

## Comandi rapidi

```powershell
# un modello, un dataset
python orchestrator/main.py --model docetl --dataset Finan --mode run+eval

# DQL (run only)
python orchestrator/main.py --model dql --dataset Finan --query-type select --mode run

# DQL (run + eval dal meta-orchestrator)
python orchestrator/main.py --model dql --dataset Finan --query-type select --mode run+eval

# DQL (run + eval su mixed)
python orchestrator/main.py --model dql --dataset Finan --query-type mixed --mode run+eval --rebuild --rebuild-eval --run-id dql_finan_mixed

# retry solo query/artifact mancanti o falliti (cross-modello)
python orchestrator/main.py --model dql --dataset Finan --query-type select --mode run+eval --retry-failed

# tutti i modelli, un dataset, solo evaluation su filter
python orchestrator/main.py --model all --dataset Finan --mode eval --query-type filter

# un modello, tutti i dataset, solo pipeline
python orchestrator/main.py --model evaporate --dataset all --mode run

# lista modelli/dataset disponibili
python orchestrator/main.py --list
```

## Naming delle run

- Se non passi `--run-id`, il nome viene generato automaticamente in modo descrittivo:
  `<models>_<datasets>_<query_types>_YYYYMMDD_HHMMSS` (UTC)
  - esempio: `docetl_finan_select_20260422_184501`
- Se passi `--run-id`, usi un nome personalizzato (es. `test_all_eval_sf`)

Esempi:

```powershell
# naming automatico
python orchestrator/main.py --model docetl --dataset Finan --mode eval

# naming custom
python orchestrator/main.py --model docetl --dataset Finan --mode eval --run-id test_docetl_eval_select
```

## Riuso artefatti esistenti (no rebuild)

Per impostazione predefinita, in `--mode eval` non viene forzato il rebuild:

- riusa CSV/output/evaluation già presenti nei sistemi
- evita rerun costosi

Usa i flag `--rebuild*` solo quando vuoi forzare ricalcolo.

## Retry solo query fallite

Per tutti i modelli puoi usare:

```powershell
python orchestrator/main.py --model <model> --dataset <dataset> --query-type <type> --mode run+eval --retry-failed
```

Comportamento:

- non forza rebuild (`--rebuild*` vengono disattivati)
- riesegue solo query senza output utili
- riesegue evaluation solo dove manca `acc.json`

Per DQL puoi aumentare il timeout request API, ad esempio a 30 minuti:

```powershell
$env:DQL_REQUEST_TIMEOUT="1800"
python orchestrator/main.py --model dql --dataset Finan --query-type select --mode run+eval --retry-failed
```

## Contenuto di `runs/<run_id>/`

Ogni run crea una struttura standardizzata:

- `manifest/run_manifest.json`
  - configurazione completa input run
  - modelli/dataset/query-type selezionati, modalità, flag rebuild
  - puntatori agli artifact principali della run

- `queries/`
  - snapshot dei file SQL usati nella run, copiati da `Query/<dataset>/...`
  - organizzati per dataset e sottocartelle originali

- `queries/queries_index.jsonl`
  - indice query-level con un record per query SQL
  - campi principali: `dataset`, `query_type`, `query_id`, `source_file`, `snapshot_file`, `sql`

- `jobs/<model>__<dataset>__<query_type>.json`
  - risultato strutturato del singolo job
  - comando, status, return code, durata
  - `summary_path` del sistema sottostante e `macro_f1_mean` (se disponibile)
  - stdout/stderr tail, timestamp start/end

- `logs/events.log`
  - log standardizzato del meta-orchestrator (JSON lines)
  - pianificazione job, esito job, riepilogo finale

- `logs/raw/<model>__<dataset>__<query_type>.log`
  - log testuale per debug job (comando + tail stdout/stderr)

- `metrics/summary.json`
  - riepilogo globale run
  - `total_jobs`, `ok_jobs`, `error_jobs`, `macro_f1_mean_over_jobs`
  - dettaglio job completo

- `outputs/`
  - copia materializzata degli output del sistema eseguito, isolata per run:
  - `outputs/<model>/<dataset>/<query_type>/...`
  - include (quando disponibili) CSV delle query, YAML/JSON (DocETL), summary e artefatti di evaluation

- `summary.json`
  - copia di compatibilità del summary globale (stesso contenuto di `metrics/summary.json`)

## Nota importante

`orchestrator/runs/` è il livello centralizzato per il tracking e il confronto tra run.
Gli output nativi dei sistemi restano nelle rispettive cartelle (`systems/DocETL/outputs`, `systems/Evaporate/outputs`) e vengono referenziati dal meta-orchestrator.

## Nota DQL (conversione JSON -> CSV)

Per DQL, il meta-orchestrator esegue la stessa `evaluation.run_eval` usata dagli altri modelli.

- Se `systems/DQL/.../results.json` contiene righe tabellari (`rows/data/results/items/records`), l'adapter genera `results.csv` da quelle righe.
- Se l'output DQL è narrativo/non tabellare, l'adapter genera comunque un `results.csv` compatibile con l'evaluator (colonne richieste + `id`) per mantenere il flusso `run+eval` unificato da un unico entrypoint.
- Dopo l'evaluation, l'adapter replica gli artifact query-level (`acc.json`, `gold_result.csv`, `matched_*.csv`) in cartelle `evaluation/<query_name>` con naming allineato a DocETL/Evaporate.
- Per allineamento struttura con DocETL/Evaporate, DQL espone query CSV/evaluation nella struttura flat:
  - `systems/DQL/outputs/<dataset>/csv/<query_name>.csv`
  - `systems/DQL/outputs/<dataset>/evaluation/<query_name>/...`
- I dettagli query-level usati durante l'esecuzione DQL sono mantenuti in:
  - `systems/DQL/outputs/<dataset>/_runtime/<query_type>/query_<n>/...`
  (internal path, non parte della struttura di output da confrontare tra modelli).

In questo modo DQL resta integrato nel flusso generale dell'orchestrator senza script esterni.

Nota tecnica (mixed/agg):
- durante `run+eval`, l'adapter DQL applica una normalizzazione SQL solo lato evaluation per evitare errori DuckDB su colonne numeriche salvate come testo:
  - `AVG(col)` -> `AVG(TRY_CAST(col AS DOUBLE))`
  - `SUM(col)` -> `SUM(TRY_CAST(col AS DOUBLE))`

## Report CSV cross-run

Per confrontare rapidamente run diverse:

```powershell
# report completo
python orchestrator/report.py

# solo dataset Finan
python orchestrator/report.py --dataset Finan

# solo select/filter per docetl+evaporate
python orchestrator/report.py --model docetl,evaporate --dataset Finan --query-type select,filter

# solo alcune run
python orchestrator/report.py --run-id test_docetl_eval_select,test_evaporate_eval_select
```

Output default: `orchestrator/reports/runs_report.csv`

## Final HTML report (SELECT)

Generate:

```powershell
.\.venv-DQL\Scripts\python.exe orchestrator/analysis/select_eval_report.py --dataset Finan --output orchestrator/analysis/select_report.html
```

Open from command line:

```powershell
start .\orchestrator\analysis\select_report.html
```

## Top-k HTML report (multi-task: SELECT/AGG/MIXED)

Generate:

```powershell
.\.venv-DQL\Scripts\python.exe orchestrator/analysis/select_topk_compare.py --dataset Finan --tasks select,agg,mixed --topk 6 --include-lotus-from-benchmark --output orchestrator/analysis/select_top6_compare.html
```
