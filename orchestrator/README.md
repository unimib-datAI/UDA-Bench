## Meta-Orchestrator (Root Level)

Questo layer orchestra run cross-modello e cross-dataset da un unico entrypoint:

- DocETL
- Evaporate

Combinazioni supportate:

- modello (`single`, `list`, `all`)
- dataset (`single`, `list`, `all`)
- tipo query (`all`, `agg`, `filter`, `select`, `mixed`, `join`)
- modalita (`run`, `eval`, `run+eval`)

## Prerequisiti

- Repo UDA-Bench completa
- Python disponibile nell'ambiente host
- Virtualenv dei modelli (consigliato):
  - `.venv-docetl`
  - `.venv-evaporate`

Il meta-orchestrator cerca automaticamente i Python dei modelli:

- DocETL: `.venv-docetl/Scripts/python.exe` (o `bin/python`)
- Evaporate: `.venv-evaporate/Scripts/python.exe` (o `bin/python`)

Override manuale opzionale:

```powershell
$env:DOCETL_PYTHON="C:\\path\\to\\docetl\\python.exe"
$env:EVAPORATE_PYTHON="C:\\path\\to\\evaporate\\python.exe"
```

## Comandi rapidi

```powershell
# un modello, un dataset
python orchestrator/main.py --model docetl --dataset Finan --mode run+eval

# tutti i modelli, un dataset, solo evaluation su filter
python orchestrator/main.py --model all --dataset Finan --mode eval --query-type filter

# un modello, tutti i dataset, solo pipeline
python orchestrator/main.py --model evaporate --dataset all --mode run

# lista modelli/dataset disponibili
python orchestrator/main.py --list
```

## Naming delle run

- Se non passi `--run-id`, il nome viene generato automaticamente: `run_YYYYMMDD_HHMMSS`
- Se passi `--run-id`, usi un nome personalizzato (es. `test_all_eval_sf`)

Esempi:

```powershell
# naming automatico
python orchestrator/main.py --model docetl --dataset Finan --mode eval

# naming custom
python orchestrator/main.py --model docetl --dataset Finan --mode eval --run-id test_docetl_eval_select
```

## Riuso artefatti esistenti (no rebuild)

Per default, in `--mode eval` non viene forzato rebuild:

- riusa CSV/output/evaluation gia presenti nei sistemi
- evita rerun costosi

Usa i flag `--rebuild*` solo quando vuoi forzare ricalcolo.

## Contenuto di `runs/<run_id>/`

Ogni run crea una struttura standardizzata:

- `manifest/run_manifest.json`
  - configurazione completa input run
  - modelli/dataset/query-type selezionati, modalita, flag rebuild
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
  - include (quando disponibili) CSV query, YAML/JSON (DocETL), summary e artefatti evaluation

- `summary.json`
  - copia di compatibilita del summary globale (stesso contenuto di `metrics/summary.json`)

## Nota importante

`orchestrator/runs/` e il livello centralizzato per tracking e confronto tra run.
Gli output nativi dei sistemi restano nelle rispettive cartelle (`systems/DocETL/outputs`, `systems/Evaporate/outputs`) e vengono referenziati dal meta-orchestrator.

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
