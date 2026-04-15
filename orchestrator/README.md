## Meta-Orchestrator (Root Level)

Questo layer permette di orchestrare run cross-modello e cross-dataset da un unico entrypoint:

- DocETL
- Evaporate

con combinazioni flessibili su:

- modello (`single`, `list`, `all`)
- dataset (`single`, `list`, `all`)
- tipo query (`all`, `agg`, `filter`, `select`, `mixed`, `join`)
- modalità (`run`, `eval`, `run+eval`)

## Prerequisiti

- Repo UDA-Bench completa
- Python disponibile in ambiente host
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

## Riuso artefatti esistenti (no rebuild)

Per default, in `--mode eval` non viene forzato rebuild:

- riusa CSV/output/evaluation già presenti nei sistemi
- utile per evitare rerun costosi

Usa i flag `--rebuild*` solo quando vuoi forzare ricalcolo.

## Contenuto di `runs/<run_id>/`

Ogni esecuzione crea:

- `run_manifest.json`
  - configurazione completa input run:
  - `run_id`, `mode`, `models`, `datasets`, `query_types`, flag rebuild, timestamp start

- `events.log`
  - log standardizzato del meta-orchestrator (JSON lines)
  - eventi di pianificazione job, esito job, riepilogo finale

- `jobs/<model>__<dataset>__<query_type>.json`
  - risultato strutturato del singolo job:
  - comando eseguito, status, return code, durata
  - `summary_path` del sistema sottostante
  - `macro_f1_mean` (se disponibile)
  - stdout/stderr tail, timestamp start/end

- `raw_logs/<model>__<dataset>__<query_type>.log`
  - log testuale per debug job
  - include comando e tail stdout/stderr

- `summary.json`
  - riepilogo globale run:
  - `total_jobs`, `ok_jobs`, `error_jobs`
  - `macro_f1_mean_over_jobs`
  - lista completa dei job con metadati

## Nota importante

La cartella `orchestrator/runs/` è il livello di tracking/comparison centralizzato.
Gli output originali dei sistemi restano nelle rispettive cartelle (`systems/DocETL/outputs`, `systems/Evaporate/outputs`) e vengono referenziati dal meta-orchestrator.
