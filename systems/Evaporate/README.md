# Evaporate in UDA-Bench

Questa cartella contiene:

- il sistema Evaporate originale (`run_profiler.py`, `profiler.py`, ecc.)
- un orchestrator UDA-Bench per esecuzioni batch su tutte le query di un dataset
- la valutazione batch integrata con il benchmark

## Cosa fa il nuovo orchestrator

Dato un dataset (es. `Finan`), l'orchestrator:

1. esegue/riprende l'estrazione Evaporate
2. costruisce la tabella unificata `evaporate_full_table.csv`
3. esegue tutte le query SQL del dataset
4. salva un CSV risultato per query
5. lancia evaluation su tutte le query e aggrega le metriche finali

## Struttura principale

- `orchestrator/main.py`: entrypoint unico (pipeline + evaluation)
- `orchestrator/runner.py`: run batch query e export CSV
- `orchestrator/evaluate_all.py`: evaluation batch su tutte le query
- `orchestrator/query_loader.py`: caricamento query SQL del benchmark
- `orchestrator/utils.py`: utility comuni

## Prerequisiti

Da root repo:

```powershell
python -m venv .venv-evaporate
.\.venv-evaporate\Scripts\Activate.ps1
pip install -r systems/Evaporate/requirements_evaporate.txt
```

API key (se usi provider LLM che la richiede):

```powershell
$env:TOGETHER_API_KEY="your_key"
```

## Input attesi

Dataset in:

- `Data/<Dataset>/txt/` documenti `.txt` (uno per file)
- `Data/<Dataset>/table.json` schema/ground truth tabellare (se assente, viene generato da CSV quando possibile)

Query SQL benchmark in `queries/` (gestite via loader orchestrator).

## Comandi principali

### Pipeline completa + evaluation

```powershell
python systems/Evaporate/orchestrator/main.py --dataset Finan
```

### Rebuild completo (estrazione, tabella, CSV query, evaluation)

```powershell
python systems/Evaporate/orchestrator/main.py --dataset Finan --rebuild-extract --rebuild-table --rebuild --rebuild-eval
```

### Solo pipeline (senza evaluation)

```powershell
python systems/Evaporate/orchestrator/main.py --dataset Finan --skip-eval
```

### Solo evaluation batch

```powershell
python systems/Evaporate/orchestrator/evaluate_all.py --dataset Finan --rebuild
```

## Parametri utili

`main.py` espone anche:

- `--model` (default `gemini-2.5-flash`)
- `--train-size` (default `20`)
- `--num-top-k-scripts` (default `2`)
- `--chunk-size` (default `2000`)
- `--max-chunks-per-file` (default `3`)

Per forcing reale del rebuild estrazione usa `--rebuild-extract` (propaga `--overwrite_cache` internamente).

## Output prodotti

Per dataset `Finan`:

- `systems/Evaporate/outputs/finan/evaporate_full_table.csv`
  - tabella unificata prodotta da metadata Evaporate, base per esecuzione SQL

- `systems/Evaporate/outputs/finan/csv/<query_id>.csv`
  - risultato query-by-query, pronto per evaluation

- `systems/Evaporate/outputs/finan/evaluation/<query_id>/acc.json`
  - metriche per singola query

- `systems/Evaporate/outputs/finan/evaluation/summary.json`
  - riepilogo globale (`macro_f1_mean`, ok/skip/error, dettagli per query)

- `systems/Evaporate/outputs/finan/evaluation/_logs/`
  - log errori/debug evaluation per query

## Note importanti

- Gli output sotto `systems/Evaporate/outputs/` sono artefatti locali e non vanno versionati.
- `systems/Evaporate/function_cache/` contiene cache funzioni locali.
- `.gitignore` include regole per evitare commit di output/cache.

## Obiettivo del flusso

Confrontare Evaporate con gli altri sistemi UDA-Bench in modo uniforme:

- stessa suite di query
- stesso evaluator
- metriche aggregate comparabili su tutti i dataset
