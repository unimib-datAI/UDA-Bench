# DocETL Orchestrator (UDA-Bench)

Questa cartella contiene l'integrazione DocETL per UDA-Bench con orchestrazione batch:

- generazione pipeline DocETL per ogni query SQL del dataset
- esecuzione DocETL
- post-processing SQL deterministico sui risultati estratti
- valutazione batch con evaluator ufficiale

L'obiettivo e' ottenere metriche confrontabili con gli altri sistemi del benchmark.

## Requisiti

- Python 3.10+
- ambiente virtuale attivo (es. `.venv-docetl`)
- dipendenze:

```bash
pip install -r systems/DocETL/requirements.txt
```

## Configurazione API key

DocETL usa LiteLLM. Configura almeno una chiave nel file `.env` in root progetto:

```env
GEMINI_API_KEY=...
# oppure OPENAI_API_KEY=...
```

Modello di default orchestrator:

- `gemini/gemini-2.5-flash` (override con `DOCETL_DEFAULT_MODEL`)

## Struttura attuale

```text
systems/DocETL/
  api.py
  README.md
  requirements.txt
  orchestrator/
    main.py
    evaluate_all.py
    yaml_builder.py
    ...
  outputs/
    <dataset>/
      yaml/          # pipeline generate per query
      json/          # output DocETL raw
      csv/           # output finale query-level (post-processed con SQL)
      evaluation/    # acc.json per query + summary.json
```

Input documenti (nuova convenzione):

- `Data/<Dataset>/txt/*.txt`
- esempio: `Data/Finan/txt/*.txt`, `Data/Player/txt/*.txt`

## Workflow completo

### 1) Esecuzione DocETL su tutte le query del dataset

```bash
python systems/DocETL/orchestrator/main.py --dataset Finan
```

Forza rebuild completo:

```bash
python systems/DocETL/orchestrator/main.py --dataset Finan --rebuild
```

Output principali:

- `systems/DocETL/outputs/finan/yaml/*.yaml`
- `systems/DocETL/outputs/finan/json/*.json`
- `systems/DocETL/outputs/finan/csv/*.csv`

Significato pratico:

- `yaml/<query_id>.yaml`
  - cosa contiene: pipeline DocETL generata per una singola query
  - a cosa serve: debug/riproducibilita' del piano (campi estratti, step, modello, output path)
  - quando guardarlo: se una query fallisce in run DocETL o vuoi capire come e' stata tradotta la SQL

- `json/<query_id>.json`
  - cosa contiene: output raw di DocETL per documento (estrazione campo-per-documento)
  - a cosa serve: ispezione qualita' estrazione LLM (valori mancanti, formati sporchi, coerenza tipi)
  - nota: non e' ancora il risultato finale SQL della query

- `csv/<query_id>.csv`
  - cosa contiene: risultato finale query-level usato per benchmark
  - a cosa serve: input diretto dell'evaluator
  - come viene prodotto: post-processing SQL in DuckDB applicato al JSON estratto (quindi include filtri/aggregazioni/proiezioni finali)

### 2) Evaluation batch su tutte le query

```bash
python systems/DocETL/orchestrator/evaluate_all.py --dataset Finan
```

Forza rebuild evaluation:

```bash
python systems/DocETL/orchestrator/evaluate_all.py --dataset Finan --rebuild
```

Evaluation per tipologia query:

```bash
python systems/DocETL/orchestrator/evaluate_all.py --dataset Finan --query-type select
```

Da `main.py` puoi anche lanciare solo evaluation:

```bash
python systems/DocETL/orchestrator/main.py --dataset Finan --eval-only --query-type filter
```

Valori supportati per `--query-type`:

- `all`
- `agg`
- `filter`
- `select`
- `mixed`
- `join`

Output evaluation:

- `systems/DocETL/outputs/finan/evaluation/<query_id>/acc.json`
- `systems/DocETL/outputs/finan/evaluation/summary.json`
- `systems/DocETL/outputs/finan/evaluation/_logs/*.log` (errori)

Significato pratico:

- `evaluation/<query_id>/acc.json`
  - cosa contiene: metriche della singola query (precision/recall/f1 per colonna + macro)
  - a cosa serve: capire quali query/colonne stanno degradando il punteggio

- `evaluation/<query_id>/gold_result.csv`
  - cosa contiene: risultato ground-truth ottenuto eseguendo la SQL sul GT benchmark
  - a cosa serve: riferimento "vero" per confronti puntuali

- `evaluation/<query_id>/matched_result.csv`
  - cosa contiene: risultato modello riallineato alle chiavi di matching
  - a cosa serve: debug di mismatch di righe/chiavi

- `evaluation/<query_id>/matched_gold_result.csv`
  - cosa contiene: ground truth riallineata rispetto al matching con il risultato modello
  - a cosa serve: confronto riga-a-riga coerente con il metodo di scoring

- `evaluation/summary.json`
  - cosa contiene: riepilogo dataset-level (`ok/skip/errori`, `macro_f1_mean`, dettagli per query)
  - a cosa serve: metrica finale da confrontare con altri modelli/sistemi
  - nota: se usi `--query-type <tipo>`, viene scritto `summary_<tipo>.json`

- `evaluation/_logs/<query_id>.log`
  - cosa contiene: stdout/stderr completi in caso di errore evaluation
  - a cosa serve: root-cause analysis rapida quando una query fallisce

## Cosa fa l'orchestrator

Per ogni query SQL:

1. parse SQL e pianificazione campi da estrarre
2. generazione YAML DocETL
3. estrazione LLM su documenti
4. retry automatici su errori transitori (`content_filter`/`No tool calls`)
5. post-processing SQL in DuckDB sul risultato estratto
6. export CSV finale per evaluator

Note tecniche importanti:

- filtri finali applicati in SQL post-processing (deterministico)
- casting numerico guidato da `Query/<Dataset>/*_attributes.json`
- supporto a condizioni complesse (`IN`, `LIKE`, `BETWEEN`)
- iniezione colonne `id` per allineamento corretto con evaluator

## Riesecuzione singola query (consigliata per debug)

Se vuoi rieseguire solo una query:

1. rimuovi i file di quella query in `outputs/<dataset>/json|csv|evaluation`
2. rilancia senza `--rebuild` (il resto viene skippato)

## Note su summary evaluation

Se esegui senza `--rebuild`, molte query risultano `SKIP`.  
Il riepilogo include comunque le metriche lette dagli `acc.json` esistenti.
