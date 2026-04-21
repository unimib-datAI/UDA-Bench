# Evaluation Benchmark: Contratto Operativo

Questo documento descrive in modo pratico:
- cosa si aspetta in input l'evaluation;
- cosa fa internamente (step reali);
- cosa produce in output;
- cosa deve fare un nuovo sistema per essere valutabile senza modificare il core.

## 1) Entrypoint principale

File: `evaluation/run_eval.py`

Comando base:

```powershell
python -m evaluation.run_eval `
  --dataset Finan `
  --task Select `
  --sql-file Query/Finan/Select/select_queries.sql `
  --result-csv path\to\result.csv `
  --attributes-file Query/Finan/Finan_attributes.json `
  --gt-dir Query/Finan `
  --output-dir path\to\acc_result
```

Argomenti chiave:
- `--dataset`: nome dataset (usato per default path).
- `--sql-file`: file SQL raw o `sql.json` con chiave `{"sql": "..."}`.
- `--result-csv`: CSV prodotto dal sistema da valutare (**input della valutazione**).
- `--attributes-file`: file `*_attributes.json` del dataset.
- `--gt-dir`: directory con GT CSV (tipicamente `Query/<dataset>`).
- `--output-dir`: directory di output evaluation.

Argomenti opzionali rilevanti:
- `--primary-key`: chiave secondaria per casi multi-entità.
- `--float-tolerance`: tolleranza assoluta per confronti numerici.
- `--multi-value-sep` (default `||`): separatore per campi multi-valore.
- `--llm-provider`, `--llm-model`: matching semantico per stringhe/chiavi.
- `--semantic-join*`: abilita/controlla join semantico durante esecuzione GT.

## 2) Input attesi (contratto)

## 2.1 Ground truth (GT)

In `--gt-dir` devono esserci CSV tabellari con nome tabella = nome file:
- `Query/<dataset>/<table>.csv`

L'SQL deve referenziare tabelle esistenti in quella directory (stesso stem file).

## 2.2 Metadata attributi

`--attributes-file` deve puntare a `*_attributes.json` con struttura per tabella/colonna.
I campi usati dal core sono soprattutto:
- `value_type` (`str`, `int`, `float`, `multi_str`, ...)
- `description` (usata per LLM semantic match)

## 2.3 SQL

Supportati:
- file `.sql` con query;
- file `.json` con campo `sql`.

La query viene parse-ata per:
- inferire output columns;
- inferire chiavi primarie di allineamento;
- classificare il tipo query (`select_filter`, `aggregation`, `join`);
- determinare stop columns (tipicamente colonne id).

Nota: per query join, il parser può aggiungere alias mancanti (`table.column`) via `tools/sql_aliaser.py`.

## 2.4 CSV risultato del sistema (fondamentale)

`--result-csv` deve contenere almeno i campi richiesti dalla SELECT target.
Il loader normalizza automaticamente:
- rimozione colonne `Unnamed*`;
- normalizzazione nomi colonna;
- conversione `file_name`/`filename` in `id` (anche `table.file_name -> table.id`);
- aggiunta colonne mancanti con `None`.

Per allineamento robusto:
- per query row-level (`Select/Filter`) conviene includere `id`;
- per join conviene includere id qualificati coerenti (`table.id`) quando previsti.

## 3) Cosa fa internamente l'evaluation (pipeline reale)

Sequenza in `run_eval.py`:

1. **Parse SQL + manifest**
   - `tools/query_manifest.py`, `tools/sql_parser.py`, `tools/sql_aliaser.py`
2. **Esecuzione GT SQL**
   - `tools/gt_runner.py`
   - opzionale semantic join in `tools/semantic_join.py`
3. **Load e normalizzazione prediction CSV**
   - `tools/result_loader.py`, `tools/utils.py`
4. **Allineamento righe pred/gt**
   - `tools/row_matcher.py`
   - usa primary keys inferite; opzionale LLM key matching
5. **Normalizzazione empty/null**
   - `tools/utils.normalize_empty_cells`
6. **Calcolo metriche**
   - `tools/metrics.py` + comparatori in `tools/comparators.py`
7. **Scrittura output**
   - `tools/result_writer.py`

## 4) Output prodotti

Dentro `--output-dir`:
- `gold_result.csv`: risultato GT SQL (post-processato).
- `matched_gold_result.csv`: GT allineato su prediction.
- `matched_result.csv`: prediction allineata su GT.
- `acc.json`: metriche finali.

Nota importante:
- `result.csv`/`--result-csv` **non** viene generato da `run_eval.py`.
- `result.csv` è l'output del sistema da valutare e deve esistere prima dell'esecuzione.
- `run_eval.py` genera invece i file dentro `--output-dir` (`gold_result.csv`, `matched_*`, `acc.json`).

Campi principali in `acc.json`:
- `columns`: metriche per colonna (`precision`, `recall`, `f1`)
- `macro_precision`, `macro_recall`, `macro_f1`
- `rows` (`len_gold`, `len_pred`, `matched_rows`)
- `used_keys`, `warnings`
- `evaluated_columns`, `skipped_columns`

## 5) Regole metriche (sintesi utile)

Comparatori in `tools/comparators.py`:
- **NumericComparator**: confronto numerico esatto o con `float_tolerance`.
- **AggComparator**: score continuo basato su errore relativo.
- **StringLLMComparator**: match lessicale normalizzato + opzionale LLM.
- **MultiValueComparator**: split con `multi_value_sep`, match termini (LLM opzionale).

Macro score: media semplice dei punteggi colonna (`tools/metrics.py`).

## 6) Integrazione di un nuovo sistema (checklist pratica)

Per rendere un sistema valutabile:

1. Esegui query e salva un CSV per query.
2. Passa a `run_eval.py`:
   - SQL della query (`--sql-file`)
   - CSV del sistema (`--result-csv`, già prodotto a monte dal sistema)
   - attributes + GT dir del dataset.
3. Assicurati che i nomi colonna risultanti siano coerenti con la SELECT.
4. Per Select/Filter includi `id` (o almeno `file_name/filename` da cui derivarlo).
5. Per Join, preserva id qualificati quando necessario (`table.id`).
6. Usa `--output-dir` separato per ogni query (evita overwrite).

In pratica, il contratto minimo è:
- SQL valido su GT;
- CSV prediction con colonne mappabili;
- metadata attributi presenti.

## 7) Configurazioni opzionali (LLM / embedding)

- `evaluation/conf/template_api_key.yaml`:
  struttura per provider (`api_key`, `api_base`) letta da `tools/load_api_keys.py`.
- `config/embedding_model.yaml`:
  usato da `tools/text_embedding.py` per semantic join vector prefilter.

Se LLM non disponibile:
- il sistema degrada a matching deterministico/lessicale (non blocca la run).

## 8) Mappa file-per-file

Core runtime:
- `run_eval.py`: orchestrazione end-to-end per singola query.
- `sql_preprocessor.py`: utility per split multi-SQL in cartelle query-by-query.

Tools:
- `tools/config.py`: dataclass settings/path + load/dump json.
- `tools/query_manifest.py`: bundle SQL parsed + attributes.
- `tools/sql_parser.py`: parse SQL, detect query type, keys, stop columns.
- `tools/sql_aliaser.py`: alias automatici per join columns senza alias.
- `tools/gt_runner.py`: registra GT CSV in DuckDB ed esegue SQL.
- `tools/result_loader.py`: carica e normalizza CSV prediction.
- `tools/row_matcher.py`: allinea righe GT/pred su keys (+ LLM fallback).
- `tools/comparators.py`: comparatori cella per tipo dato.
- `tools/metrics.py`: metriche per colonna e macro.
- `tools/result_writer.py`: scrive CSV allineati + `acc.json`.
- `tools/semantic_join.py`: augmentation semantica per join su GT.
- `tools/text_embedding.py`: wrapper embedding litellm.
- `tools/utils.py`: normalizzazioni comuni.
- `tools/load_api_keys.py`: lettura API keys provider.
- `tools/logging_utils.py`: logger comune.
- `tools/normalize_result.py`: helper compatibilità per normalizzazione output.

Supporto:
- `dev_test_empty_normalization.py`: test dev locale su normalizzazione vuoti.

## 9) Errori tipici da evitare

- Tabelle SQL non presenti in `Query/<dataset>/*.csv`.
- `--attributes-file` mancante o non coerente.
- CSV prediction senza colonne minime della SELECT.
- Join con chiavi ambigue/non tracciabili in output.
- Uso LLM senza chiavi configurate (degrada, ma può ridurre qualità matching).
