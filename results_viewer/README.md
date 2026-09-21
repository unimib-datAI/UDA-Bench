# Results viewer

App Streamlit per esplorare i risultati Finance di UDA-Bench: per ogni query SQL mostra GT, risposte e score dei modelli.
La vista Note dell'app mostra questo file e `note_valutazione.md` letti dal disco, così il testo non è duplicato nel codice.

## Avvio

Dalla root del repo:

```bash
python3 -m venv results_viewer/.venv
results_viewer/.venv/bin/pip install -r results_viewer/requirements.txt
unzip -q results_viewer/data/uda_outputs.zip -d results_viewer/data
results_viewer/.venv/bin/streamlit run results_viewer/app.py
```

## Dati

- Query: `Query/Finan/*/*.sql`.
- Risultati: archiviati in `data/uda_outputs.zip` (output completi dei sistemi, circa 360 MB scompattati) e letti in sola lettura da `data/uda_outputs/`, che non è versionata; per un'altra cartella imposta `UDA_BACKUP_DIR`. Dopo un pull che aggiorna lo zip, cancella `data/uda_outputs/` e scompatta di nuovo.
- Nomi delle sorgenti: `exact match` o `LLM match` dice se la valutazione ha usato il giudice LLM; `string` o `number` dice se il GT confronta i numeri come testo o come numeri, cioè evaluator prima o dopo il commit `047aea3`.
- Sorgenti: dizionario `MODELS` in `config.py`. Ogni voce ha la cartella `root` e due template di percorso: `eval` (cartella con `acc.json`) e `answer` (CSV della risposta), con i segnaposto `{cat}`, `{n}` e `{stem}` (nome del file SQL della categoria).

## Metriche

- `P`, `R`, `F1`: macro precision, recall e F1 di `acc.json`, mediate sulle query.
- `F1_adj`: F1 = 1 quando GT e risposta sono entrambi vuoti, come nella tesi; `EE` conta questi casi.
- Il confronto riga per riga evidenzia le celle diverse dal GT con un confronto testuale: il verdetto LLM per singola cella non è salvato dall'evaluator.
