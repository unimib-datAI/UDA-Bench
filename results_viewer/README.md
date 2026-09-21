# Results viewer

App Streamlit per esplorare i risultati Finance di UDA-Bench: per ogni query SQL mostra GT, risposte e score dei modelli.
La vista Note dell'app mostra questo file e `note_valutazione.md` letti dal disco, così il testo non è duplicato nel codice.

## Avvio

Solo via Docker, dalla root del repo:

```bash
git pull
docker compose -f results_viewer/docker-compose.yml up -d --build
```

- Si rilancia con `--build` dopo ogni modifica al codice o allo zip: l'immagine contiene codice e dati.
- Log: `docker compose -f results_viewer/docker-compose.yml logs -f`; stop: `... down`.
- L'app ascolta solo su `127.0.0.1:8501`, perché Streamlit non ha login. In locale si apre http://localhost:8501; da un server remoto prima `ssh -L 8501:localhost:8501 <server>`.

## Dati

- Query: `Query/Finan/*/*.sql`.
- Risultati: archiviati in `data/uda_outputs.zip` (output completi dei sistemi, circa 360 MB scompattati). La build ne scompatta solo `evaluation/` e `csv/` in `data/uda_outputs/` dentro l'immagine, e l'app li legge in sola lettura.
- Nomi delle sorgenti: `exact match` o `LLM match` dice se la valutazione ha usato il giudice LLM; `string` o `number` dice se il GT confronta i numeri come testo o come numeri, cioè evaluator prima o dopo il commit `047aea3`.
- Sorgenti: dizionario `MODELS` in `config.py`. Ogni voce ha la cartella `root` e due template di percorso: `eval` (cartella con `acc.json`) e `answer` (CSV della risposta), con i segnaposto `{cat}`, `{n}` e `{stem}` (nome del file SQL della categoria).

## Metriche

- `P`, `R`, `F1`: macro precision, recall e F1 di `acc.json`, mediate sulle query.
- `F1_adj`: F1 = 1 quando GT e risposta sono entrambi vuoti, come nella tesi; `EE` conta questi casi.
- Il confronto riga per riga evidenzia le celle diverse dal GT con un confronto testuale: il verdetto LLM per singola cella non è salvato dall'evaluator.
