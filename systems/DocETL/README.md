# DocETL – Esecuzione Pipeline di Esempio

Questa cartella contiene un esempio minimale di esecuzione di **DocETL** su un dataset strutturato (`player.csv`) utilizzando un modello LLM (es. Gemini, OpenAI, Claude).

⚠️ **Nota importante**
Questa non è l’esecuzione completa del benchmark UDA-Bench, ma un **test minimale (smoke test)** per verificare che:

* DocETL funzioni correttamente
* il modello LLM sia configurato
* la pipeline YAML venga eseguita senza errori

---

## 📦 Requisiti

* Python 3.10+
* ambiente virtuale attivo (`.venv` consigliato)

Installare le dipendenze:

```bash
pip install docetl litellm "pyrate-limiter<4"
```

---

## 🔑 Configurazione API Key

DocETL usa **LiteLLM**, quindi il provider dipende dal modello scelto nello YAML.

### ✔️ Gemini (Google)

```bash
setx GEMINI_API_KEY "your_api_key"
```

oppure in sessione corrente:

```bash
$env:GEMINI_API_KEY="your_api_key"
```

---

### ✔️ OpenAI

```bash
setx OPENAI_API_KEY "your_api_key"
```

---

### ✔️ Claude (Anthropic)

```bash
setx ANTHROPIC_API_KEY "your_api_key"
```

---

## ⚙️ Configurazione modello

Nel file YAML:

```yaml
default_model: gemini/gemini-2.5-flash
```

Puoi cambiarlo con:

| Provider | Esempio                   |
| -------- | ------------------------- |
| Gemini   | `gemini/gemini-2.5-flash` |
| OpenAI   | `gpt-4o-mini`             |
| Claude   | `claude-3-haiku-20240307` |

---

## ▶️ Esecuzione pipeline

Dalla root del progetto:

```bash
docetl run systems/DocETL/real/pipelines/player_select_q1.yaml
```

---

## 📊 Output

Il risultato viene salvato in:

```text
systems/DocETL/real/outputs/player_select_q1.json
```

Formato esempio:

```json
[
  {
    "id": "1",
    "draft_pick": ""
  }
]
```

---

## 🔍 Cosa fa questa pipeline

Pipeline molto semplice:

1. legge il dataset `player.csv`
2. per ogni riga:

   * invia i dati al modello LLM
   * estrae `id` e `draft_pick`
3. salva il risultato in JSON

Tipo operazione:

```yaml
type: map
```

👉 significa: **una chiamata LLM per ogni riga**

---

## ⚠️ Limiti di questo esempio

Questa pipeline:

* usa solo dati strutturati (CSV)
* non usa documenti testuali
* non include join, filter o aggregazioni
* esegue un singolo step

👉 quindi è **molto più semplice** rispetto al benchmark UDA-Bench completo.

---

## 🧪 Scopo

Questo esempio serve per:

* verificare setup ambiente
* testare integrazione LLM
* comprendere il funzionamento base di DocETL

---

## 🚀 Prossimi passi

Per un uso più avanzato:

* eseguire più query (non solo Q1)
* costruire pipeline multi-step
* integrare evaluation (F1 score)
* confrontare con altri sistemi (QUEST, DQL, ecc.)

---
