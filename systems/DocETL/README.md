# DocETL – Esecuzione Pipeline su Documenti (UDA-Bench)

Questa cartella contiene un esempio di esecuzione di **DocETL** su un dataset **non strutturato** (documenti testuali di giocatori NBA), utilizzando modelli LLM (Gemini, OpenAI, Claude).

A differenza della versione iniziale, questa pipeline lavora su:

* documenti `.txt`
* conversione in JSON
* estrazione di informazioni tramite LLM

---

## ⚠️ Nota importante

Questa non è l’esecuzione completa del benchmark UDA-Bench, ma una **pipeline reale su documenti** per:

* verificare il funzionamento di DocETL su dati non strutturati
* testare l’estrazione di informazioni (es. `draft_pick`)
* simulare il comportamento dei sistemi del benchmark

👉 **I dati NON sono inclusi nella repository** (per mantenere il progetto leggero e riproducibile)

---

## 📦 Requisiti

* Python 3.10+
* ambiente virtuale attivo (`.venv` consigliato)

Installare le dipendenze:

```bash
pip install docetl litellm "pyrate-limiter<4"
```

---

## 📁 Struttura della cartella

```text
DocETL/
├── real/
│   ├── data/
│   │   ├── player_docs/              # documenti .txt (NON inclusi)
│   │   ├── player_docs.json          # dataset generato (NON incluso)
│   │   └── build_player_docs_json.py
│   │
│   ├── pipelines/
│   │   └── player_docs_select_q1.yaml
│   │
│   └── outputs/                     # output generati (NON inclusi)
│
├── api.py
├── README.md
└── requirements.txt
```

---

## 📥 Preparazione dei dati

### 1️⃣ Inserire i documenti

Posizionare i file `.txt` in:

```text
systems/DocETL/real/data/player_docs/
```

Ogni file deve contenere un documento testuale (es. Wikipedia di un giocatore).

---

### 2️⃣ Generare il dataset JSON

Eseguire lo script:

```bash
python systems/DocETL/real/data/build_player_docs_json.py
```

Questo script:

* legge tutti i `.txt`
* costruisce un dataset strutturato
* salva:

```text
systems/DocETL/real/data/player_docs.json
```

Formato esempio:

```json
{
  "id": "5",
  "filename": "5.txt",
  "content": "Mark Price ..."
}
```

---

## 🔑 Configurazione API Key

DocETL utilizza **LiteLLM**, quindi il provider dipende dal modello scelto nello YAML.

---

### ✔️ Gemini

```bash
$env:GEMINI_API_KEY="your_api_key"
```

---

### ✔️ OpenAI

```bash
$env:OPENAI_API_KEY="your_api_key"
```

---

### ✔️ Claude

```bash
$env:ANTHROPIC_API_KEY="your_api_key"
```

---

## ⚙️ Configurazione modello

Nel file YAML:

```yaml
default_model: gemini/gemini-2.5-flash
```

Puoi cambiarlo con:

| Provider | Modello esempio         |
| -------- | ----------------------- |
| Gemini   | gemini/gemini-2.5-flash |
| OpenAI   | gpt-4o-mini             |
| Claude   | claude-3-haiku-20240307 |

---

## ▶️ Esecuzione pipeline

Dalla root del progetto:

```bash
docetl run systems/DocETL/real/pipelines/player_docs_select_q1.yaml
```

---

## 📊 Output

Il risultato viene salvato in:

```text
systems/DocETL/real/outputs/player_docs_select_q1.json
```

Formato esempio:

```json
[
  {
    "id": "5",
    "draft_pick": "25th overall"
  },
  {
    "id": "6",
    "draft_pick": ""
  }
]
```

---

## 🔍 Cosa fa questa pipeline

Pipeline DocETL dichiarativa composta da:

### Input

* dataset JSON (`player_docs.json`)
* documenti testuali non strutturati

### Operazione principale

```yaml
type: map
```

Per ogni documento:

* invia il contenuto al modello LLM
* estrae:

  * `id`
  * `draft_pick`

### Output

* record strutturati JSON

---

## 🧠 Comportamento del modello

Regole di estrazione:

* `"undrafted"` se esplicitamente indicato
* stringa vuota se non presente
* nessuna inferenza oltre il testo

👉 Questo rende la pipeline coerente con un task di **information extraction controllata**

---

## ⚠️ Limiti

Questa pipeline:

* esegue un singolo step (`map`)
* non utilizza:

  * join
  * aggregazioni
  * multi-step reasoning
* dipende fortemente dal modello LLM
* può produrre output non normalizzati (es. formati diversi di draft pick)

---

## 🧪 Scopo

Questo setup serve per:

* testare DocETL su documenti reali
* simulare task del benchmark UDA
* confrontare con altri sistemi (QUEST, DQL)

---

## 🚀 Possibili estensioni

* normalizzazione output (es. parsing numerico draft pick)
* pipeline multi-step
* integrazione evaluator (F1 score)
* confronto automatico con ground truth
* supporto multi-query

---

## 📌 Riproducibilità

La repository non include:

* documenti `.txt`
* dataset JSON generati
* output

👉 Per riprodurre i risultati:

1. aggiungere i documenti
2. generare il dataset JSON
3. eseguire la pipeline

---
