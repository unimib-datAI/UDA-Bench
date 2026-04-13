# DocETL – Esecuzione Pipeline su Documenti (UDA-Bench)

Questa cartella contiene un esempio di esecuzione di **DocETL** su un dataset **non strutturato** (documenti testuali di giocatori NBA), utilizzando modelli LLM (Gemini, OpenAI, Claude).

---

## ⚠️ Nota importante

Questa non è l’esecuzione completa del benchmark UDA-Bench, ma una **pipeline reale su documenti** per:

* verificare il funzionamento di DocETL su dati non strutturati
* testare l’estrazione di informazioni (es. `draft_pick`)
* lo confronta con la ground truth ufficiale

👉 **I dati NON sono inclusi nella repository** (per mantenere il progetto leggero e riproducibile)

---

## 📦 Requisiti

* Python 3.10+
* ambiente virtuale attivo (`.venv` consigliato)

Installare le dipendenze:

```bash
pip install -r requirements.txt
```

---

## 📁 Struttura della cartella

```text
DocETL/
├── real/
│   ├── finance/
│   │   ├── data/
│   │   │   ├── finance_docs.json
│   │   │   └── build_docs_json.py
│   │   │
│   │   ├── generated/              # YAML generati automaticamente
│   │   ├── outputs/                # JSON e CSV prodotti
│   │   ├── eval/                   # risultati evaluation
│   │   │   └── select_q1/
│   │   │       ├── sql.json
│   │   │       └── acc_result/
│   │   │
│   │   ├── generate_yaml.py        # 🔥 genera YAML da SQL
│   │   ├── export_to_csv.py        # 🔥 converte JSON → CSV
│   │   └── build_docs_json.py
│
├── evaluation/                    # UDA evaluation engine
├── Query/                         # query SQL benchmark
└── requirements.txt
```
## 🔑 Configurazione API Key
DocETL usa LiteLLM → serve una chiave.
Metodo consigliato: .env

Crea un file .env nella root:
```
GEMINI_API_KEY=your_key_here
```

---

### Workflow completo (IMPORTANTE)
Workflow completo (IMPORTANTE)

### 1️⃣ Inserire i documenti

Caricare i file `.txt`

Ogni file deve contenere un documento testuale (es. Wikipedia di un giocatore).

---


## 2️⃣ Generare lo YAML automaticamente
Esempio: query 1

```bash
python systems/DocETL/real/finance/generate_yaml.py --sql-file Query/Finan/Select/select_queries.sql --query-id 1
```
Output
```bash
systems/DocETL/real/finance/generated/select_q1.yaml
```

## 3️⃣ Eseguire DocETL
```bash
docetl run systems/DocETL/real/finance/generated/select_q1.yaml
```
Output
```bash
systems/DocETL/real/finance/outputs/select_q1.json
```

## 4️⃣ Convertire JSON → CSV
```bash
python systems/DocETL/real/finance/export_to_csv.py --query-id 1
```
Output
```bash
systems/DocETL/real/finance/outputs/select_q1.csv
```

## 5️⃣ Preparare SQL per evaluation
Creare file:
```bash
systems/DocETL/real/finance/eval/select_q1/sql.json
```
Contenuto
```bash
{
  "sql": "SELECT earnings_per_share, id FROM Finan"
}
```
⚠️ Importante:

* usare Finan (nome dataset UDA)
* NON finance

## 6️⃣Lanciare evaluation
```bash
python -m evaluation.run_eval --dataset Finan --task Select --sql-file systems/DocETL/real/finance/eval/select_q1/sql.json --result-csv systems/DocETL/real/finance/outputs/select_q1.csv --attributes-file Query/Finan/Finan_attributes.json --gt-dir Query/Finan --output-dir systems/DocETL/real/finance/eval/select_q1/acc_result
```

## Leggere i risultati
File:
```bash
acc.json
```
Esempio:
```bash
{
  "macro_precision": 0.40,
  "macro_recall": 0.41,
  "macro_f1": 0.4059
}
```
👉 Questa è la metrica principale del benchmark.
---

## 🔁 Workflow per una nuova query

Per esempio query 10:
```bash
# 1. YAML
python generate_yaml.py --query-id 10

# 2. DocETL
docetl run generated/select_q10.yaml

# 3. CSV
python export_to_csv.py --query-id 10

# 4. crea sql.json

# 5. evaluation
python -m evaluation.run_eval ...
```
## 🧠 Cosa sta succedendo dietro le quinte

Pipeline completa:
```bash
# 1. YAML
python generate_yaml.py --query-id 10

# 2. DocETL
docetl run generated/select_q10.yaml

# 3. CSV
python export_to_csv.py --query-id 10

# 4. crea sql.json

# 5. evaluation
python -m evaluation.run_eval ...
```

## 🧠 Cosa sta succedendo dietro le quinte

Pipeline completa:
```
SQL (UDA)
   ↓
generate_yaml.py
   ↓
DocETL (LLM su documenti)
   ↓
JSON
   ↓
CSV
   ↓
evaluation.run_eval
   ↓
F1 score
```
## ▶️ Esecuzione pipeline (vecchia pipeline su Player)

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
