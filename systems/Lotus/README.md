# 🚀 Lotus – Quick Start Guide

This guide explains how to set up the project and run a test execution.

---

## 📂 Setup & Run

From the root of the repository (`UDA-Bench`):

### 1. Navigate to the project directory

```bash
cd systems/Lotus
```

---

### 2. Create and activate the Conda environment

```bash
conda create -n lotus_env python=3.10.16
conda activate lotus_env
```

---

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Configure the `.env` file

Copy `.env.example` to `.env` in `systems/Lotus/` and set `GEMINI_API_KEY`. When Lotus is launched by the root orchestrator, the repository-root `.env` is loaded by the parent process instead.

---

### 6. Run a query

```bash
python main.py \
  --sql "<your_sql_query>" \
  [--limit <num_rows>] \
  [--cascade] \
  [--out_dir <output_directory>]
```

#### Parameters

* `--sql`
  SQL query (o lista di query) da eseguire.

* `--limit` *(optional)*
  Limita il numero di righe del dataset da processare.
  Default: `-1` (nessun limite).

* `--cascade` *(optional)*
  Abilita la strategia di *LM cascade*.

* `--out_dir` *(optional)*
  Directory base degli output. Se il basename non contiene `query_`, il runner aggiunge `query_<n>` per ogni SQL.

---

### 7. Output

By default, each invocation writes `systems/Lotus/results/<timestamp>/query_<n>/results.csv`. A custom `--out_dir` changes only the base directory.

---

## 🤖 Models Used

### Large Language Models (LLMs)
- **Primary LLM**: `gemini/gemini-2.5-flash` (Google Gemini 2.5 Flash)
- **Cascade Mode**: Uses `MODEL_MINI` (gemini/gemini-2.5-flash) and `MODEL_PRO` (gemini/gemini-3-flash-preview) for the cascade strategy
