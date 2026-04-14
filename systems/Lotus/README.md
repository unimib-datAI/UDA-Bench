# 🚀 Lotus – Quick Start Guide

This guide explains how to set up the project and run a test execution.

---

## 📂 Setup & Run

From the root of the repository (`UDA-Bench`):

### 1. Navigate to the project directory

```bash
cd systems/lotus
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

Use the `.env.example` file to create your personal `.env` file in the root of lotus

---

### 6. Run a query

```bash
python main.py \
  --sql "<your_sql_query>" \
  [--limit <num_rows>] \
  [--cascade]
```

#### Parameters

* `--sql`
  SQL query (o lista di query) da eseguire.

* `--limit` *(optional)*
  Limita il numero di righe del dataset da processare.
  Default: `-1` (nessun limite).

* `--cascade` *(optional)*
  Abilita la strategia di *LM cascade*.

---

### 7. Output

Results are available in the folder `results`