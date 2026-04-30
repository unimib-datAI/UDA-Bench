# 🚀 Quest – Quick Start Guide

This guide explains how to set up the project and run a test execution.

---

## 📂 Setup & Run

From the root of the repository (`UDA-Bench`):

### 1. Navigate to the project directory

```bash
cd systems/quest
```

---

### 2. Create and activate the Conda environment

```bash
conda create -n quest_env python=3.10.16
conda activate quest_env
```

---

### 3. Install dependencies

```bash
pip install torch==2.1.2 torchvision==0.16.2 torchaudio==2.1.2 --index-url https://download.pytorch.org/whl/cu121

pip install -r requirements.txt

python -m spacy download en_core_web_sm
python -m spacy download en_core_web_md
```

---

### 4. Configure the `.env` file

Use the `.env.example` file to create your personal `.env` file in the root of quest

⚠️ Note: the credentials of deepseek in the original repository were exposed in plain text.

---

### 5. Download local models

Run the following command from the project root directory:

```bash
mkdir -p model/BAAI model/intfloat model/sentence-transformers
git clone https://huggingface.co/BAAI/bge-m3 model/BAAI/bge-m3
git clone https://huggingface.co/intfloat/multilingual-e5-large model/intfloat/multilingual-e5-large
git clone https://huggingface.co/sentence-transformers/all-mpnet-base-v2 model/sentence-transformers/all-mpnet-base-v2
```

---

### 6. Build and start the database

```bash
docker compose up --build -d
```

---

### 7. Run a query

```bash
python main.py \
  --sql "<your_sql_query>" \
  --debug
```

#### Parameters:

* `--sql` → list of SQL queries to execute
* `--out_dir` → directory where data is stored (default `quest/results`)
* `--debug` → if present, only 5 documents are indexed (useful for quick testing)

⚠️ Note: When switching between debug and non-debug mode, you must manually empty the database.