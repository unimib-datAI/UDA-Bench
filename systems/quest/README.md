# QUEST Quick Start

This guide assumes commands are run from the repository root (`UDA-Bench`).

## Local prerequisites

These paths are intentionally not versioned and must exist locally:

- `.env`
- `.venv-quest/` or a Python 3.10 environment exposed through `QUEST_PYTHON`
- `Data/Finan/txt/*.txt`
- `systems/quest/model/intfloat/multilingual-e5-large/*`
- Docker volume `quest_pgvector_data`
- `systems/quest/results/`

`Query/Finan/Finan_attributes.json` is versioned and is used as a fallback for Finance attribute metadata, so a local `Dataset/finance/Attributes.json` copy is not required.

## 1. Configure `.env`

Create or update `.env` in the repository root:

```env
AZURE_OPENAI_API_KEY=
AZURE_OPENAI_ENDPOINT=https://<resource>.openai.azure.com
AZURE_OPENAI_DEPLOYMENT=
OPENAI_API_VERSION=2024-12-01-preview

HOST=localhost
DATABASE=quest
USER=quest
PASSWORD=quest_password
DB_PORT_EXTERNAL=5433
DB_PORT_INTERNAL=5432
```

Use `--env-file .env` with Docker Compose, because the compose file lives under `systems/quest`.

## 2. Create the QUEST environment

```powershell
py -3.10 -m venv .venv-quest
.\.venv-quest\Scripts\python.exe -m pip install --upgrade pip
.\.venv-quest\Scripts\pip.exe install torch==2.1.2 torchvision==0.16.2 torchaudio==2.1.2 --index-url https://download.pytorch.org/whl/cu121
.\.venv-quest\Scripts\pip.exe install -r systems\quest\requirements.txt
```

If the spaCy models are missing:

```powershell
.\.venv-quest\Scripts\python.exe -m spacy download en_core_web_sm
.\.venv-quest\Scripts\python.exe -m spacy download en_core_web_md
```

## 3. Download the local embedding model

The standard Finance run uses `intfloat/multilingual-e5-large`:

```powershell
.\.venv-quest\Scripts\huggingface-cli.exe download intfloat/multilingual-e5-large --local-dir systems\quest\model\intfloat\multilingual-e5-large
```

Expected files include:

```text
systems/quest/model/intfloat/multilingual-e5-large/config.json
systems/quest/model/intfloat/multilingual-e5-large/model.safetensors
```

The README from the original QUEST project also mentions `BAAI/bge-m3` and `sentence-transformers/all-mpnet-base-v2`; those are only needed for alternate code paths.

## 4. Start pgvector

```powershell
docker compose --env-file .env -f systems\quest\docker-compose.yml up --build -d
docker compose --env-file .env -f systems\quest\docker-compose.yml ps
```

The `quest_pgvector` container should be healthy.

## 5. Smoke test

```powershell
$env:PYTHONUTF8='1'
$env:PYTHONIOENCODING='utf-8'

.\.venv-quest\Scripts\python.exe systems\quest\main.py --sql "SELECT company_name FROM finance" --debug --out_dir systems\quest\results\Finan\smoke
```

When switching from `--debug` to a full run, reset the DB volume:

```powershell
docker compose --env-file .env -f systems\quest\docker-compose.yml down -v
docker compose --env-file .env -f systems\quest\docker-compose.yml up --build -d
```

## 6. Full run through the orchestrator

```powershell
.\.venv-quest\Scripts\python.exe orchestrator\main.py --model quest --dataset Finan --query-type all --mode run+eval --run-id quest_finan_run_eval
```

To resume after failures or interrupted network calls:

```powershell
.\.venv-quest\Scripts\python.exe orchestrator\main.py --model quest --dataset Finan --query-type all --mode run+eval --retry-failed --run-id quest_finan_run_eval_retry
```
