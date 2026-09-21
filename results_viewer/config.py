import os
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
REPO = APP_DIR.parent
ARCHIVE = APP_DIR / "data" / "uda_outputs.zip"
BACKUP = Path(os.environ.get("UDA_BACKUP_DIR", APP_DIR / "data" / "uda_outputs")).expanduser()
QUERY_DIR = REPO / "Query" / "Finan"
DOCS = [REPO / "note_valutazione.md", APP_DIR / "README.md"]

CATEGORIES = {
    "select": "Select/select_queries.sql",
    "filter": "Filter/filter_queries_Finan.sql",
    "agg": "Agg/agg_queries_finance.sql",
    "mixed": "Mixed/mixed_queries.sql",
}

MODELS = {
    "DocETL (exact match · string)": {
        "root": BACKUP / "DocETL outputs/outputs/finan",
        "eval": "evaluation/{cat}_{stem}_{n}",
        "answer": "csv/{cat}_{stem}_{n}.csv",
    },
    "RVCL (exact match · string)": {
        "root": BACKUP / "DQL outputs/outputs/finan",
        "eval": "evaluation/{cat}_{stem}_{n}",
        "answer": "csv/{cat}_{stem}_{n}.csv",
    },
    "Evaporate (exact match · string)": {
        "root": BACKUP / "Evaporate/outputs/finan",
        "eval": "evaluation/{cat}_{stem}_{n}",
        "answer": "csv/{cat}_{stem}_{n}.csv",
    },
    "QUEST (exact match · string)": {
        "root": BACKUP / "Quest outputs/native/systems/quest/results/Finan/csv",
        "eval": "{cat}/query_{n}/acc_result",
        "answer": "{cat}/query_{n}/results.csv",
    },
    "QUEST (LLM match · number)": {
        "root": BACKUP / "Quest outputs/report_ready/systems/quest/outputs/finan/evaluation_llm_quest",
        "eval": "{cat}_{n}",
        "answer": "{cat}_{n}/results.csv",
    },
}
