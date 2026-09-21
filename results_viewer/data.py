import json
import re
from pathlib import Path

import pandas as pd
import streamlit as st

from config import CATEGORIES, MODELS, QUERY_DIR

EMPTY_VALUES = {"", "nan", "none", "null"}


def split_sql(text):
    text = re.sub(r"--[^\n]*", "", text)
    return [sql.strip() for sql in text.split(";") if sql.strip()]


def model_path(model, kind, cat, n):
    source = MODELS[model]
    return source["root"] / source[kind].format(cat=cat, n=n, stem=Path(CATEGORIES[cat]).stem)


@st.cache_data
def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else None


@st.cache_data
def read_csv(path):
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        return pd.DataFrame()


@st.cache_data
def load_queries():
    return pd.DataFrame([
        {"query": f"{cat}_{n}", "cat": cat, "n": n, "sql": sql}
        for cat, file in CATEGORIES.items()
        for n, sql in enumerate(split_sql((QUERY_DIR / file).read_text(encoding="utf-8")), start=1)
    ])


@st.cache_data
def load_scores():
    rows = []
    for query in load_queries().itertuples():
        for model in MODELS:
            acc = read_json(model_path(model, "eval", query.cat, query.n) / "acc.json")
            if acc is None:
                continue
            counts = acc["rows"]
            empty_empty = counts["len_gold"] == 0 and counts["len_pred"] == 0
            rows.append({
                "query": query.query,
                "cat": query.cat,
                "model": model,
                "P": acc["macro_precision"],
                "R": acc["macro_recall"],
                "F1": acc["macro_f1"],
                "F1_adj": 1.0 if empty_empty else acc["macro_f1"],
                "gold": counts["len_gold"],
                "pred": counts["len_pred"],
                "matched": counts["matched_rows"],
                "EE": empty_empty,
            })
    return pd.DataFrame(rows)


def summarize(scores, by):
    summary = scores.groupby(by, sort=False, as_index=False).agg(
        n=("query", "count"),
        P=("P", "mean"),
        R=("R", "mean"),
        F1=("F1", "mean"),
        F1_adj=("F1_adj", "mean"),
        EE=("EE", "sum"),
        matched=("matched", "sum"),
    )
    return summary.round(4)


def load_detail(model, cat, n):
    eval_dir = model_path(model, "eval", cat, n)
    return {
        "acc": read_json(eval_dir / "acc.json"),
        "gold": read_csv(eval_dir / "gold_result.csv"),
        "answer": read_csv(model_path(model, "answer", cat, n)),
        "matched_gold": read_csv(eval_dir / "matched_gold_result.csv"),
        "matched_pred": read_csv(eval_dir / "matched_result.csv"),
    }


def normalize(value):
    text = str(value).strip().lower()
    if text in EMPTY_VALUES:
        return ""
    try:
        return float(text)
    except ValueError:
        return frozenset(part.strip() for part in text.split("||"))


def side_by_side(gold, pred):
    table = gold[["id"]].copy() if "id" in gold else pd.DataFrame(index=gold.index)
    differs = pd.DataFrame(False, index=table.index, columns=table.columns)
    for col in gold.columns.drop("id", errors="ignore"):
        predicted = pred[col] if col in pred else pd.Series("", index=gold.index)
        table[f"{col} · GT"] = gold[col]
        table[f"{col} · modello"] = predicted
        differs[f"{col} · GT"] = False
        differs[f"{col} · modello"] = gold[col].map(normalize) != predicted.map(normalize)
    return table, differs
