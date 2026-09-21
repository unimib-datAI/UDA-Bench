import pandas as pd
import streamlit as st

import data
from config import ARCHIVE, BACKUP, CATEGORIES, DOCS, MODELS

DIFF_STYLE = "background-color: rgba(255, 75, 75, 0.3)"

st.set_page_config(page_title="UDA-Bench results", layout="wide")

if not BACKUP.is_dir():
    st.error(f"Cartella dei risultati non trovata: `{BACKUP}`. Scompatta `{ARCHIVE}` nella sua cartella, vedi README.")
    st.stop()

queries = data.load_queries().set_index("query")
scores = data.load_scores()


def filter_queries(models, cat, gt_differs, empty_answer):
    selected = scores[scores["model"].isin(models)]
    if cat != "tutte":
        selected = selected[selected["cat"] == cat]
    if gt_differs:
        selected = selected[selected.groupby("query")["gold"].transform("nunique") > 1]
    if empty_answer:
        empty = (selected["pred"] == 0) & (selected["gold"] > 0)
        selected = selected[empty.groupby(selected["query"]).transform("any")]
    keep = set(selected["query"])
    return [query for query in queries.index if query in keep]


def sidebar():
    st.sidebar.title("UDA-Bench · Finance")
    view = st.sidebar.radio("Vista", ["Panoramica", "Query", "Note"], key="view", horizontal=True)
    models = st.sidebar.multiselect("Modelli", list(MODELS), default=list(MODELS))
    st.sidebar.caption("exact match / LLM match: giudice usato. string / number: come il GT confronta i numeri.")
    cat = st.sidebar.selectbox("Categoria", ["tutte", *CATEGORIES])
    gt_differs = st.sidebar.checkbox("Solo GT diverso tra modelli")
    empty_answer = st.sidebar.checkbox("Solo risposte vuote con GT non vuoto")
    ids = filter_queries(models, cat, gt_differs, empty_answer)
    query = st.session_state.get("query")
    if query not in ids:
        query = ids[0] if ids else None
    if view == "Query" and ids:
        query = st.sidebar.selectbox("Query", ids, index=ids.index(query))
    st.session_state.query = query
    return view, models, ids


def open_query(ids):
    rows = st.session_state.matrix.selection.rows
    if rows:
        st.session_state.query = ids[rows[0]]
        st.session_state.view = "Query"


def overview(models, ids):
    selected = scores[scores["model"].isin(models)]
    st.subheader("Score per modello (tutte le query)")
    st.dataframe(data.summarize(selected, ["model"]), hide_index=True)

    by_cat = data.summarize(selected, ["model", "cat"])
    for column, metric in zip(st.columns(2), ["F1", "F1_adj"]):
        column.subheader(f"{metric} per categoria")
        pivot = by_cat.pivot(index="model", columns="cat", values=metric)
        column.dataframe(pivot.reindex(index=models, columns=list(CATEGORIES)))

    st.subheader("F1 per query")
    st.caption("Clicca una riga per aprire la query.")
    matrix = selected.pivot(index="query", columns="model", values="F1").reindex(index=ids, columns=models)
    st.dataframe(
        matrix,
        height=600,
        column_config={m: st.column_config.ProgressColumn(m, min_value=0, max_value=1, format="%.2f") for m in models},
        key="matrix",
        on_select=lambda: open_query(ids),
        selection_mode="single-row",
    )


def query_page(query, models):
    info = queries.loc[query]
    st.subheader(query)
    st.code(info["sql"], language="sql")

    board = scores[(scores["query"] == query) & scores["model"].isin(models)]
    if board["gold"].nunique() > 1:
        sizes = ", ".join(f"{model} {gold}" for model, gold in zip(board["model"], board["gold"]))
        st.warning(f"GT diverso tra modelli (righe gold): {sizes}")
    st.dataframe(board.drop(columns=["query", "cat"]).round(4), hide_index=True)

    for tab, model in zip(st.tabs(models), models):
        with tab:
            model_detail(model, info["cat"], info["n"])


def model_detail(model, cat, n):
    detail = data.load_detail(model, cat, n)
    if detail["acc"] is None:
        st.info("Nessuna evaluation per questa query.")
        return

    for column, title, frame in zip(st.columns(2), ["GT", "Risposta"], [detail["gold"], detail["answer"]]):
        column.markdown(f"**{title}** · {len(frame)} righe")
        column.dataframe(frame, hide_index=True)

    with st.expander("Confronto riga per riga"):
        st.caption("In rosso le celle diverse dal GT (confronto testuale: il verdetto LLM per cella non è salvato).")
        table, differs = data.side_by_side(detail["matched_gold"], detail["matched_pred"])
        styles = differs.map(lambda different: DIFF_STYLE if different else "")
        st.dataframe(table.style.apply(lambda _: styles, axis=None), hide_index=True)

    with st.expander("Score per colonna"):
        st.dataframe(pd.DataFrame(detail["acc"]["columns"]).T.round(4))


def docs_page():
    for tab, path in zip(st.tabs([path.name for path in DOCS]), DOCS):
        tab.markdown(path.read_text(encoding="utf-8") if path.exists() else f"File non trovato: {path}")


view, models, ids = sidebar()
if view == "Note":
    docs_page()
elif not models or not ids:
    st.info("Nessuna query corrisponde ai filtri.")
elif view == "Panoramica":
    overview(models, ids)
else:
    query_page(st.session_state.query, models)
