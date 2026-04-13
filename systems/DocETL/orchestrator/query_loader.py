from pathlib import Path
from utils import repo_root


def split_sql_queries(text: str) -> list[str]:
    chunks = [q.strip() for q in text.split(";")]
    return [q for q in chunks if q]


def load_all_sql_queries(dataset_name: str) -> list[dict]:
    root = repo_root()
    query_root = root / "Query" / dataset_name

    if not query_root.exists():
        raise FileNotFoundError(f"Cartella query non trovata: {query_root}")

    all_queries = []

    for sql_file in sorted(query_root.rglob("*.sql")):
        category = sql_file.parent.name.lower()
        with open(sql_file, "r", encoding="utf-8") as f:
            content = f.read()

        queries = split_sql_queries(content)

        for i, sql in enumerate(queries, start=1):
            query_id = f"{category}_{sql_file.stem}_{i}"
            all_queries.append({
                "id": query_id,
                "category": category,
                "source_file": str(sql_file),
                "sql": sql
            })

    return all_queries
