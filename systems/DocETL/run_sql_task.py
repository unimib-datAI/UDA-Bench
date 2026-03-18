import argparse
import json
import re
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd


QUERY_BLOCK_RE = re.compile(
    r"--\s*Query\s*(\d+)\s*:(.*?)\n(SELECT\s+.*?;)",
    re.IGNORECASE | re.DOTALL,
)


def parse_sql_queries(sql_text: str) -> List[Dict]:
    queries: List[Dict] = []
    matches = list(QUERY_BLOCK_RE.finditer(sql_text))

    if matches:
        for m in matches:
            queries.append(
                {
                    "query_id": int(m.group(1)),
                    "label": m.group(2).strip(),
                    "sql": m.group(3).strip(),
                }
            )
        return queries

    parts = [p.strip() for p in sql_text.split(";") if p.strip()]
    for idx, part in enumerate(parts, start=1):
        queries.append(
            {
                "query_id": idx,
                "label": "sql",
                "sql": part + ";",
            }
        )
    return queries


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_csv_tables_to_duckdb(con: duckdb.DuckDBPyConnection, csv_dir: Path) -> None:
    csv_files = list(csv_dir.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"Nessun CSV trovato in {csv_dir}")

    for csv_file in csv_files:
        table_name = csv_file.stem
        csv_path = str(csv_file).replace("\\", "/")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true)
            """
        )


def normalize_id_column(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for col in df.columns:
        if col.lower() == "id" and col != "id":
            rename_map[col] = "id"
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Generic SQL task runner for UDA-Bench style tasks")
    parser.add_argument(
        "--sql-file",
        required=True,
        help="Percorso file .sql con le query benchmark",
    )
    parser.add_argument(
        "--csv-dir",
        required=True,
        help="Cartella contenente i CSV del dataset, es. Query/Player",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Cartella root in cui creare 1/, 2/, 3/ con sql.json e result.csv",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Numero massimo di query da eseguire",
    )
    args = parser.parse_args()

    sql_path = Path(args.sql_file)
    csv_dir = Path(args.csv_dir)
    output_root = Path(args.output_root)

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file non trovato: {sql_path}")
    if not csv_dir.exists():
        raise FileNotFoundError(f"CSV dir non trovata: {csv_dir}")

    ensure_dir(output_root)

    sql_text = sql_path.read_text(encoding="utf-8")
    queries = parse_sql_queries(sql_text)

    if args.limit is not None:
        queries = queries[: args.limit]

    con = duckdb.connect(database=":memory:")
    load_csv_tables_to_duckdb(con, csv_dir)

    for q in queries:
        query_id = q["query_id"]
        sql = auto_cast_numeric_conditions(q["sql"])

        query_dir = output_root / str(query_id)
        ensure_dir(query_dir)

        sql_json_path = query_dir / "sql.json"
        result_csv_path = query_dir / "result.csv"

        try:
            result_df = con.execute(sql).df()

            # 🔥 FIX: aggiungi sempre ID se non presente
            columns_lower = [c.lower() for c in result_df.columns]

            if "id" not in columns_lower:
                try:
                    # ricostruiamo una query con id
                    sql_with_id = sql.replace("SELECT", "SELECT id,", 1)
                    result_df = con.execute(sql_with_id).df()
                except Exception:
                     # fallback: aggiungi id dal dataset player
                    player_df = con.execute("SELECT id FROM player").df()
                    if len(player_df) == len(result_df):
                        result_df.insert(0, "id", player_df["id"])
                    else:
                        raise ValueError("Impossibile aggiungere id coerente al risultato")

            # normalizza nome colonna ID
            result_df = normalize_id_column(result_df)

            with sql_json_path.open("w", encoding="utf-8") as f:
                json.dump({"sql": sql}, f, ensure_ascii=False, indent=2)

            result_df.to_csv(result_csv_path, index=False, encoding="utf-8")

            print(f"[OK] Query {query_id}")
            print(f"     sql.json   -> {sql_json_path}")
            print(f"     result.csv -> {result_csv_path}")

        except Exception as e:
            print(f"[ERROR] Query {query_id}: {e}")

    print("\nCompletato.")


NUMERIC_COLUMNS = [
    "age",
    "draft_pick",
    "draft_year",
    "nba_championships",
    "mvp_awards",
    "olympic_gold_medals",
    "fiba_world_cup",
]

def auto_cast_numeric_conditions(sql: str) -> str:
    for col in NUMERIC_COLUMNS:
        # >=, <=, >, <
        sql = re.sub(
            rf"\b{col}\s*(>=|<=|>|<)\s*(\d+)",
            rf"CAST({col} AS DOUBLE) \1 \2",
            sql,
            flags=re.IGNORECASE,
        )

        # = e !=
        sql = re.sub(
            rf"\b{col}\s*(=|!=)\s*(\d+)",
            rf"CAST({col} AS DOUBLE) \1 \2",
            sql,
            flags=re.IGNORECASE,
        )

    return sql


if __name__ == "__main__":
    main()