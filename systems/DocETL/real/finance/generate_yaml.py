from pathlib import Path
import argparse
import re
import yaml


BASE_DIR = Path(__file__).resolve().parent
GENERATED_DIR = BASE_DIR / "generated"
OUTPUTS_DIR = BASE_DIR / "outputs"
DOCS_PATH = "systems/DocETL/real/finance/data/finance_docs.json"


def split_sql_queries(sql_text: str) -> list[str]:
    queries = []
    current = []

    for line in sql_text.splitlines():
        stripped = line.strip()

        if stripped.startswith("-- Query"):
            if current:
                q = "\n".join(current).strip()
                if q:
                    queries.append(q)
                current = []
            continue

        if stripped:
            current.append(line)

    if current:
        q = "\n".join(current).strip()
        if q:
            queries.append(q)

    cleaned = []
    for q in queries:
        q = q.strip()
        if q.endswith(";"):
            q = q[:-1].strip()
        cleaned.append(q)

    return cleaned


def extract_select_columns(sql: str) -> list[str]:
    match = re.search(r"SELECT\s+(.*?)\s+FROM\s+", sql, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Impossibile parsare la SELECT:\n{sql}")

    cols_part = match.group(1)
    cols = [c.strip() for c in cols_part.split(",")]
    return cols


def build_prompt(sql: str, columns: list[str]) -> str:
    fields = ["id"] + [c for c in columns if c.lower() != "id"]
    bullet_list = "\n".join(f"- {c}" for c in fields)

    return f"""You are given an unstructured document about a company.

Extract the following fields:
{bullet_list}

SQL query:
{sql}

Record ID: {{{{ input.id }}}}

Document:
{{{{ input.content }}}}

Rules:
- Always copy the record id exactly from the input
- Extract only the requested fields
- If a value is not explicitly present, return an empty string
- Do not infer or guess
- Preserve the separator "||" for multi-value fields
- Keep numeric values as they appear in the text
"""


def build_yaml(query_id: int, sql: str, columns: list[str]) -> dict:
    schema = {"id": "string"}
    for col in columns:
        if col.lower() != "id":
            schema[col] = "string"

    output_json_path = f"systems/DocETL/real/finance/outputs/select_q{query_id}.json"

    return {
        "default_model": "gemini/gemini-2.5-flash",
        "system_prompt": {
            "dataset_description": "a corpus of unstructured financial company documents",
            "persona": "a precise information extraction assistant",
        },
        "datasets": {
            "finance_docs": {
                "type": "file",
                "path": DOCS_PATH,
            }
        },
        "operations": [
            {
                "name": f"extract_select_q{query_id}",
                "type": "map",
                "model": "gemini/gemini-2.5-flash",
                "prompt": build_prompt(sql, columns),
                "output": {
                    "schema": schema
                }
            }
        ],
        "pipeline": {
            "steps": [
                {
                    "name": f"finance_select_q{query_id}_step",
                    "input": "finance_docs",
                    "operations": [f"extract_select_q{query_id}"]
                }
            ],
            "output": {
                "type": "file",
                "path": output_json_path,
            }
        }
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-file", required=True, help="Path al file delle query SQL")
    parser.add_argument("--query-id", type=int, required=True, help="Indice query, da 1")
    args = parser.parse_args()

    sql_text = Path(args.sql_file).read_text(encoding="utf-8")
    queries = split_sql_queries(sql_text)

    if args.query_id < 1 or args.query_id > len(queries):
        raise ValueError(f"query-id fuori range: {args.query_id}. Query trovate: {len(queries)}")

    sql = queries[args.query_id - 1]
    columns = extract_select_columns(sql)

    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    yaml_obj = build_yaml(args.query_id, sql, columns)
    out_path = GENERATED_DIR / f"select_q{args.query_id}.yaml"
    out_path.write_text(yaml.safe_dump(yaml_obj, sort_keys=False, allow_unicode=True), encoding="utf-8")

    print(f"YAML generato in: {out_path}")


if __name__ == "__main__":
    main()