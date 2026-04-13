import argparse
import csv
import re
from pathlib import Path


COLUMN_ALIASES = {
    "total_Debt": "total_debt",
    "total_debt": "total_debt",
    "total_assets": "total_assets",
    "cash_reserves": "cash_reserves",
    "net_assets": "net_assets",
    "net_profit_or_loss": "net_profit_or_loss",
    "earnings_per_share": "earnings_per_share",
    "dividend_per_share": "dividend_per_share",
    "largest_shareholder": "largest_shareholder",
    "the_highest_ownership_stake": "the_highest_ownership_stake",
    "major_equity_changes": "major_equity_changes",
    "major_events": "major_events",
    "company_name": "company_name",
    "registered_office": "registered_office",
    "exchange_code": "exchange_code",
    "principal_activities": "principal_activities",
    "board_members": "board_members",
    "executive_profiles": "executive_profiles",
    "revenue": "revenue",
    "auditor": "auditor",
    "remuneration_policy": "remuneration_policy",
    "business_segments_num": "business_segments_num",
    "business_risks": "business_risks",
    "bussiness_sales": "bussiness_sales",
    "bussiness_profit": "bussiness_profit",
    "bussiness_cost": "bussiness_cost",
    "business_sales": "bussiness_sales",
    "business_profit": "bussiness_profit",
    "business_cost": "bussiness_cost",
}


def read_csv(input_path: Path):
    if not input_path.exists():
        raise FileNotFoundError(f"CSV file not found: {input_path}")

    with input_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    if not fieldnames:
        raise ValueError(f"CSV has no header: {input_path}")

    return fieldnames, rows


def write_csv(output_path: Path, columns: list[str], rows: list[dict[str, str]]):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in columns})


def load_sql_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"SQL file is empty: {path}")
    return text


def extract_queries(sql_text: str) -> list[str]:
    lines = sql_text.splitlines()
    queries = []
    current = []

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("--"):
            continue

        if not stripped:
            continue

        current.append(stripped)

        if stripped.endswith(";"):
            query = " ".join(current).strip()
            queries.append(query)
            current = []

    if current:
        query = " ".join(current).strip()
        queries.append(query)

    return queries


def normalize_sql(sql: str) -> str:
    sql = " ".join(sql.split())
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    return sql


def parse_select_query(sql: str) -> list[str]:
    normalized = normalize_sql(sql)

    match = re.fullmatch(
        r"SELECT\s+(.+?)\s+FROM\s+[A-Za-z_][A-Za-z0-9_]*",
        normalized,
        flags=re.IGNORECASE,
    )
    if not match:
        raise ValueError(
            f"Unsupported query format: {sql}\n"
            "Supported only: SELECT col1[, col2, ...] FROM table"
        )

    raw_columns = match.group(1)

    if raw_columns.strip() == "*":
        raise ValueError("SELECT * is not supported.")

    columns = [col.strip() for col in raw_columns.split(",")]
    columns = [col for col in columns if col]

    if not columns:
        raise ValueError("No valid columns found in SELECT clause.")

    return columns


def resolve_columns(requested_columns: list[str], available_columns: list[str]):
    resolved = []
    missing = []

    lower_to_actual = {col.lower(): col for col in available_columns}

    for original in requested_columns:
        alias_target = COLUMN_ALIASES.get(original, original)
        alias_target_lower = alias_target.lower()

        if alias_target_lower in lower_to_actual:
            resolved.append((original, lower_to_actual[alias_target_lower]))
        else:
            missing.append(original)

    return resolved, missing


def project_rows(rows: list[dict[str, str]], resolved_columns: list[tuple[str, str]]):
    projected = []
    for row in rows:
        projected_row = {}
        for output_name, source_name in resolved_columns:
            projected_row[output_name] = row.get(source_name, "")
        projected.append(projected_row)
    return projected


def main():
    parser = argparse.ArgumentParser(
        description="Run one SELECT query from a multi-query UDA SQL file on the Evaporate table."
    )
    parser.add_argument(
        "--input-table",
        required=True,
        help="Path to Evaporate consolidated CSV table"
    )
    parser.add_argument(
        "--query-file",
        required=True,
        help="Path to SQL file containing multiple SELECT queries"
    )
    parser.add_argument(
        "--query-id",
        type=int,
        required=True,
        help="1-based query index inside the SQL file"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output result CSV"
    )

    args = parser.parse_args()

    input_table = Path(args.input_table)
    query_file = Path(args.query_file)
    output_path = Path(args.output)
    query_id = args.query_id

    available_columns, rows = read_csv(input_table)
    sql_text = load_sql_file(query_file)
    queries = extract_queries(sql_text)

    if not queries:
        raise ValueError(f"No SQL queries found in {query_file}")

    if query_id < 1 or query_id > len(queries):
        raise ValueError(f"query-id must be between 1 and {len(queries)}")

    sql = queries[query_id - 1]
    requested_columns = parse_select_query(sql)
    resolved_columns, missing_columns = resolve_columns(requested_columns, available_columns)

    if missing_columns:
        raise ValueError(
            f"Query {query_id} cannot be executed.\n"
            f"Missing columns in Evaporate table: {missing_columns}\n"
            f"Available columns: {available_columns}"
        )

    result_rows = project_rows(rows, resolved_columns)
    output_columns = [original for original, _ in resolved_columns]
    write_csv(output_path, output_columns, result_rows)

    print("\n=== Query execution completed ===")
    print(f"Query file: {query_file}")
    print(f"Query id: {query_id}")
    print(f"SQL: {sql}")
    print(f"Requested columns: {requested_columns}")
    print(f"Resolved columns: {resolved_columns}")
    print(f"Rows written: {len(result_rows)}")
    print(f"Output CSV: {output_path}")


if __name__ == "__main__":
    main()