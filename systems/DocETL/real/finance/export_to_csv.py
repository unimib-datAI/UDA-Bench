from pathlib import Path
import json
import csv
import re
import argparse

BASE_DIR = Path(__file__).resolve().parent

def clean_eps(value: str) -> str:
    if not value:
        return ""

    value = value.lower()

    # prendi SOLO il primo numero (gestisce multi valori)
    match = re.search(r"-?\d+(\.\d+)?", value)
    if not match:
        return ""

    num = match.group()

    # gestisce (1.23) → -1.23
    if "(" in value and ")" in value and "-" not in num:
        num = "-" + num

    return num

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-id", type=int, required=True)
    args = parser.parse_args()

    in_file = BASE_DIR / "outputs" / f"select_q{args.query_id}.json"
    out_file = BASE_DIR / "outputs" / f"select_q{args.query_id}.csv"

    data = json.loads(in_file.read_text(encoding="utf-8"))

    rows = []
    for item in data:
        rows.append({
            "id": item.get("id", ""),
            "earnings_per_share": clean_eps(item.get("earnings_per_share", ""))
        })

    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "earnings_per_share"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Creato: {out_file}")

if __name__ == "__main__":
    main()