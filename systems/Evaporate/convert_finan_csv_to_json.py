import csv
import json
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(base_dir, "data", "finance", "Finan.csv")
json_path = os.path.join(base_dir, "data", "finance", "table.json")

data = {}

with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)

    for row in reader:
        file_id = str(row["ID"]).strip()
        file_key = f"{file_id}.txt"

        row_clean = {}
        for k, v in row.items():
            if k == "ID":
                continue
            row_clean[k] = v.strip() if isinstance(v, str) else v

        data[file_key] = row_clean

with open(json_path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"Creato: {json_path}")
print(f"Numero record: {len(data)}")