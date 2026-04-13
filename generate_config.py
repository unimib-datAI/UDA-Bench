# =========================
# GENERATE CONFIG FILE FOR EACH DATASET IN THE "Data" FOLDER
# =========================
import os
import json
import pandas as pd


# =========================
# TYPE INFERENCE
# =========================

def infer_type(series):
    if pd.api.types.is_integer_dtype(series):
        return "int"
    if pd.api.types.is_float_dtype(series):
        return "float"
    return "string"


# =========================
# BUILD SCHEMA
# =========================

def build_schema_from_csv(csv_path):
    df = pd.read_csv(csv_path)

    schema = {}
    for col in df.columns:
        schema[col] = infer_type(df[col])

    return schema


# =========================
# GENERATE CONFIG FOR ONE DATASET
# =========================

def generate_config_from_dataset(dataset_path):
    dataset_path = os.path.abspath(dataset_path)
    dataset_name = os.path.basename(dataset_path)

    csv_files = [f for f in os.listdir(dataset_path) if f.endswith(".csv")]

    if not csv_files:
        print(f"[SKIP] Nessun CSV in {dataset_name}")
        return

    tables = {}

    for csv_file in csv_files:
        table_name = csv_file.replace(".csv", "")
        csv_path = os.path.join(dataset_path, csv_file)

        schema = build_schema_from_csv(csv_path)

        tables[table_name] = {
            "documents": f"REPLACE_ME/{table_name}/*.txt",
            "fields": schema
        }

    config = {
        "dataset_name": dataset_name,
        "tables": tables
    }

    output_path = os.path.join(dataset_path, "config.json")

    with open(output_path, "w") as f:
        json.dump(config, f, indent=2)

    print(f"[OK] {dataset_name} → config.json aggiornato")


# =========================
# GENERATE FOR ALL DATASETS
# =========================

def generate_all_configs(data_root):
    data_root = os.path.abspath(data_root)

    if not os.path.exists(data_root):
        raise Exception(f"Path non trovato: {data_root}")

    for item in os.listdir(data_root):
        dataset_path = os.path.join(data_root, item)

        if os.path.isdir(dataset_path):
            generate_config_from_dataset(dataset_path)


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    # prende automaticamente la root Data
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "Data")

    generate_all_configs(DATA_DIR)