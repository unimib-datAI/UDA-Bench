import argparse
import csv
import json
import re
from pathlib import Path
from collections import defaultdict


def load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected dict JSON in {path}")
    return data


def normalize_doc_id(doc_path: str) -> str:
    normalized = doc_path.replace("\\", "/")
    return normalized.split("/")[-1]


def extract_attribute_name(file_path: Path) -> str:
    name = file_path.name
    match = re.search(r"_([^_]+(?:_[^_]+)*)_file2metadata\.json$", name)
    if not match:
        raise ValueError(f"Cannot extract attribute name from filename: {name}")

    suffix = match.group(1)
    parts = suffix.split("_")

    config_like = {"fs100", "ts1", "ts5", "k1", "cs2000", "rt0", "b0", "c0", "ub1", "m1"}
    attr_start = 0
    for i, part in enumerate(parts):
        if part in config_like or re.fullmatch(r"[a-z]+\d+", part):
            attr_start = i + 1

    attribute = "_".join(parts[attr_start:]) if attr_start < len(parts) else suffix
    return attribute


def find_metadata_files(input_dir: Path, run_prefix: str | None) -> list[Path]:
    files = sorted(input_dir.glob("*_file2metadata.json"))
    if run_prefix:
        files = [f for f in files if f.name.startswith(run_prefix)]
    if not files:
        raise FileNotFoundError(
            f"No *_file2metadata.json files found in {input_dir} "
            f"with run_prefix={run_prefix!r}"
        )
    return files


def build_table(input_dir: Path, run_prefix: str | None):
    metadata_files = find_metadata_files(input_dir, run_prefix)

    table = defaultdict(dict)
    attributes = []

    print("\n=== Processing metadata files ===")
    for file_path in metadata_files:
        attribute = extract_attribute_name(file_path)
        data = load_json(file_path)

        attributes.append(attribute)

        for doc_path, value in data.items():
            doc_id = normalize_doc_id(doc_path)
            if value is None:
                value = ""
            table[doc_id][attribute] = str(value)

        print(f"- {file_path.name}")
        print(f"  -> attribute: {attribute}")
        print(f"  -> documents: {len(data)}")

    return sorted(set(attributes)), table


def write_csv(output_path: Path, attributes: list[str], table: dict[str, dict[str, str]]):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    columns = ["doc_id"] + attributes

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)

        for doc_id in sorted(table.keys()):
            row = [doc_id] + [table[doc_id].get(attr, "") for attr in attributes]
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(
        description="Build document-level CSV table from Evaporate *_file2metadata.json outputs."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing *_file2metadata.json files"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output CSV"
    )
    parser.add_argument(
        "--run-prefix",
        required=False,
        default=None,
        help="Optional run prefix, e.g. dlfinance_d04032026_fs100_ts5_k1_cs2000_rt0_b0_c0_ub1_m1"
    )

    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_path = Path(args.output)
    run_prefix = args.run_prefix

    attributes, table = build_table(input_dir, run_prefix)
    write_csv(output_path, attributes, table)

    print("\n=== Final table completed ===")
    print(f"Attributes consolidated: {len(attributes)}")
    print(f"Documents in final table: {len(table)}")
    print(f"Output CSV: {output_path}")


if __name__ == "__main__":
    main()