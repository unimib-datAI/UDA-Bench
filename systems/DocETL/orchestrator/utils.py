from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def dataset_real_name(dataset_name: str) -> str:
    return dataset_name.lower()


def normalize_table_name(name: str) -> str:
    return name.strip().strip('"').strip("'")