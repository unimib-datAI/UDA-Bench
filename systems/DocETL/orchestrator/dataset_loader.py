import json
from pathlib import Path
from difflib import SequenceMatcher
from utils import repo_root, dataset_real_name


def load_dataset_config(dataset_name: str) -> dict:
    root = repo_root()
    config_path = root / "Data" / dataset_name / "config.json"

    if not config_path.exists():
        raise FileNotFoundError(f"Config non trovato: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    config["config_path"] = str(config_path)
    config["dataset_name_input"] = dataset_name
    config["resolved_documents"] = resolve_document_paths(dataset_name, config)

    return config


def resolve_document_paths(dataset_name: str, config: dict) -> dict:
    root = repo_root()
    dataset_dir = root / "systems" / "DocETL" / "real" / dataset_real_name(dataset_name) / "data"
    data_dataset_dir = root / "Data" / dataset_name

    resolved = {}
    tables = config["tables"]

    single_table = len(tables) == 1

    # Preferred canonical location for uploaded docs:
    # Data/<Dataset>/txt (or Data/<Dataset>/<table>/txt for multi-table datasets)
    preferred_txt_dir = data_dataset_dir / "txt"
    if _dir_has_txt(preferred_txt_dir):
        dataset_dir = preferred_txt_dir

    if not _dir_has_txt(dataset_dir):
        discovered = _discover_best_txt_dir(root, dataset_name, list(tables.keys()))
        if discovered is not None:
            dataset_dir = discovered

    for table_name in tables:
        table_subdir = dataset_dir / table_name
        if _dir_has_txt(table_subdir):
            txt_glob = str(table_subdir / "*.txt")
        elif single_table:
            txt_glob = str(dataset_dir / "*.txt")
        else:
            fallback_subdir = _find_best_matching_subdir(dataset_dir, table_name)
            if fallback_subdir is not None and _dir_has_txt(fallback_subdir):
                txt_glob = str(fallback_subdir / "*.txt")
            else:
                txt_glob = str(dataset_dir / table_name / "*.txt")

        resolved[table_name] = ensure_docetl_json_dataset(
            root=root,
            dataset_name=dataset_name,
            table_name=table_name,
            txt_glob=txt_glob,
        )

    return resolved


def ensure_docetl_json_dataset(root: Path, dataset_name: str, table_name: str, txt_glob: str) -> str:
    import glob

    txt_files = sorted(
        [Path(p) for p in glob.glob(txt_glob)],
        key=lambda p: _sort_key(p.stem),
    )

    if not txt_files:
        raise FileNotFoundError(f"Nessun documento trovato per glob: {txt_glob}")

    out_dir = (
        root
        / "systems"
        / "DocETL"
        / "outputs"
        / dataset_real_name(dataset_name)
        / "inputs"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{table_name}.json"

    docs = []
    for txt_file in txt_files:
        content = txt_file.read_text(encoding="utf-8", errors="ignore").strip()
        docs.append(
            {
                "id": txt_file.stem,
                "filename": txt_file.name,
                "text": content,
                "content": content,
            }
        )

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=True, indent=2)

    return str(out_file)


def _discover_best_txt_dir(root: Path, dataset_name: str, table_names: list[str]) -> Path | None:
    candidate_roots = [
        root / "Data" / dataset_name,
        root / "systems" / "DocETL" / "real",
    ]

    dataset_l = dataset_name.lower()
    table_l = [t.lower() for t in table_names]
    candidates: list[tuple[float, Path]] = []

    for base in candidate_roots:
        if not base.exists():
            continue
        for d in base.rglob("*"):
            if not d.is_dir():
                continue
            if not _dir_has_txt(d):
                continue

            name = d.name.lower()
            parent = d.parent.name.lower()
            score = 0.0

            score += SequenceMatcher(None, dataset_l, name).ratio()
            score += SequenceMatcher(None, dataset_l, parent).ratio()

            if dataset_l in name or name in dataset_l:
                score += 2.0
            if dataset_l in parent or parent in dataset_l:
                score += 3.0

            # prefer explicit txt folders under Data/<dataset>
            if name == "txt":
                score += 5.0

            for t in table_l:
                score += SequenceMatcher(None, t, name).ratio()
                if t in name or name in t:
                    score += 1.5

            candidates.append((score, d))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _find_best_matching_subdir(base: Path, table_name: str) -> Path | None:
    if not base.exists():
        return None
    table_l = table_name.lower()
    best: tuple[float, Path] | None = None

    for d in base.iterdir():
        if not d.is_dir():
            continue
        score = SequenceMatcher(None, table_l, d.name.lower()).ratio()
        if table_l in d.name.lower() or d.name.lower() in table_l:
            score += 1.0
        if best is None or score > best[0]:
            best = (score, d)

    if best is None:
        return None
    return best[1]


def _dir_has_txt(path: Path) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    return any(path.glob("*.txt"))


def _sort_key(stem: str):
    if stem.isdigit():
        return (0, int(stem))
    return (1, stem)
