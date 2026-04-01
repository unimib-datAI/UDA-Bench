import json
from pathlib import Path

def load_json(filepath: Path, domain: str = None) -> dict | list:
    """Carica un file JSON. Se viene passato un dominio, restituisce solo quella chiave."""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
        if domain and domain in data:
            return data[domain]
        return data

def read_text_file(filepath: Path) -> str:
    """Legge il contenuto di un file di testo in modo sicuro."""
    if filepath.exists():
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read().strip()
    return ""