import json
from pathlib import Path

def load_json(filepath: Path, domain: str = None) -> dict | list:
    """Load a JSON file. If a domain is provided, return only that key."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get(domain, []) if domain else data
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return [] if domain else {}

def read_text_file(filepath: Path) -> str:
    """Safely read the contents of a text file."""
    if filepath.exists():
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read().strip()
    return ""