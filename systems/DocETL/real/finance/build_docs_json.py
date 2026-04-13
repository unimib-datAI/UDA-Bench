from pathlib import Path
import json

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUT_FILE = DATA_DIR / "finance_docs.json"


def main():
    docs = []

    for txt_file in sorted(DATA_DIR.glob("*.txt"), key=lambda p: int(p.stem)):
        content = txt_file.read_text(encoding="utf-8", errors="ignore").strip()
        docs.append({
            "id": txt_file.stem,
            "content": content
        })

    OUT_FILE.write_text(json.dumps(docs, ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"Creato: {OUT_FILE} con {len(docs)} documenti")


if __name__ == "__main__":
    main()