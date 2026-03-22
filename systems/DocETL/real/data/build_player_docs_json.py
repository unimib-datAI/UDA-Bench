import json
from pathlib import Path

INPUT_DIR = Path("systems/DocETL/real/data/player_docs")
OUTPUT_FILE = Path("systems/DocETL/real/data/player_docs.json")

def main() -> None:
    records = []

    txt_files = sorted(
        INPUT_DIR.rglob("*.txt"),
        key=lambda p: int(p.stem) if p.stem.isdigit() else p.stem
    )

    for txt_file in txt_files:
        content = txt_file.read_text(encoding="utf-8", errors="ignore").strip()
        records.append(
            {
                "id": txt_file.stem,
                "filename": txt_file.name,
                "content": content,
            }
        )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    print(f"[OK] Creato {OUTPUT_FILE} con {len(records)} documenti")

if __name__ == "__main__":
    main()