import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


def run_eval_for_query(dataset: str, task: str, sql_file: Path, result_csv: Path) -> Dict:
    cmd = [
        sys.executable,
        "-m",
        "evaluation.run_eval",
        "--dataset",
        dataset,
        "--task",
        task,
        "--sql-file",
        str(sql_file),
        "--result-csv",
        str(result_csv),
    ]

    completed = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    acc_json = result_csv.parent / "acc_result" / "acc.json"

    row: Dict = {
        "query_id": result_csv.parent.name,
        "sql_file": str(sql_file).replace("\\", "/"),
        "result_csv": str(result_csv).replace("\\", "/"),
        "acc_json": str(acc_json).replace("\\", "/"),
        "return_code": completed.returncode,
        "status": "ok" if completed.returncode == 0 else "error",
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }

    if acc_json.exists():
        try:
            with acc_json.open("r", encoding="utf-8") as f:
                metrics = json.load(f)

            row["macro_f1"] = metrics.get("macro_f1")
            row["macro_precision"] = metrics.get("macro_precision")
            row["macro_recall"] = metrics.get("macro_recall")
        except Exception as e:
            row["status"] = "error"
            row["metrics_read_error"] = str(e)
    else:
        row["status"] = "error"
        row["metrics_read_error"] = f"acc.json non trovato: {acc_json}"

    return row


def load_sql_from_json(sql_json_path: Path) -> Optional[str]:
    try:
        with sql_json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data.get("sql")
    except Exception:
        return None
    return None


def collect_query_dirs(output_root: Path) -> List[Path]:
    dirs = []
    for p in output_root.iterdir():
        if p.is_dir() and p.name.isdigit():
            dirs.append(p)
    return sorted(dirs, key=lambda x: int(x.name))


def main() -> None:
    parser = argparse.ArgumentParser(description="Generic batch evaluation for UDA-Bench task outputs")
    parser.add_argument("--output-root", required=True, help="Cartella contenente 1/, 2/, 3/, ...")
    parser.add_argument("--dataset", required=True, help="Es. Player")
    parser.add_argument("--task", required=True, help="Es. Select, Filter, Agg, Join")
    parser.add_argument("--limit", type=int, default=None, help="Valuta solo le prime N query")
    args = parser.parse_args()

    output_root = Path(args.output_root)
    if not output_root.exists():
        raise FileNotFoundError(f"Cartella output non trovata: {output_root}")

    query_dirs = collect_query_dirs(output_root)
    if args.limit is not None:
        query_dirs = query_dirs[: args.limit]

    rows: List[Dict] = []

    for qdir in query_dirs:
        sql_file = qdir / "sql.json"
        result_csv = qdir / "result.csv"

        if not sql_file.exists() or not result_csv.exists():
            rows.append(
                {
                    "query_id": qdir.name,
                    "status": "error",
                    "error": "sql.json o result.csv mancante",
                }
            )
            print(f"[ERROR] Query {qdir.name}: file mancanti")
            continue

        print(f"[RUN] Query {qdir.name}")
        row = run_eval_for_query(
            dataset=args.dataset,
            task=args.task,
            sql_file=sql_file,
            result_csv=result_csv,
        )

        row["sql"] = load_sql_from_json(sql_file)
        rows.append(row)

        if row["status"] == "ok":
            print(f"[OK] Query {qdir.name} - macro_f1={row.get('macro_f1')}")
        else:
            print(f"[ERROR] Query {qdir.name}")

    summary_df = pd.DataFrame(rows)
    summary_csv = output_root / "evaluation_summary.csv"
    summary_df.to_csv(summary_csv, index=False, encoding="utf-8")

    ok_df = summary_df[summary_df["status"] == "ok"].copy() if "status" in summary_df.columns else pd.DataFrame()
    if not ok_df.empty and "macro_f1" in ok_df.columns:
        macro_f1_mean = pd.to_numeric(ok_df["macro_f1"], errors="coerce").mean()
        print(f"\nMedia macro_f1 sulle query riuscite: {macro_f1_mean:.4f}")
    else:
        print("\nNessuna query valutata con successo.")

    print(f"Summary salvato in: {summary_csv}")


if __name__ == "__main__":
    main()