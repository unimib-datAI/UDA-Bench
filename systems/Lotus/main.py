import argparse
import os
from pathlib import Path
from core.pipeline import LotusPipeline

from config.settings import settings

def main():
    parser = argparse.ArgumentParser(description="Lotus LLM Data Extractor/Filter")
    parser.add_argument("--domain", type=str, required=True, help="Domain (e.g., finance, institutes)")
    parser.add_argument("--query-type", type=str, choices=["SF", "SFW"], required=True, help="Type of query")
    parser.add_argument("--cascade", action="store_true", help="Use LM cascade strategy")
    args = parser.parse_args()

    # Carica le query SQL
    sql_path = settings.BENCHMARK_DIR / f"Query/{args.domain}/{args.query_type}.sql"
    
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file not found at {sql_path}")
    
    with open(sql_path, 'r', encoding='utf-8') as f:
        sql_blocks = f.read().split('\n')
        queries = [sql_block.strip() for sql_block in sql_blocks if sql_block.strip()]

    # Inizializza la pipeline
    pipeline = LotusPipeline(domain=args.domain, use_cascade=args.cascade)

    # Esegue le query
    for i, sql in enumerate(queries):
        print(f"Esecuzione Query SQL {i}/{len(queries)}...")
        out_folder = settings.RESULTS_DIR / args.domain / args.query_type / f"SQL{i}"
        pipeline.run_sql_task(sql, out_folder)

if __name__ == "__main__":
    main()