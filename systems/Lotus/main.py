import argparse
import os
from pathlib import Path
from core.pipeline import LotusPipeline

from config.settings import settings

def main():
    parser = argparse.ArgumentParser(description="Lotus LLM Data Extractor/Filter")
    parser.add_argument("--domain", type=str, required=True, help="Domain (e.g., finance, institutes)")
    parser.add_argument("--cascade", action="store_true", help="Use LM cascade strategy")
    parser.add_argument("--query-type", type=str, choices=["SF", "SFW"], required=False, help="Type of query", default="SF")
    args = parser.parse_args()

    sql_path = settings.BENCHMARK_DIR / f"Query/{args.domain}/{args.query_type}.sql"
    
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file not found at {sql_path}")
    
    with open(sql_path, 'r', encoding='utf-8') as f:
        sql_blocks = f.read().split('\n')
        queries = [sql_block.strip() for sql_block in sql_blocks if sql_block.strip()]

    pipeline = LotusPipeline(domain=args.domain, use_cascade=args.cascade)

    try:
        for i, sql in enumerate(queries):
            print(f"Execution Query SQL {i}/{len(queries)}...")
            out_folder = settings.RESULTS_DIR / args.domain / args.query_type / f"SQL{i}"
            pipeline.run_sql_task(sql, out_folder)
    except Exception as e:
        print(f"Error during \"{sql}\": {e}")

if __name__ == "__main__":
    main()