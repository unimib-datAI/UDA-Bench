import argparse
import time

from config.settings import settings

from core.pipeline import LotusPipeline
from sql_metadata import Parser

def main():
    parser = argparse.ArgumentParser(description="Lotus LLM Data Extractor/Filter")
    parser.add_argument("--query", type=str, nargs="+", required=True, help="SQL query")
    parser.add_argument("--limit", type=int, nargs="+", required=True, help="Limit for the dataset rows")
    parser.add_argument("--cascade", action="store_true", help="Use LM cascade strategy")
    args = parser.parse_args()
        
    if not args.query and len(args.query) < 1:
        print("Error: No SQL query provided. Use --query to specify the SQL query.")
        return
    
    domains = set()
    queries = []
    
    for query in args.query:
        domain = Parser(query).tables
        domain = domain[0] if domain else ""
        domains.add(domain)
        queries.append((query, domain))

    pipelines = {domain: LotusPipeline(domain=domain, use_cascade=args.cascade) for domain in domains}
    
    try:
        timestamp = int(time.time())
        for i, sql_info in enumerate(queries):
            query, domain = sql_info
            
            print(f"Execution Query SQL {i+1}/{len(queries)}: {query} (Domain: {domain})")
            
            pipeline = pipelines[domain]
            out_folder = settings.RESULTS_DIR / f"{timestamp}" / f"{i}"
            pipeline.run_sql_task(query, out_folder)
            
            print("Execution completed. Results saved to:", out_folder)
    except Exception as e:
        print(f"Error during \"{query}\": {e}")

if __name__ == "__main__":
    main()