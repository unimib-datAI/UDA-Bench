import argparse
import json
import os
import sys
import time

from config.settings import settings

sys.path.insert(0, str(settings.PROJECT_ROOT))

from download import download_from_datasets, REDIRECT_LINKS, DRIVE_LINKS

from core.pipeline import LotusPipeline
from sql_metadata import Parser

def main(queries=None, cascade=False, limit=-1):
    if not queries:
        print("Error: No SQL query provided.")
        return
    
    domains = set()
    queries_map = []
    
    for query in queries:
        domain = Parser(query).tables
        domain = domain[0] if domain else ""
        domains.add(domain)
        queries_map.append((query, domain))
        
    domains = check_and_download_dataset(domains)
    build_config_files(domains)
    
    pipelines = {
        domain: LotusPipeline(
            domain=domain, 
            path=domains[domain], 
            use_cascade=cascade, 
            limit=limit
        ) 
        for domain in domains.keys()
    }
    
    try:
        timestamp = int(time.time())
        for i, sql_info in enumerate(queries_map):
            query, domain = sql_info
            
            print(f"Execution Query SQL {i+1}/{len(queries_map)}: {query} (Domain: {domain})")
            
            pipeline = pipelines[domain]
            out_folder = settings.RESULTS_DIR / f"{timestamp}" / f"{i}"
            pipeline.run_sql_task(query, out_folder)
            
            print("Execution completed. Results saved to:", out_folder)
    except Exception as e:
        print(f"Error during \"{query}\": {e}")
        

def check_and_download_dataset(domains):
    domains = set(d.lower() for d in domains)
    available = {
        str(p.name).lower()
        for d in settings.DATASET_DIR.iterdir() if d.is_dir()
        for p in ([d] + [sub for sub in d.iterdir() if sub.is_dir()])
    }
     
    to_download = set(domains) - set(available)
    to_download = [t for t in to_download if t in DRIVE_LINKS or t in REDIRECT_LINKS]
    
    if to_download:
        print(f"Datasets to download: {to_download}")
        download_from_datasets(to_download)
        
    paths = {
        str(p.name).lower(): p
        for d in settings.DATASET_DIR.iterdir() if d.is_dir()
        for p in ([d] + [sub for sub in d.iterdir() if sub.is_dir()])
        if str(p.name).lower() in domains
    }
    
    return paths
    
def build_config_files(domains):
    descriptions = {}
    examples = {}
    extractions = {}
    
    for domain, path in domains.items():
        json_domain = path / "Attributes.json"
        
        with open(json_domain, "r", encoding="utf-8", errors="ignore") as f:
            info = json.loads(f.read()).get(domain, {})
        
        descriptions.update(
            {
                domain: [f"{attr}: {details.get('description', 'No description available.')}" for attr, details in info.items()]
            }
        )
        
        with open(settings.CONFIG_FILES_DIR / "examples_original.json", "r", encoding="utf-8", errors="ignore") as f:
            examples_data = json.loads(f.read())
        
            if domain in examples_data:
                examples.update({domain: examples_data[domain]})
            elif domain == "art":
                examples.update({domain: examples_data["Wiki_Image"]})
            elif domain == "legal":
                examples.update({domain: examples_data["legal_case"]})
            else:
                examples.update(
                    {
                        domain: [
                            {
                                "context": [""],
                                "Answer": [""]
                            }
                        ]
                    }
                )
        
        extractions.update(
            {
                domain: [f"{k}" for k in info.keys()]
            }
        )
        
    update_json_file(settings.CONFIG_FILES_DIR / "descriptions.json", descriptions)
    update_json_file(settings.CONFIG_FILES_DIR / "examples.json", examples)
    update_json_file(settings.CONFIG_FILES_DIR / "extractions.json", extractions)

def update_json_file(path, new_data):
    if path.exists():
        with open(path, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {}
    else:
        data = {}

    data.update(new_data)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Lotus LLM Data Extractor/Filter")
    parser.add_argument("--sql", type=str, nargs="+", required=True, help="SQL query")
    parser.add_argument("--limit", type=int, required=False, default=-1,help="Limit for the dataset rows")
    parser.add_argument("--cascade", action="store_true", help="Use LM cascade strategy")
    args = parser.parse_args()
    
    main(queries=args.sql, cascade=args.cascade, limit=args.limit)