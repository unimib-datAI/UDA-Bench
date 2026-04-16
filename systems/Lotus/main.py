import argparse
import json
import sys
import time
import csv

from pathlib import Path

from config.settings import settings

sys.path.insert(0, str(settings.PROJECT_ROOT))

from download import download_from_datasets, REDIRECT_LINKS, DRIVE_LINKS

from core.pipeline import LotusPipeline
from sql_metadata import Parser

def main(query=None, cascade=False, limit=-1, out_dir=None):
    if not query:
        print("Error: No SQL query provided.")
        return 1
    
    if out_dir is None:
        out_dir = settings.SYSTEM_ROOT / "results" / str(int(time.time()))
    
    # Assicurati che la directory di output esista
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        parser = Parser(query)
        domain_list = parser.tables
        domain_list = ["legal" if d.lower() == "legal_case" else d.lower() for d in domain_list]
        domain = domain_list[0] if domain_list else ""
        # Estrae solo le colonne specificate nella clausola SELECT
        select_columns = parser.columns_dict.get("select", [])
    except Exception as e:
        print(f"Error parsing query '{query}': {e}")
        return 1

    domains = {domain}
    
    domain_paths = check_and_download_dataset(domains)
    build_config_files(domain_paths)
    
    if domain not in domain_paths:
        print(f"Error: Domain '{domain}' not found or failed to download.")
        _create_empty_csv(select_columns, out_dir)
        return 1

    pipeline = LotusPipeline(
        domain=domain, 
        path=domain_paths[domain], 
        use_cascade=cascade, 
        limit=limit
    ) 

    try:
        pipeline.run_sql_task(query, out_dir)
        print("Execution completed. Results saved to:", out_dir)
    except Exception as e:
        print(f"Error during \"{query}\": {e}")
        print("Generating empty CSV with SELECT attributes as columns...")
        _create_empty_csv(select_columns, out_dir)

def _create_empty_csv(columns, out_dir):
    """Crea un file CSV vuoto usando solo le colonne fornite come header."""
    csv_path = Path(out_dir) / "results.csv"
    with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if columns:
            writer.writerow(columns)
        else:
            # Fallback nel caso in cui non sia stato possibile parsare alcuna colonna
            writer.writerow(["result"]) 
    print(f"Empty CSV created at: {csv_path}")

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
    parser.add_argument("--limit", type=int, required=False, default=-1, help="Limit for the dataset rows")
    parser.add_argument("--cascade", action="store_true", help="Use LM cascade strategy")
    parser.add_argument("--out_dir", type=str, required=False, default=settings.SYSTEM_ROOT / "results" / str(int(time.time())), help="Output folder for results")
    args = parser.parse_args()

    queries = [q.strip("\"") for q in args.sql]
    
    for i, query in enumerate(queries):
        print(f"\n--- Execution Query SQL {i+1}/{len(queries)}: {query} ---")
        
        current_out_dir = Path(str(args.out_dir).strip('"'))
        
        if "query_" not in str(current_out_dir.name):
            current_out_dir = current_out_dir / f"query_{i+1}"

        main(
            query=query,
            cascade=args.cascade,
            limit=args.limit,
            out_dir=current_out_dir,
        )