import os
import sys

from conf.settings import PROJECT_ROOT

sys.path.insert(0, str(PROJECT_ROOT))

from download import download_from_datasets, REDIRECT_LINKS, DRIVE_LINKS

from db.indexer.indexer import build_all_indexer

DATASET_ROOT = os.path.join(PROJECT_ROOT, "Dataset")

def index_tables(table_names: list, debug_flag: bool = False):
    table_names = [t.lower() for t in table_names if t.lower() in DRIVE_LINKS or t.lower() in REDIRECT_LINKS]
    
    t_d = [t for t in table_names if not os.path.exists(os.path.join(DATASET_ROOT, t))]
    
    print("📥 Starting the download and extraction of files from Google Drive...\n")

    download_from_datasets(t_d)

    print("✅ Download and extraction completed!\n")
                
    print("-" * 50)
    print("⚙️ Starting the indexing phase...\n")

    doc_dirs = []
    tables_name = []
    types = []

    for root, dirs, files in os.walk(DATASET_ROOT):
        for d in dirs:
            if d == "files" and os.path.basename(root) in table_names:
                doc_dirs.append(os.path.join(root, d))
                tables_name.append(os.path.basename(root))
                
                types.append("TextDoc")
                '''
                if tables_name[-1].lower() in ["cspaper"]:
                    types.append("ZenDBDoc")
                else:
                    types.append("TextDoc")'''

    try:
        build_all_indexer(
            doc_dirs=doc_dirs, 
            tables_name=tables_name, 
            types=types,
            debug_flag=debug_flag
        )
        
        print(f"✅ Indexing of completed successfully!")
    except Exception as e:
        raise Exception(f"❌ Error during the indexing {e}")