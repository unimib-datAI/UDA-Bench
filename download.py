import os
import gdown
import shutil
import zipfile
import argparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_ROOT = os.path.join(BASE_DIR, "Dataset")
os.makedirs(DATASET_ROOT, exist_ok=True)

DRIVE_LINKS = {
    "art": "https://drive.google.com/file/d/136dCzLljcJsAZbMYO_6V6EzTjs8lCvLa/view?usp=sharing", # Art
    "nba": "https://drive.google.com/file/d/1FYIH1n2hFO6Ziz_DerYZf_tb3qz7QmVB/view?usp=sharing", # NBA
    "legal": "https://drive.google.com/file/d/1JgRB8hTRKny7IHFbvNMGE5Off32b2cL8/view?usp=sharing", # Legal
    "finance": "https://drive.google.com/file/d/1yMI-kn9WfAk-g-LSrWycvDDkwVtGhCaK/view?usp=sharing", # Finance
    "medical": "https://drive.google.com/file/d/1byJT-z2r_rX5wFiAy5Os2jP6bYfFb9nq/view?usp=sharing", # Healthcare
    #"cspaper": "https://drive.google.com/file/d/1uMy7Q-95YMrLoQcthoK2F1Ayh2qvnvZe/view?usp=sharing" # RAG
}

REDIRECT_LINKS = {
    "art": "art",
    # "cspaper": "rag",
    "finance": "finance",
    "legal": "legal",
    "disease": "medical",
    "drug": "medical",
    "institution": "medical",
    "city": "nba",
    "manager": "nba",
    "player": "nba",
    "team": "nba"
}

def download_and_extract(dataset: list[tuple[str, str]]) -> None:
    print("📥 Starting the download and extraction of files from Google Drive...\n")

    for i, (name_folder, link) in enumerate(dataset, 1):
        if os.path.exists(os.path.join(DATASET_ROOT, name_folder)):
            print(f"⚠️ Warning: The folder '{name_folder}' already exists. It will be removed and recreated.")
            shutil.rmtree(os.path.join(DATASET_ROOT, name_folder))
        
        print(f"🔄 Processing item {i} of {len(dataset)} (Destination: {name_folder})...")
        
        zip_filename = os.path.join(DATASET_ROOT, f"dataset_temp_{i}.zip")
        
        gdown.download(url=link, output=zip_filename, quiet=False)
        
        if os.path.exists(zip_filename):
            target_extraction_path = os.path.join(DATASET_ROOT, name_folder)
            os.makedirs(target_extraction_path, exist_ok=True)
            
            print(f"📦 Extracting {zip_filename} into '{target_extraction_path}'...")
            
            try:
                with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                    zip_ref.extractall(target_extraction_path)
                
                # --- START NESTED FOLDER FIX ---
                extracted_items = os.listdir(target_extraction_path)
                
                # If there is only one item and it's a directory, move its contents up one level
                if len(extracted_items) == 1:
                    single_item_path = os.path.join(target_extraction_path, extracted_items[0])
                    
                    if os.path.isdir(single_item_path):
                        print(f"🔧 Nested folder '{extracted_items[0]}' detected. Fixing structure...")
                        
                        # Move all internal files/folders to the main level
                        for item in os.listdir(single_item_path):
                            shutil.move(os.path.join(single_item_path, item), target_extraction_path)
                        
                        # Remove the now-empty folder
                        os.rmdir(single_item_path)
                        
                # --- END NESTED FOLDER FIX ---
                
                print(f"✅ Extraction completed and structured in {name_folder}!\n")
                
                os.remove(zip_filename)
                print("🗑️ Temporary zip file removed.\n")
                
            except zipfile.BadZipFile:
                print(f"❌ Error: The downloaded file is not a valid zip. Check the link.\n")
        else:
            print(f"❌ Error downloading file {i}.\n")
            

def download_from_datasets(datasets: list[str]) -> None:
    links_to_download = []
    for dataset in datasets:
        if dataset.lower() in DRIVE_LINKS:
            links_to_download.append((dataset.lower(), DRIVE_LINKS[dataset.lower()]))
        else:
            new_dataset = REDIRECT_LINKS.get(dataset.lower(), None)
            if new_dataset:
                new_dataset = new_dataset.lower()
                if new_dataset in DRIVE_LINKS:
                    links_to_download.append((new_dataset, DRIVE_LINKS[new_dataset]))
    
    print(f"🚀 Starting script for dataset(s): {', '.join(datasets).upper()}\n")
    
    download_and_extract(links_to_download)
    
    
def download_all_datasets() -> None:
    all_datasets = list(DRIVE_LINKS.keys())
    download_from_datasets(all_datasets)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and extract specific datasets from Google Drive.")
    
    keys = list(DRIVE_LINKS.keys())
    keys.extend(list(REDIRECT_LINKS.keys()))
    unique_keys = sorted(set(keys))
    
    parser.add_argument(
        "--dataset",
        type=str,
        required=False,
        nargs='+',
        default=list(DRIVE_LINKS.keys()),
        choices=unique_keys,
        help=f"Specify which dataset to download. Allowed values: {', '.join(unique_keys)}"
    )
    
    args = parser.parse_args()
    
    download_from_datasets(args.dataset)