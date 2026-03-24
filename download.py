import os
import gdown
import shutil
import zipfile
import argparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_ROOT = os.path.join(BASE_DIR, "Dataset")
os.makedirs(DATASET_ROOT, exist_ok=True)

DRIVE_LINKS = {
    "art": [
        ("art", "https://drive.google.com/file/d/14s0kjBXO8_Zr2yDhJFjNFjqVq8-91X3J/view?usp=drive_link")
    ], # Art
    "player": [
        ("team", "https://drive.google.com/file/d/1fTpZU60IIfoumlqHuArmLv7zW3hCMMzV/view?usp=drive_link"),
        ("player", "https://drive.google.com/file/d/1SscnlaJd52ZfaorjfRbOSQfphRighcqh/view?usp=drive_link"),
        ("owner", "https://drive.google.com/file/d/1UKDwY861mlcSpT_CchRFZI1i_xdTccYO/view?usp=drive_link"),
        ("city", "https://drive.google.com/file/d/1KEO8o5OBf9JJtdPR46hAvqf_YgO21iXO/view?usp=drive_link")
    ], # Player
    "legal": [
        ("legal", "https://drive.google.com/file/d/1q6jZaUrNuYOeTfombi3dsc8k40GZ2tcU/view?usp=drive_link")
    ], # Legal
    "finance": [
        ("finance", "https://drive.google.com/file/d/1fnfxA3oS4RE1su7x8JjHfvvskakvMvUz/view?usp=drive_link")
    ], # Finance
    "healthcare": [
        ("institutes", "https://drive.google.com/file/d/1RLd2sagpY5-cIFGAII3gxtHEdAHT9_hf/view?usp=sharing"),
        ("drug", "https://drive.google.com/file/d/1shmOOyI9LRMYm8N16tcsSen0AmAz9eY-/view?usp=sharing"),
        ("disease", "https://drive.google.com/file/d/1i2w_7U8jEgM_Nz-GxwRvtTVicC3FNF_j/view?usp=sharing")
    ], # Healthcare
    "cspaper": [
        ("cspaper", "https://drive.google.com/file/d/1ScmZHsRLTDbgKxL22PDU4P5VrNXct9pi/view?usp=drive_link")
    ] # RAG
}

def download_and_extract(dataset: list[tuple[str, str]]) -> None:
    print("📥 Starting the download and extraction of files from Google Drive...\n")

    for i, (name_folder, link) in enumerate(dataset, 1):
        
        if os.path.exists(os.path.join(DATASET_ROOT, name_folder)):
            print(f"⚠️ Warning: The folder '{name_folder}' already exists. It will be removed and recreated.")
            shutil.rmtree(os.path.join(DATASET_ROOT, name_folder))
        
        
        print(f"🔄 Processing item {i} of {len(dataset)} (Destination: {name_folder})...")
        
        zip_filename = os.path.join(DATASET_ROOT, f"dataset_temp_{i}.zip")
        
        gdown.download(url=link, output=zip_filename, quiet=False, fuzzy=True)
        
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and extract specific datasets from Google Drive.")
    
    parser.add_argument(
        "--dataset",
        type=str,
        required=False,
        nargs='+',
        default=list(DRIVE_LINKS.keys()),
        choices=list(DRIVE_LINKS.keys()),
        help=f"Specify which dataset to download. Allowed values: {', '.join(DRIVE_LINKS.keys())}"
    )
    
    args = parser.parse_args()
    
    links_to_download = []
    for dataset in args.dataset:
        links_to_download.extend(DRIVE_LINKS.get(dataset.lower(), []))
    
    print(f"🚀 Starting script for dataset(s): {', '.join(args.dataset).upper()}\n")
    
    download_and_extract(links_to_download)