import os
import gdown
import shutil
import warnings
import zipfile

import pdfplumber
from yaml import warnings

from quest.db.indexer.indexer import build_all_indexer

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_ROOT = os.path.join(BASE_DIR, "data", "dataset")

if os.path.exists(DATASET_ROOT):
    print(f"⚠️ Warning: The dataset root directory '{DATASET_ROOT}' already exists. It will be removed and recreated.")
    shutil.rmtree(DATASET_ROOT)

os.makedirs(DATASET_ROOT, exist_ok=True)

drive_links = [
    #"https://drive.google.com/file/d/14s0kjBXO8_Zr2yDhJFjNFjqVq8-91X3J/view?usp=drive_link", # Art
    #"https://drive.google.com/file/d/1fTpZU60IIfoumlqHuArmLv7zW3hCMMzV/view?usp=drive_link", # Player
    #"https://drive.google.com/file/d/1SscnlaJd52ZfaorjfRbOSQfphRighcqh/view?usp=drive_link", # Player
    #"https://drive.google.com/file/d/1UKDwY861mlcSpT_CchRFZI1i_xdTccYO/view?usp=drive_link", # Player
    #"https://drive.google.com/file/d/1KEO8o5OBf9JJtdPR46hAvqf_YgO21iXO/view?usp=drive_link", # Player
    #"https://drive.google.com/file/d/1q6jZaUrNuYOeTfombi3dsc8k40GZ2tcU/view?usp=drive_link", # Legal
    #"https://drive.google.com/file/d/1fnfxA3oS4RE1su7x8JjHfvvskakvMvUz/view?usp=drive_link", # Finance
    #"https://drive.google.com/file/d/1RLd2sagpY5-cIFGAII3gxtHEdAHT9_hf/view?usp=sharing", # Healthcare
    #"https://drive.google.com/file/d/1shmOOyI9LRMYm8N16tcsSen0AmAz9eY-/view?usp=sharing", # Healthcare
    #"https://drive.google.com/file/d/1i2w_7U8jEgM_Nz-GxwRvtTVicC3FNF_j/view?usp=sharing", # Healthcare
    "https://drive.google.com/file/d/1ScmZHsRLTDbgKxL22PDU4P5VrNXct9pi/view?usp=drive_link", # RAG
]

print("📥 Starting the download and extraction of files from Google Drive...\n")

for i, link in enumerate(drive_links, 1):
    print(f"🔄 Processing item {i} of {len(drive_links)}...")
    
    zip_filename = os.path.join(DATASET_ROOT, f"dataset_temp_{i}.zip")
    
    gdown.download(url=link, output=zip_filename, quiet=False, fuzzy=True)
    
    if os.path.exists(zip_filename):
        print(f"📦 Extracting {zip_filename}...")
        try:
            with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                zip_ref.extractall(DATASET_ROOT)
            print("✅ Extraction completed!")
            
            os.remove(zip_filename)
            print("🗑️ Temporary zip file removed.\n")
            
        except zipfile.BadZipFile:
            print(f"❌ Error: The downloaded file is not a valid zip. Check the link.\n")
    else:
        print(f"❌ Error downloading file {i}.\n")
        

print("✅ Download and extraction completed!\n")
            
print("-" * 50)
print("⚙️ Starting the indexing phase...\n")

for folder in os.listdir(DATASET_ROOT):
    DATASET_PATH = os.path.join(DATASET_ROOT, folder)
    
    if os.path.isdir(DATASET_PATH):
        print(f"📂 Processing folder: {folder}...")
        
        doc_dirs = [DATASET_PATH]
        
        if folder == "wikiart":
            tables_name = ["art"]
        elif folder == "legal_case":
            tables_name = ["legal"]
        elif folder == 'rag_papers':
            tables_name = ["cspaper"]
        else:
            tables_name = [folder]
        
        types = ["ZenDBDoc"]

        print(f"Indexing folder: {DATASET_PATH}...")

        try:
            build_all_indexer(
                doc_dirs=doc_dirs, 
                tables_name=tables_name, 
                types=types,
                debug_flag=False
            )
            print(f"✅ Indexing of '{folder}' completed successfully!\n")
        except Exception as e:
            raise Exception(f"❌ Error during the indexing of '{folder}': {e}\n")
        
shutil.rmtree(DATASET_ROOT)
print("🗑️ Temporary dataset folder removed.\n")