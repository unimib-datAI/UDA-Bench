import os
from pathlib import Path
from dotenv import load_dotenv

try:
    load_dotenv()
except ImportError:
    pass

class Settings:
    def __init__(self):
        self.SYSTEM_ROOT = Path(__file__).resolve().parent.parent
        self.PROJECT_ROOT = self.SYSTEM_ROOT.parent.parent
        
        self.CONFIG_FILES_DIR = self.SYSTEM_ROOT / "config" / "prompt info"
        self.RESULTS_DIR = self.SYSTEM_ROOT / "results"
        
        self.DATASET_DIR = self.PROJECT_ROOT / "Dataset"
        
        self.CONFIG_FILES_DIR.mkdir(parents=True, exist_ok=True)
        self.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        
        self.GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
        self.MODEL_MINI = os.getenv("MODEL_MINI", "gemini/gemini-2.5-flash")
        self.MODEL_PRO = os.getenv("MODEL_PRO", "gemini/gemini-3-flash-preview")

settings = Settings()