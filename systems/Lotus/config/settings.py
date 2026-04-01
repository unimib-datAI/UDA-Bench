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
        
        self.BENCHMARK_DIR = self.SYSTEM_ROOT / "benchmark"
        self.RESULTS_DIR = self.SYSTEM_ROOT / "results"
        
        self.BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
        self.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        
        self.GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
        self.MODEL_MINI = os.getenv("MODEL_MINI", "gemini/gemini-2.0-flash")
        self.MODEL_PRO = os.getenv("MODEL_PRO", "gemini/gemini-2.5-flash")

settings = Settings()