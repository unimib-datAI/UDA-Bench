import os
from pathlib import Path
from dotenv import load_dotenv

try:
    load_dotenv()
except ImportError:
    pass

class Settings:
    def __init__(self):
        self.BASE_DIR = Path(__file__).resolve().parent.parent
        self.BENCHMARK_DIR = self.BASE_DIR / "benchmark"
        self.RESULTS_DIR = self.BASE_DIR / "results"
        
        self.BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
        self.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        
        self.OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "")
        self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

settings = Settings()