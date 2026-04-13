import os
from pathlib import Path
from dotenv import load_dotenv
import google.generativeai as genai
import tiktoken

load_dotenv()

from db.connector.connector import create_opengauss_engine

opengauss_conn = create_opengauss_engine()

FILE_PATH = Path(__file__).resolve()
SYSTEM_ROOT = FILE_PATH.parent.parent
PROJECT_ROOT = SYSTEM_ROOT.parent.parent

# THRESHOLD
JOIN_EDIT_DISTANCE_THRESHOLD = 0.8
JOIN_SEMANTIC_THRESHOLD = 0.9
RETRIEVE_FULL_THRESHOLD = 0.1

# LOG
LOG_DIR = SYSTEM_ROOT / "tests" / "log"
LOG_DIR_NAME = LOG_DIR / "log_sampling.log"

# local small model
LOCAL_MODEL_DIR = SYSTEM_ROOT / "model/"
DATASET_DIR = PROJECT_ROOT / "Dataset"

# index file
INDEX_ROOT_DIR = PROJECT_ROOT / "Data" / "Index/"
OLLAMA_BASE =  "http://localhost:11434"

GEMINI_API_BASE = os.getenv("GEMINI_API_BASE", "https://generativelanguage.googleapis.com")

LLM_MODEL = 'gemini/gemini-2.5-flash'

API_EMB_MODEL = "gemini/gemini-embedding-001" 

API_EMB_API_KEY = os.getenv("GEMINI_API_KEY")
if not API_EMB_API_KEY:
    raise ValueError("⚠️ ERRORE CRITICO: GEMINI_API_KEY mancante. Inseriscila nel file .env!")

GPT_MODEL = LLM_MODEL
GPT_API_BASE = GEMINI_API_BASE
GPT_API_KEY = API_EMB_API_KEY

LLM_BATCH_SIZE = 10

os.environ['GEMINI_API_KEY'] = GPT_API_KEY
#os.environ['GEMINI_API_BASE'] = GPT_API_BASE

# <-- MODIFICA GEMINI: Setup del conteggio token con il tokenizer reale di Gemini
genai.configure(api_key=GPT_API_KEY)
# Rimuoviamo il prefisso 'gemini/' solo per l'SDK genai interno
gemini_token_model = genai.GenerativeModel('gemini-2.0-flash')

enc = tiktoken.get_encoding("cl100k_base")
Enc_token_cnt = enc

def count_tokens(text):
    # Calcola in modo esatto i token fatturati/gestiti da Gemini
    response = gemini_token_model.count_tokens(text)
    return response.total_tokens

# SAMPLE
SAMPLE_NUM = 5
TOPK = 5
ZENDB_TOPK = 5
GROUP_SAMPLE_NUM = 3

# CLUSTER
N_CLUSTERS = 3

# OTHERS
VALUE_OP = ['<', '>', '>=', '<=']