import os
from openai import OpenAI
import tiktoken
from dotenv import load_dotenv

load_dotenv()

from quest.db.connector.connector import create_opengauss_engine

opengauss_conn = create_opengauss_engine()

current_file_path = os.path.abspath(__file__)
dir_name= os.path.dirname(current_file_path)

ABS_PROJECT_ROOT_PATH = "/quest"  

# THRESHOLD
JOIN_EDIT_DISTANCE_THRESHOLD = 0.8
JOIN_SEMANTIC_THRESHOLD = 0.9
RETRIEVE_FULL_THRESHOLD = 0.1

# LOG
RELATIVE_PROJECT_ROOT_PATH = os.path.join(dir_name)
LOG_DIR = os.path.join(dir_name, "/tests/log")
LOG_DIR_NAME = os.path.join(LOG_DIR, "log_sampling.log")

# local small model
LOCAL_MODEL_DIR = os.path.join(ABS_PROJECT_ROOT_PATH, "model/")
DATASET_DIR = os.path.join(ABS_PROJECT_ROOT_PATH, "data/dataset/")

# index file
INDEX_ROOT_DIR = os.path.join(RELATIVE_PROJECT_ROOT_PATH, "data/index/")
OLLAMA_BASE =  "http://localhost:11434"

# API & LLM
API_BASE = os.getenv("API_BASE")  
LLM_MODEL = 'openai/gpt-4.1-mini'

API_EMB_MODEL = "openai/text-embedding-3-small"
API_EMB_API_BASE = API_BASE 

API_EMB_API_KEY = os.getenv("OPENAI_API_KEY")

if not API_EMB_API_KEY:
    raise ValueError("⚠️ ERRORE CRITICO: OPENAI_API_KEY mancante. Inseriscila nel file .env!")

GPT_MODEL = 'openai/gpt-4.1-mini'
GPT_API_BASE = API_BASE 
GPT_API_KEY = API_EMB_API_KEY

LLM_BATCH_SIZE = 10

os.environ['OPENAI_API_KEY'] = GPT_API_KEY
os.environ['OPENAI_BASE'] = API_BASE

enc = tiktoken.get_encoding("cl100k_base")
Enc_token_cnt = enc

def count_tokens(text):
    tokens = Enc_token_cnt.encode(text)
    return len(tokens)

client = OpenAI(
    base_url=GPT_API_BASE,
    api_key=GPT_API_KEY
)

# SAMPLE
SAMPLE_NUM = 5
TOPK = 5
ZENDB_TOPK = 5
GROUP_SAMPLE_NUM = 3

# CLUSTER
N_CLUSTERS = 3

# OTHERS
VALUE_OP = ['<', '>', '>=', '<=']