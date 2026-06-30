import os
from pathlib import Path
from dotenv import load_dotenv
import tiktoken

load_dotenv()

from db.connector.connector import create_opengauss_engine


def _normalize_azure_endpoint(value: str | None) -> str:
    value = (value or "").strip().rstrip("/")
    for suffix in ("/openai/v1", "/openai"):
        if value.lower().endswith(suffix):
            return value[: -len(suffix)].rstrip("/")
    return value

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
OLLAMA_BASE = "http://localhost:11434"

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = _normalize_azure_endpoint(os.getenv("AZURE_OPENAI_ENDPOINT"))
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")

missing_azure_vars = [
    name
    for name, value in {
        "AZURE_OPENAI_API_KEY": AZURE_OPENAI_API_KEY,
        "AZURE_OPENAI_ENDPOINT": AZURE_OPENAI_ENDPOINT,
        "AZURE_OPENAI_DEPLOYMENT": AZURE_OPENAI_DEPLOYMENT,
        "OPENAI_API_VERSION": OPENAI_API_VERSION,
    }.items()
    if not value
]
if missing_azure_vars:
    raise ValueError(f"Missing Azure OpenAI configuration: {', '.join(missing_azure_vars)}")

LLM_MODEL = f"azure/{AZURE_OPENAI_DEPLOYMENT}"

# QUEST uses local E5 embeddings by default. These API embedding settings are
# retained only for compatibility with the optional ApiEmbeddings path.
API_EMB_MODEL = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "")
API_EMB_API_KEY = AZURE_OPENAI_API_KEY

GPT_MODEL = LLM_MODEL
GPT_API_BASE = AZURE_OPENAI_ENDPOINT
GPT_API_KEY = API_EMB_API_KEY
GPT_API_VERSION = OPENAI_API_VERSION

LLM_BATCH_SIZE = int(os.getenv("QUEST_LLM_BATCH_SIZE", "5"))
LLM_MAX_RETRIES = int(os.getenv("QUEST_LLM_MAX_RETRIES", "6"))
LLM_RETRY_BASE_SECONDS = float(os.getenv("QUEST_LLM_RETRY_BASE_SECONDS", "10"))

os.environ["AZURE_API_KEY"] = GPT_API_KEY
os.environ["AZURE_API_BASE"] = GPT_API_BASE
os.environ["AZURE_API_VERSION"] = GPT_API_VERSION

# Backward-compatible name used by old call sites; Azure calls should use GPT_API_BASE.
GEMINI_API_BASE = None

enc = tiktoken.get_encoding("cl100k_base")
Enc_token_cnt = enc


def count_tokens(text):
    return len(enc.encode(text or ""))


# SAMPLE
SAMPLE_NUM = 5
TOPK = 5
ZENDB_TOPK = 5
GROUP_SAMPLE_NUM = 3

# CLUSTER
N_CLUSTERS = 3

# OTHERS
VALUE_OP = ['<', '>', '>=', '<=']
