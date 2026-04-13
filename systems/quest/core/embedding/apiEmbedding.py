from typing import List, Union
import numpy as np
from numpy.typing import NDArray
from langchain_core.embeddings import Embeddings
from tqdm import tqdm
import litellm  # 确保你已安装 litellm
from conf.settings import count_tokens, API_EMB_MODEL, , API_EMB_API_KEY

import os

class ApiEmbeddings(Embeddings):
    def __init__(
        self, 
        model: str = API_EMB_MODEL,
        api_base: str = None,
        api_key: str = API_EMB_API_KEY,
        max_emb_size: int = 8190,
        batch_size: int = 32
    ):
        """
        Args:
            model: litellm 支持的模型名（如 Gemini、OpenAI、Qwen、Mistral 等 Embedding 模型）
            api_base: 可选，api base 地址（如用本地部署 Embedding 服务）
            api_key: 可选，API 密钥
        """
        self.max_emb_size = max_emb_size
        self.model = model
        self.api_base = api_base
        self.api_key = api_key
        self.batch_size = batch_size

        # 调用一次 embedding 接口以获得 emb_size
        test_emb = self._embed(["test"])
        self.emb_size = test_emb.shape[1] if len(test_emb.shape) == 2 else len(test_emb)

    def embed_documents(self, texts: List[str]) -> List[NDArray]:
        """批量嵌入文档"""
        return self._embed(texts)

    def embed_query(self, text: str) -> NDArray:
        """嵌入单个查询"""
        return self._embed([text])[0]

    def __call__(self, text: Union[str, List[str]]):
        if isinstance(text, str):
            return self.embed_query(text)
        return self.embed_documents(text)

    def _embed(self, sentences: List[str]) -> NDArray:
        import os # <-- Assicurati che os sia importato qui o in cima al file
        all_embeddings = []
        batch_size = self.batch_size

        for i in tqdm(range(0, len(sentences), batch_size), desc="LiteLLM Embedding", unit="batch"):
            batch = sentences[i:i+batch_size]
            batch = [sentence[:self.max_emb_size] if count_tokens(sentence) > self.max_emb_size else sentence for sentence in batch]

            kwargs = {
                "model": self.model,
                "input": batch,
                "api_key": self.api_key
            }
            
            # 1. Passiamo api_base SOLO se è un server custom (es. Ollama)
            if self.api_base and "generativelanguage.googleapis.com" not in self.api_base:
                kwargs["api_base"] = self.api_base

            # 2. FIX AGGRESSIVO: Rimuoviamo temporaneamente le variabili d'ambiente 
            # che confondono il routing interno di LiteLLM
            temp_gemini_base = os.environ.pop("GEMINI_API_BASE", None)
            temp_api_base = os.environ.pop("API_BASE", None)

            try:
                response = litellm.embedding(**kwargs, num_retries=5)
            except Exception as e:
                print(f"❌ Errore durante l'embedding: {e}")    
            finally:
                # 3. Rimettiamo tutto a posto per non rompere il resto del programma
                if temp_gemini_base is not None:
                    os.environ["GEMINI_API_BASE"] = temp_gemini_base
                if temp_api_base is not None:
                    os.environ["API_BASE"] = temp_api_base
            
            embeddings = [item["embedding"] for item in response["data"]] if response and "data" in response else []
            all_embeddings.extend(embeddings)

        return np.array(all_embeddings)
