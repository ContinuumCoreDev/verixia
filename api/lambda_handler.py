"""
Verixia — AWS Lambda Handler
Lightweight version using HuggingFace Inference API
for model calls instead of loading models locally.
Local deployment uses full models — Lambda uses API calls.
"""
import os
import warnings
os.environ["TRANSFORMERS_VERBOSITY"] = "error"
os.environ["HF_DATASETS_OFFLINE"] = "1"
warnings.filterwarnings("ignore")

from mangum import Mangum

# Monkey-patch model loaders before importing app
# so Lambda doesn't try to load 1.6GB models at init
import engine.ingest as _ingest
import engine.stance as _stance

_HF_KEY = os.environ.get("HF_API_KEY", "")
_EMBED_URL = "https://api-inference.huggingface.co/models/sentence-transformers/all-MiniLM-L6-v2"
_NLI_URL   = "https://api-inference.huggingface.co/models/facebook/bart-large-mnli"

import requests
import numpy as np

def _hf_embed(texts):
    if isinstance(texts, str):
        texts = [texts]
    r = requests.post(
        _EMBED_URL,
        headers={"Authorization": f"Bearer {_HF_KEY}"},
        json={"inputs": texts},
        timeout=30
    )
    return np.array(r.json())

def _hf_classify(text, labels):
    r = requests.post(
        _NLI_URL,
        headers={"Authorization": f"Bearer {_HF_KEY}"},
        json={"inputs": text, "parameters": {"candidate_labels": labels}},
        timeout=30
    )
    data = r.json()
    return {"labels": data["labels"], "scores": data["scores"]}

# Patch the embedding model
class _HFEmbedder:
    def encode(self, texts, **kwargs):
        return _hf_embed(texts)

class _HFClassifier:
    def __call__(self, text, labels, **kwargs):
        return _hf_classify(text, labels)

_ingest._model  = _HFEmbedder()
_stance._classifier = _HFClassifier()
_stance._embedder   = _HFEmbedder()

from api.main import app
handler = Mangum(app, lifespan="off")
