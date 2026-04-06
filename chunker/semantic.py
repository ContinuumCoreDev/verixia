"""
Verixia — Semantic Boundary Detector
Layer 2 of the multi-layer chunking system.
Uses sentence embeddings and cosine similarity to detect
topic shifts between adjacent sentences.
A sharp similarity drop = natural break in meaning.
These are medium-confidence breaks.
"""

import logging
import numpy as np
from dataclasses import dataclass
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

SEMANTIC_THRESHOLD = _cfg["chunker"]["semantic_threshold"]
EMBEDDING_MODEL    = _cfg["embedding"]["model"]
EMBEDDING_DEVICE   = _cfg["embedding"]["device"]

# Lazy-load the model — only instantiate when first needed
_model = None

def _get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        logger.info(f"Loading embedding model: {EMBEDDING_MODEL}")
        _model = SentenceTransformer(EMBEDDING_MODEL, device=EMBEDDING_DEVICE)
    return _model


@dataclass
class SemanticBreak:
    sentence_index: int   # break occurs BEFORE this sentence
    similarity:     float # cosine similarity between adjacent sentences
    confidence:     float # 1.0 - similarity (lower sim = higher confidence)


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def detect_semantic_breaks(
    sentences: list[str],
    threshold: float = None,
    exclude_indices: set = None
) -> list[SemanticBreak]:
    """
    Detect topic-shift boundaries using embedding similarity.
    Compares each sentence to the next — a drop below threshold
    indicates a natural break in meaning.

    Args:
        sentences       List of sentence strings
        threshold       Similarity threshold (default from config)
        exclude_indices Sentence indices already marked as structural
                        breaks — skip these to avoid redundant marking

    Returns:
        List of SemanticBreak at detected boundaries
    """
    if len(sentences) < 2:
        return []

    threshold      = threshold or SEMANTIC_THRESHOLD
    exclude_indices = exclude_indices or set()

    model      = _get_model()
    embeddings = model.encode(
        sentences,
        batch_size=32,
        show_progress_bar=False,
        convert_to_numpy=True
    )

    breaks = []
    for i in range(1, len(embeddings)):
        if i in exclude_indices:
            continue

        sim = cosine_similarity(embeddings[i - 1], embeddings[i])

        if sim < threshold:
            breaks.append(SemanticBreak(
                sentence_index = i,
                similarity     = round(sim, 4),
                confidence     = round(1.0 - sim, 4)
            ))

    logger.debug(
        f"Semantic detection: {len(breaks)} breaks found "
        f"in {len(sentences)} sentences "
        f"(threshold={threshold})"
    )
    return breaks
