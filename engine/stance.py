"""
Probatum — Stance Classifier
Two-pass stance detection optimized for legal text.

Key insight from testing:
  NLI models fail on abstract "supports/contradicts" labels for legal text.
  They succeed when labels describe what the text IS DOING rather than
  its relationship to a claim.

Approach:
  1. Generate claim-specific "what would this text be doing if true" labels
  2. Use semantic similarity between claim and chunk as supporting signal
  3. Combine both signals with break_type weighting
"""

import re
import logging
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

import numpy as np
import yaml

logger = logging.getLogger(__name__)

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

BREAK_TYPE_WEIGHTS = {
    "structural": 1.00,
    "first":      0.90,
    "semantic":   0.85,
    "size_guard": 0.70,
}

SUPPORTS    = "SUPPORTS"
CONTRADICTS = "CONTRADICTS"
NEUTRAL     = "NEUTRAL"

# Thresholds
SIMILARITY_NEUTRAL_THRESHOLD = 0.28
NLI_SUPPORT_THRESHOLD        = 0.55
NLI_CONTRADICT_THRESHOLD     = 0.45

_classifier = None
_embedder   = None


def _get_classifier():
    global _classifier
    if _classifier is None:
        from transformers import pipeline
        import transformers
        transformers.logging.set_verbosity_error()
        _classifier = pipeline(
            "zero-shot-classification",
            model  = "facebook/bart-large-mnli",
            device = -1,
        )
        logger.info("Stance classifier loaded: bart-large-mnli")
    return _classifier


def _get_embedder():
    global _embedder
    if _embedder is None:
        from sentence_transformers import SentenceTransformer
        _embedder = SentenceTransformer(
            _cfg["embedding"]["model"],
            device=_cfg["embedding"]["device"]
        )
    return _embedder


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def _build_labels(claim: str) -> tuple[list[str], list[str], list[str]]:
    """
    Build three label sets for characterizing chunk stance.
    Returns (support_labels, contradict_labels, neutral_labels).

    Labels describe what the text IS DOING, not its relationship
    to the claim — this approach scores reliably on legal text.
    """
    # Extract key subject from claim
    claim_lower = claim.lower()

    # Determine domain-specific action labels
    if any(w in claim_lower for w in ["court", "held", "ruling", "opinion",
                                       "decided", "established", "judicial"]):
        support_labels = [
            "the court is exercising its power to interpret the law",
            "this is a ruling or opinion of the Supreme Court",
            f"this text is evidence that {claim[:120]}",
        ]
        contradict_labels = [
            "the court is deferring to Congress or the executive",
            "this text limits or restricts judicial authority",
            f"this text is evidence against: {claim[:120]}",
        ]

    elif any(w in claim_lower for w in ["congress", "legislature", "statute",
                                         "law", "enacted", "power"]):
        support_labels = [
            "this describes legislative power or congressional authority",
            "Congress is exercising or expanding its powers",
            f"this text is evidence that {claim[:120]}",
        ]
        contradict_labels = [
            "this describes limits on congressional or legislative power",
            "the legislature has constrained or defined authority",
            f"this text is evidence against: {claim[:120]}",
        ]

    elif any(w in claim_lower for w in ["right", "rights", "civil",
                                         "liberty", "freedom", "due process"]):
        support_labels = [
            "this affirms or protects individual rights",
            "a right or liberty is being recognized or upheld",
            f"this text is evidence that {claim[:120]}",
        ]
        contradict_labels = [
            "this restricts or limits individual rights",
            "a right or liberty is being denied or constrained",
            f"this text is evidence against: {claim[:120]}",
        ]

    else:
        # Generic legal fallback
        support_labels = [
            f"this text is evidence that {claim[:120]}",
            "this confirms or establishes the stated legal principle",
            "this text directly addresses and affirms the claim",
        ]
        contradict_labels = [
            f"this text is evidence against: {claim[:120]}",
            "this contradicts or limits the stated legal principle",
            "this text directly opposes the claim",
        ]

    neutral_labels = [
        "this text is about a different legal matter",
        "this text does not address this claim",
    ]

    return support_labels, contradict_labels, neutral_labels


@dataclass
class StanceResult:
    stance:           str
    raw_score:        float
    similarity_score: float
    weighted_score:   float
    break_type:       str
    chunk_id:         str
    doc_id:           str
    published_date:   Optional[str]
    text_snippet:     str


def classify_stance(
    claim: str,
    chunk: dict,
    similarity_score: float = 0.5,
) -> StanceResult:
    """
    Classify the stance of a chunk toward a claim.

    Args:
        claim             The claim text to verify
        chunk             Payload dict from Qdrant search result
        similarity_score  Qdrant cosine similarity score

    Returns:
        StanceResult
    """
    chunk_text = chunk.get("text", "")
    break_type = chunk.get("break_type", "semantic")
    chunk_id   = chunk.get("chunk_id", "unknown")
    doc_id     = chunk.get("doc_id", "unknown")
    pub_date   = chunk.get("published_date")

    # Low relevance → NEUTRAL
    if not chunk_text.strip() or similarity_score < SIMILARITY_NEUTRAL_THRESHOLD:
        return StanceResult(
            stance            = NEUTRAL,
            raw_score         = 0.0,
            similarity_score  = similarity_score,
            weighted_score    = 0.0,
            break_type        = break_type,
            chunk_id          = chunk_id,
            doc_id            = doc_id,
            published_date    = pub_date,
            text_snippet      = chunk_text[:200],
        )

    # Build claim-specific labels
    support_labels, contradict_labels, neutral_labels = _build_labels(claim)
    all_labels = support_labels[:2] + contradict_labels[:2] + neutral_labels[:1]

    clf = _get_classifier()

    try:
        result = clf(chunk_text[:512], all_labels, multi_label=False)

        top_label = result["labels"][0]
        top_score = result["scores"][0]

        # Determine stance from which label group won
        if top_label in support_labels and top_score >= NLI_SUPPORT_THRESHOLD:
            stance    = SUPPORTS
            nli_score = top_score

        elif top_label in contradict_labels and top_score >= NLI_CONTRADICT_THRESHOLD:
            stance    = CONTRADICTS
            nli_score = top_score

        else:
            # Embedding similarity tiebreaker
            embedder  = _get_embedder()
            claim_emb = embedder.encode(claim, convert_to_numpy=True)
            chunk_emb = embedder.encode(chunk_text[:512], convert_to_numpy=True)
            char_sim  = _cosine(claim_emb, chunk_emb)

            if char_sim >= 0.72:
                stance    = SUPPORTS
                nli_score = char_sim
            elif char_sim <= 0.28 and similarity_score > 0.45:
                stance    = CONTRADICTS
                nli_score = 1.0 - char_sim
            else:
                stance    = NEUTRAL
                nli_score = 0.0

    except Exception as e:
        logger.error(f"Classifier error on {chunk_id}: {e}")
        stance    = NEUTRAL
        nli_score = 0.0

    break_weight   = BREAK_TYPE_WEIGHTS.get(break_type, 0.85)
    # sqrt(similarity) reduces penalty for moderate similarity
    # so high-NLI chunks aren't drowned out by low cosine scores
    import math
    weighted_score = round(nli_score * math.sqrt(similarity_score) * break_weight, 4)

    logger.debug(
        f"{chunk_id}: {stance} weighted={weighted_score:.4f} "
        f"nli={nli_score:.4f} sim={similarity_score:.4f}"
    )

    return StanceResult(
        stance            = stance,
        raw_score         = round(nli_score, 4),
        similarity_score  = similarity_score,
        weighted_score    = weighted_score,
        break_type        = break_type,
        chunk_id          = chunk_id,
        doc_id            = doc_id,
        published_date    = pub_date,
        text_snippet      = chunk_text[:200],
    )


def classify_batch(
    claim: str,
    chunks: list[dict],
    search_results: list[dict] = None,
) -> list[StanceResult]:
    """
    Classify stance for a list of chunks.
    Returns sorted by weighted_score descending.
    """
    sim_map = {}
    if search_results:
        for r in search_results:
            cid = r["payload"].get("chunk_id", "")
            sim_map[cid] = r["score"]

    results = []
    for chunk in chunks:
        chunk_id = chunk.get("chunk_id", "")
        sim      = sim_map.get(chunk_id, 0.5)
        result   = classify_stance(claim, chunk, similarity_score=sim)
        results.append(result)

    results.sort(key=lambda r: r.weighted_score, reverse=True)
    return results


if __name__ == "__main__":
    import os, warnings
    os.environ["TRANSFORMERS_VERBOSITY"] = "error"
    warnings.filterwarnings("ignore")
    import transformers
    transformers.logging.set_verbosity_error()

    import logging
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("engine.stance").setLevel(logging.INFO)

    print("Testing stance classifier...\n")

    from engine.ingest import search

    tests = [
        (
            "The Supreme Court established the principle of judicial review in Marbury v. Madison.",
            SUPPORTS
        ),
        (
            "The Supreme Court held that Congress has unlimited power to expand its own jurisdiction.",
            CONTRADICTS
        ),
        (
            "The weather in Washington DC is typically cold in February.",
            NEUTRAL
        ),
    ]

    for claim, expected in tests:
        results = search(claim, top_k=10)
        print(f"Claim: {claim[:75]}...")
        print(f"Expected: {expected} | Chunks: {len(results)}")

        best_s = best_c = None
        neutral = 0

        for r in results:
            sr = classify_stance(claim, r["payload"], similarity_score=r["score"])
            if sr.stance == SUPPORTS:
                if best_s is None or sr.weighted_score > best_s.weighted_score:
                    best_s = sr
            elif sr.stance == CONTRADICTS:
                if best_c is None or sr.weighted_score > best_c.weighted_score:
                    best_c = sr
            else:
                neutral += 1

        sup = best_s.weighted_score if best_s else 0.0
        con = best_c.weighted_score if best_c else 0.0
        total = sup + con

        if total < 0.01:
            overall = NEUTRAL
        elif sup / total >= 0.60:
            overall = SUPPORTS
        elif con / total >= 0.60:
            overall = CONTRADICTS
        else:
            overall = NEUTRAL

        match = "✓" if overall == expected else "✗"
        print(f"{match} Overall: {overall}")
        print(f"   Best support:    {sup:.4f} — "
              f"{best_s.text_snippet[:80]}..." if best_s else
              f"   Best support:    None")
        print(f"   Best contradict: {con:.4f} — "
              f"{best_c.text_snippet[:80]}..." if best_c else
              f"   Best contradict: None")
        print(f"   Neutral: {neutral}")
        print()
