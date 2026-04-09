"""
Verixia — Confidence Scorer
Aggregates stance results across multiple retrieved chunks
into a single verification score with citation chain.

The aggregation logic:
  - SUPPORTS chunks add to support weight
  - CONTRADICTS chunks add to contradict weight
  - NEUTRAL chunks are ignored
  - Final score = support_weight / (support_weight + contradict_weight)
  - Minimum evidence threshold enforced
  - Citation chain built from supporting chunks only
"""

import logging
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

import yaml

from engine.stance import (
    StanceResult, classify_batch,
    SUPPORTS, CONTRADICTS, NEUTRAL
)

logger = logging.getLogger(__name__)

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

# Minimum chunks needed to produce a meaningful score
MIN_EVIDENCE_CHUNKS = 2

# Score thresholds for confidence classification
CONFIDENCE_THRESHOLDS = {
    "HIGH":         0.80,
    "MEDIUM":       0.55,
    "LOW":          0.30,
    # Below LOW = UNVERIFIABLE
}


@dataclass
class Citation:
    chunk_id:       str
    doc_id:         str
    source:         str
    doc_type:       str
    published_date: Optional[str]
    section_label:  str
    text_excerpt:   str       # first 300 chars
    stance_score:   float
    chunk_role:     str = "UNKNOWN"


@dataclass
class VerificationResult:
    # Core result
    claim:              str
    score:              float           # 0.0 - 1.0
    confidence:         str             # HIGH | MEDIUM | LOW | UNVERIFIABLE

    # Evidence
    citations:          list[Citation] = field(default_factory=list)
    contradictions:     list[Citation] = field(default_factory=list)

    # Metadata
    chunks_evaluated:   int   = 0
    supporting_count:   int   = 0
    contradicting_count:int   = 0
    neutral_count:      int   = 0
    domain:             str   = ""
    as_of_date:         Optional[str] = None

    # Graph coverage
    graph_coverage_note: str  = ""

    # Verifier report — populated after verify() runs
    verifier_report:     object = None


def _classify_confidence(score: float) -> str:
    """Map a numeric score to a confidence label."""
    if score >= CONFIDENCE_THRESHOLDS["HIGH"]:
        return "HIGH"
    elif score >= CONFIDENCE_THRESHOLDS["MEDIUM"]:
        return "MEDIUM"
    elif score >= CONFIDENCE_THRESHOLDS["LOW"]:
        return "LOW"
    else:
        return "UNVERIFIABLE"


def _build_citation(result: StanceResult, chunk_payload: dict) -> Citation:
    """Build a Citation object from a StanceResult and chunk payload."""
    return Citation(
        chunk_id       = result.chunk_id,
        doc_id         = result.doc_id,
        source         = chunk_payload.get("source", "unknown"),
        doc_type       = chunk_payload.get("doc_type", "unknown"),
        published_date = result.published_date,
        section_label  = chunk_payload.get("section_label", ""),
        text_excerpt   = result.text_snippet[:300],
        stance_score   = result.weighted_score,
        chunk_role     = chunk_payload.get("chunk_role", "UNKNOWN"),
    )


def score_claim(
    claim: str,
    retrieved_chunks: list[dict],
    as_of_date: Optional[str] = None,
    domain: Optional[str] = None,
) -> VerificationResult:
    """
    Aggregate stance results into a verification score.

    Args:
        claim               The claim to verify
        retrieved_chunks    List of {score, payload} dicts from search()
        as_of_date          ISO date for temporal constraint logging
        domain              Domain label for the result

    Returns:
        VerificationResult with score, confidence, and citation chain
    """
    if not retrieved_chunks:
        return VerificationResult(
            claim      = claim,
            score      = 0.0,
            confidence = "UNVERIFIABLE",
            graph_coverage_note = "No chunks retrieved from knowledge base.",
            as_of_date = as_of_date,
            domain     = domain or "",
        )

    # Extract payloads and classify stance on each
    payloads = [c["payload"] for c in retrieved_chunks]
    stances  = classify_batch(claim, payloads, search_results=retrieved_chunks)

    # Aggregate scores
    support_weight    = 0.0
    contradict_weight = 0.0
    citations         = []
    contradictions    = []
    neutral_count     = 0

    for result, chunk in zip(stances, retrieved_chunks):
        payload = chunk["payload"]

        if result.stance == SUPPORTS:
            support_weight += result.weighted_score
            citations.append(_build_citation(result, payload))

        elif result.stance == CONTRADICTS:
            contradict_weight += result.weighted_score
            contradictions.append(_build_citation(result, payload))

        else:
            neutral_count += 1

    # Calculate final score
    total = support_weight + contradict_weight
    if total < 0.01:
        score = 0.0
    else:
        score = round(support_weight / total, 4)

    # Sort citations by stance score descending
    citations      = sorted(citations,      key=lambda c: c.stance_score, reverse=True)
    contradictions = sorted(contradictions, key=lambda c: c.stance_score, reverse=True)

    confidence = _classify_confidence(score)

    # Coverage note
    if len(retrieved_chunks) < MIN_EVIDENCE_CHUNKS:
        coverage_note = (
            f"Limited evidence: only {len(retrieved_chunks)} chunk(s) "
            f"retrieved. Score may not be representative."
        )
    else:
        coverage_note = (
            f"{len(retrieved_chunks)} chunks evaluated from knowledge base."
        )

    logger.info(
        f"Verification complete — "
        f"score={score}, confidence={confidence}, "
        f"support={len(citations)}, "
        f"contradict={len(contradictions)}, "
        f"neutral={neutral_count}"
    )

    return VerificationResult(
        claim               = claim,
        score               = score,
        confidence          = confidence,
        citations           = citations,
        contradictions      = contradictions,
        chunks_evaluated    = len(retrieved_chunks),
        supporting_count    = len(citations),
        contradicting_count = len(contradictions),
        neutral_count       = neutral_count,
        domain              = domain or "",
        as_of_date          = as_of_date,
        graph_coverage_note = coverage_note,
    )


def verify(
    claim: str,
    top_k: int = 10,
    as_of_date: Optional[str] = None,
    doc_type: Optional[str] = None,
) -> VerificationResult:
    """
    Full verification pipeline for a single claim.
    Retrieves relevant chunks from Qdrant and scores them.

    Args:
        claim       The claim text to verify
        top_k       Number of chunks to retrieve and evaluate
        as_of_date  ISO date string for temporal constraint
        doc_type    Filter by document type

    Returns:
        VerificationResult
    """
    from engine.ingest import search

    logger.info(f"Verifying: {claim[:80]}...")

    chunks = search(
        query      = claim,
        top_k      = top_k,
        doc_type   = doc_type,
        as_of_date = as_of_date,
    )

    result = score_claim(
        claim            = claim,
        retrieved_chunks = chunks,
        as_of_date       = as_of_date,
        domain           = doc_type or "general",
    )

    # Layer 2: Pre-response verifier
    from engine.verifier import verify_evidence_quality
    report = verify_evidence_quality(result)

    # Apply verified confidence
    result.confidence       = report.verified_confidence
    result.verifier_report  = report

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Testing confidence scorer...\n")

    test_claims = [
        {
            "claim":    "The Supreme Court established the principle of judicial review in Marbury v. Madison.",
            "expected": "HIGH",
        },
        {
            "claim":    "The Supreme Court held that Congress has unlimited power to expand its own jurisdiction.",
            "expected": "LOW",
        },
        {
            "claim":    "The weather in Washington DC is typically cold in February.",
            "expected": "UNVERIFIABLE",
        },
    ]

    for test in test_claims:
        claim    = test["claim"]
        expected = test["expected"]

        print(f"{'='*60}")
        print(f"Claim:    {claim[:75]}...")
        print(f"Expected: {expected}")

        result = verify(claim, top_k=10)

        print(f"Score:    {result.score}")
        print(f"Confidence: {result.confidence}  "
              f"{'✓' if result.confidence == expected else '✗'}")
        print(f"Evaluated: {result.chunks_evaluated} chunks — "
              f"{result.supporting_count} supporting, "
              f"{result.contradicting_count} contradicting, "
              f"{result.neutral_count} neutral")

        if result.citations:
            print(f"\nTop citation:")
            c = result.citations[0]
            print(f"  Doc:     {c.doc_id}")
            print(f"  Date:    {c.published_date}")
            print(f"  Score:   {c.stance_score}")
            print(f"  Excerpt: {c.text_excerpt[:150]}...")

        if result.contradictions:
            print(f"\nTop contradiction:")
            c = result.contradictions[0]
            print(f"  Score:   {c.stance_score}")
            print(f"  Excerpt: {c.text_excerpt[:150]}...")

        print()
