"""
Verixia — Verification API Routes
POST /v1/verify  — verify a claim against the knowledge graph
GET  /v1/claims/{claim_id}  — retrieve a stored verification
GET  /v1/stats  — registry and collection statistics
"""

import logging
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["verification"])


# ── Request / Response Models ─────────────────────────────────

class VerifyRequest(BaseModel):
    claim:      str   = Field(..., min_length=10, max_length=2000,
                              description="The claim to verify")
    domain:     Optional[str]  = Field(None,
                              description="Document type filter: case_law | statute | regulation")
    as_of_date: Optional[str]  = Field(None,
                              description="ISO date — only use sources published on or before this date")
    top_k:      int   = Field(10, ge=3, le=50,
                              description="Number of chunks to retrieve and evaluate")
    store:      bool  = Field(True,
                              description="Store result in verification registry")


class CitationOut(BaseModel):
    chunk_id:       str
    doc_id:         str
    source:         str
    doc_type:       str
    published_date: Optional[str]
    section_label:  str
    text_excerpt:   str
    stance_score:   float


class VerifyResponse(BaseModel):
    claim_id:           str
    claim:              str
    score:              float
    confidence:         str
    citations:          list[CitationOut]
    contradictions:     list[CitationOut]
    chunks_evaluated:   int
    supporting_count:   int
    contradicting_count:int
    neutral_count:      int
    domain:             str
    as_of_date:         Optional[str]
    graph_coverage_note:str
    audit_trail:        dict
    evidence_quality:   dict


class ClaimResponse(BaseModel):
    claim_id:           str
    claim_text:         str
    domain:             str
    current_score:      float
    current_confidence: str
    first_scored:       str
    last_scored:        str
    score_history:      list[dict]
    as_of_date:         Optional[str]


class StatsResponse(BaseModel):
    registry:   dict
    collection: dict


# ── Routes ────────────────────────────────────────────────────

@router.post("/verify", response_model=VerifyResponse)
async def verify_claim(request: VerifyRequest):
    """
    Verify a claim against the Verixia knowledge graph.

    Returns a confidence score, full citation chain,
    contradicting evidence, and audit trail.

    The as_of_date parameter enforces temporal constraints —
    only sources published on or before that date are used.
    This prevents anachronistic citations in legal contexts.
    """
    from engine.confidence import verify
    from engine.registry   import record_verification, _claim_id, _graph_version
    from engine.ingest     import collection_stats

    logger.info(
        f"Verify request: '{request.claim[:60]}...' "
        f"domain={request.domain} as_of={request.as_of_date}"
    )

    try:
        result = verify(
            claim      = request.claim,
            top_k      = request.top_k,
            as_of_date = request.as_of_date,
            doc_type   = request.domain,
        )
    except Exception as e:
        logger.error(f"Verification error: {e}")
        raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")

    # Store in registry
    claim_id = _claim_id(request.claim)
    if request.store:
        try:
            claim_id = record_verification(result)
        except Exception as e:
            logger.warning(f"Registry storage failed: {e}")

    # Build audit trail
    try:
        col_stats = collection_stats()
    except Exception:
        col_stats = {}

    audit_trail = {
        "sources_queried": col_stats.get("points_count", 0),
        "graph_version":   _graph_version(),
        "temporal_filter": request.as_of_date or "none",
        "top_k":           request.top_k,
    }

    # Build citation output
    def citation_to_out(c) -> CitationOut:
        return CitationOut(
            chunk_id       = c.chunk_id,
            doc_id         = c.doc_id,
            source         = c.source,
            doc_type       = c.doc_type,
            published_date = c.published_date,
            section_label  = c.section_label,
            text_excerpt   = c.text_excerpt,
            stance_score   = c.stance_score,
        )

    return VerifyResponse(
        claim_id            = claim_id,
        claim               = result.claim,
        score               = result.score,
        confidence          = result.confidence,
        citations           = [citation_to_out(c) for c in result.citations],
        contradictions      = [citation_to_out(c) for c in result.contradictions],
        chunks_evaluated    = result.chunks_evaluated,
        supporting_count    = result.supporting_count,
        contradicting_count = result.contradicting_count,
        neutral_count       = result.neutral_count,
        domain              = result.domain,
        as_of_date          = result.as_of_date,
        graph_coverage_note = result.graph_coverage_note,
        audit_trail         = audit_trail,
        evidence_quality    = {
            "holdings_percentage":  getattr(getattr(result, "verifier_report", None), "holdings_percentage", None),
            "weak_evidence_flag":   getattr(getattr(result, "verifier_report", None), "weak_evidence_flag", None),
            "score_without_weak":   getattr(getattr(result, "verifier_report", None), "score_without_weak", None),
            "score_drop":           getattr(getattr(result, "verifier_report", None), "score_drop", None),
            "original_confidence":  getattr(getattr(result, "verifier_report", None), "original_confidence", None),
            "verifier_passed":      getattr(getattr(result, "verifier_report", None), "passed", None),
            "downgrade_reason":     getattr(getattr(result, "verifier_report", None), "downgrade_reason", None),
            "notes":                getattr(getattr(result, "verifier_report", None), "notes", []),
        },
    )


@router.get("/claims/{claim_id}", response_model=ClaimResponse)
async def get_claim(claim_id: str):
    """
    Retrieve a stored verification result by claim ID.
    Returns the full score history for audit purposes.
    """
    from engine.registry import get_claim_by_id

    entry = get_claim_by_id(claim_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")

    return ClaimResponse(
        claim_id           = entry["claim_id"],
        claim_text         = entry["claim_text"],
        domain             = entry["domain"],
        current_score      = entry["current_score"],
        current_confidence = entry["current_confidence"],
        first_scored       = entry["first_scored"],
        last_scored        = entry["last_scored"],
        score_history      = entry["score_history"],
        as_of_date         = entry["as_of_date"],
    )


@router.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    Return registry and collection statistics.
    Used for monitoring graph growth and coverage.
    """
    from engine.registry import registry_stats
    from engine.ingest   import collection_stats

    return StatsResponse(
        registry   = registry_stats(),
        collection = collection_stats(),
    )
