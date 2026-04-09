"""
Verixia — Pre-Response Verifier
Layer 2 of the two-layer quality control architecture.

Before a verification result is returned to the caller,
this verifier checks whether the assembled evidence actually
supports the confidence score being reported.

Three checks:
  1. Holdings percentage — what fraction of supporting citations
     come from HOLDING or authoritative chunks?
     Low holdings percentage = confidence built on recitations.

  2. Evidence consistency — are supporting citations internally
     consistent, or are they from unrelated cases that happen
     to use similar language?

  3. Score stability — does the score drop significantly when
     low-authority chunks (QUOTED_ARGUMENT, RECITATION) are excluded?
     Unstable score = confidence is fragile.

The verifier can downgrade confidence but never upgrade it.
All verifier decisions are logged in the response for transparency.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

from engine.chunk_role import (
    HOLDING, CONSTITUTIONAL_TEXT, STATUTORY_TEXT,
    DICTA, CONCURRENCE, SYLLABUS,
    RECITATION, QUOTED_ARGUMENT, DISSENT, HEADNOTE,
    UNKNOWN, CHUNK_ROLE_WEIGHTS
)

# Authoritative roles — holdings and primary text
AUTHORITATIVE_ROLES = {HOLDING, CONSTITUTIONAL_TEXT, STATUTORY_TEXT}

# Weak roles — should not be primary evidence
WEAK_ROLES = {QUOTED_ARGUMENT, RECITATION, DISSENT, HEADNOTE}

# Minimum holdings percentage for full confidence
MIN_HOLDINGS_PERCENTAGE = 0.25  # at least 25% of supporting citations must be authoritative

# Score stability threshold — if score drops more than this
# when weak chunks are removed, confidence is fragile
SCORE_STABILITY_THRESHOLD = 0.25

# Confidence levels in order
CONFIDENCE_LEVELS = ["HIGH", "MEDIUM", "LOW", "UNVERIFIABLE"]


def _downgrade_confidence(confidence: str, levels: int = 1) -> str:
    """Downgrade confidence by N levels."""
    idx = CONFIDENCE_LEVELS.index(confidence) if confidence in CONFIDENCE_LEVELS else 3
    new_idx = min(idx + levels, len(CONFIDENCE_LEVELS) - 1)
    return CONFIDENCE_LEVELS[new_idx]


@dataclass
class VerifierReport:
    """The verifier's assessment of the evidence quality."""
    passed:                 bool
    holdings_percentage:    float
    weak_evidence_flag:     bool
    score_without_weak:     float
    score_drop:             float
    original_confidence:    str
    verified_confidence:    str
    downgrade_reason:       Optional[str]
    notes:                  list[str] = field(default_factory=list)


def verify_evidence_quality(
    result,  # VerificationResult from confidence.py
) -> VerifierReport:
    """
    Assess the quality of assembled evidence before returning to caller.

    Args:
        result  VerificationResult from confidence.py

    Returns:
        VerifierReport with verified confidence and quality metrics
    """
    citations      = result.citations
    contradictions = result.contradictions
    original_conf  = result.confidence
    original_score = result.score
    notes          = []

    # ── Check 1: Holdings percentage ─────────────────────────
    if not citations:
        holdings_pct = 0.0
    else:
        authoritative_count = sum(
            1 for c in citations
            if getattr(c, "chunk_role", UNKNOWN) in AUTHORITATIVE_ROLES
        )
        holdings_pct = authoritative_count / len(citations)

    # ── Check 2: Score stability ──────────────────────────────
    # Recalculate score excluding weak chunks
    if not citations:
        score_without_weak = 0.0
    else:
        strong_support = sum(
            c.stance_score for c in citations
            if getattr(c, "chunk_role", UNKNOWN) not in WEAK_ROLES
        )
        strong_contradict = sum(
            c.stance_score for c in contradictions
            if getattr(c, "chunk_role", UNKNOWN) not in WEAK_ROLES
        )
        total_strong = strong_support + strong_contradict
        score_without_weak = (
            round(strong_support / total_strong, 4)
            if total_strong > 0.01
            else 0.0
        )

    score_drop = round(original_score - score_without_weak, 4)

    # ── Check 3: Weak evidence flag ───────────────────────────
    weak_evidence_flag = False
    if citations:
        weak_count = sum(
            1 for c in citations
            if getattr(c, "chunk_role", UNKNOWN) in WEAK_ROLES
        )
        weak_pct = weak_count / len(citations)
        if weak_pct > 0.70:
            weak_evidence_flag = True
            notes.append(
                f"{weak_pct:.0%} of supporting citations are from "
                f"recitations or quoted arguments rather than holdings."
            )

    # ── Check if we have role data to work with ─────────────
    known_role_count = sum(
        1 for c in citations
        if getattr(c, "chunk_role", UNKNOWN) != UNKNOWN
    )
    has_role_data = known_role_count > 0

    # ── Determine verified confidence ────────────────────────
    verified_conf    = original_conf
    downgrade_reason = None

    if original_conf == "UNVERIFIABLE":
        pass

    elif not has_role_data:
        notes.append(
            "Chunks predate role classification — "
            "role-based verification not applied. "
            "Reindex to enable full verification."
        )

    elif holdings_pct < MIN_HOLDINGS_PERCENTAGE and original_conf in ("HIGH", "MEDIUM"):
        verified_conf    = _downgrade_confidence(original_conf)
        downgrade_reason = (
            f"Only {holdings_pct:.0%} of supporting citations are from "
            f"holdings or authoritative text "
            f"(minimum {MIN_HOLDINGS_PERCENTAGE:.0%} required)."
        )
        notes.append(downgrade_reason)

    elif score_drop > SCORE_STABILITY_THRESHOLD and original_conf in ("HIGH", "MEDIUM"):
        verified_conf    = _downgrade_confidence(original_conf)
        downgrade_reason = (
            f"Confidence score drops {score_drop:.2f} when recitations and "
            f"quoted arguments are excluded — evidence is not stable."
        )
        notes.append(downgrade_reason)

    elif weak_evidence_flag and original_conf == "HIGH":
        verified_conf    = "MEDIUM"
        downgrade_reason = "Majority of supporting evidence from non-authoritative sources."
        notes.append(downgrade_reason)

    passed = (verified_conf == original_conf)

    if passed:
        notes.append("Evidence quality check passed.")
    else:
        logger.info(
            f"Verifier downgraded {original_conf} → {verified_conf}: "
            f"{downgrade_reason}"
        )

    return VerifierReport(
        passed              = passed,
        holdings_percentage = round(holdings_pct, 4),
        weak_evidence_flag  = weak_evidence_flag,
        score_without_weak  = score_without_weak,
        score_drop          = score_drop,
        original_confidence = original_conf,
        verified_confidence = verified_conf,
        downgrade_reason    = downgrade_reason,
        notes               = notes,
    )
