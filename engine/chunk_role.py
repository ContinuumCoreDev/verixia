"""
Verixia — Chunk Role Classifier
Determines the structural role of a chunk within a legal opinion.
Role is assigned at ingest time and stored as chunk metadata.
Role weights are applied during stance scoring to ensure
holdings are weighted higher than recitations or quoted arguments.

This is Layer 1 of the two-layer quality control architecture.
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Role definitions ──────────────────────────────────────────

HOLDING             = "HOLDING"
DICTA               = "DICTA"
RECITATION          = "RECITATION"
QUOTED_ARGUMENT     = "QUOTED_ARGUMENT"
DISSENT             = "DISSENT"
CONCURRENCE         = "CONCURRENCE"
SYLLABUS            = "SYLLABUS"
HEADNOTE            = "HEADNOTE"
STATUTORY_TEXT      = "STATUTORY_TEXT"
CONSTITUTIONAL_TEXT = "CONSTITUTIONAL_TEXT"
UNKNOWN             = "UNKNOWN"

# ── Role weights for stance scoring ──────────────────────────
# Higher weight = more authoritative evidence

CHUNK_ROLE_WEIGHTS = {
    HOLDING:             1.00,
    CONSTITUTIONAL_TEXT: 1.00,
    STATUTORY_TEXT:      0.95,
    CONCURRENCE:         0.75,
    SYLLABUS:            0.70,
    DICTA:               0.60,
    RECITATION:          0.40,
    QUOTED_ARGUMENT:     0.20,
    DISSENT:             0.15,
    HEADNOTE:            0.10,
    UNKNOWN:             0.50,
}

# ── Detection patterns ────────────────────────────────────────

# Holding patterns — the court's actual decision
HOLDING_PATTERNS = [
    r"\bwe hold\b",
    r"\bwe conclude\b",
    r"\bwe find\b",
    r"\bwe affirm\b",
    r"\bwe reverse\b",
    r"\bwe remand\b",
    r"\bit is (the opinion|held|decided|ordered)\b",
    r"\bthe (court|judgment) (holds?|finds?|concludes?|decides?)\b",
    r"\bour holding\b",
    r"\bthis court holds?\b",
    r"\bthe judgment (of|must be|is)\b",
    r"\baccordingly[,.]?\s+(?:we|the court)\b",
    r"\bfor (these|the foregoing) reasons\b",
    r"\bthe (decision|judgment) below (is|must be)\b",
    r"\bis (affirmed|reversed|remanded|vacated)\b",
]

# Constitutional text patterns
CONSTITUTIONAL_TEXT_PATTERNS = [
    r"\bthe constitution (provides?|states?|says?|reads?)\b",
    r"\barticle [ivxlc]+[,.]?\s+section\b",
    r"\bthe (first|second|third|fourth|fifth|sixth|seventh|"
    r"eighth|ninth|tenth|eleventh|twelfth|thirteenth|"
    r"fourteenth|fifteenth|sixteenth|seventeenth|"
    r"eighteenth|nineteenth|twentieth|twenty) amendment\b",
    r"\bno (person|state|congress) shall\b",
    r"\bcongress shall make no law\b",
    r"\bwe the people\b",
    r"\bthe right of the people\b",
    r"\bnor shall any state\b",
    r"\bdue process of law\b",
    r"\bequal protection of the laws\b",
]

# Statutory text patterns
STATUTORY_TEXT_PATTERNS = [
    r"\b\d+\s+u\.s\.c\.?\s*[§s]\s*\d+\b",
    r"\bthe (act|statute|code) (provides?|states?|reads?|says?)\b",
    r"\bsection \d+ (of|provides?|states?)\b",
    r"\bpursuant to \d+ u\.s\.c\b",
    r"\bunder \d+ u\.s\.c\b",
    r"\b(pub\.?\s*l\.?|public law)\s*\d+[-–]\d+\b",
]

# Recitation patterns — restating facts or lower court findings
RECITATION_PATTERNS = [
    r"\bthe (facts|record|evidence|testimony) (show|reveal|establish|indicate)\b",
    r"\bthe (district|circuit|lower|trial) court (found|held|ruled|concluded)\b",
    r"\bthe (plaintiff|defendant|petitioner|respondent|appellant|appellee)\b"
    r"\s+(argued?|contended?|claimed?|alleged?|assert)\b",
    r"\bthe facts (of this case|are|were|as follows)\b",
    r"\bthe (case|matter) (came|comes|was brought)\b",
    r"\bthe record (reflects?|shows?|reveals?|contains?)\b",
    r"\bas the (district|trial|lower) court (found|noted|observed)\b",
]

# Quoted argument patterns — opposing counsel or party arguments
QUOTED_ARGUMENT_PATTERNS = [
    r"\b(petitioner|respondent|appellant|appellee|plaintiff|defendant)"
    r"\s+(?:argues?|contends?|asserts?|claims?|insists?|maintains?)\b",
    r"\bthe (government|state|prosecution) (argues?|contends?|asserts?)\b",
    r"\bit (is|was) (argued|contended|asserted|claimed)\b",
    r"\baccording to (petitioner|respondent|appellant|appellee)\b",
    r"\bin (his|her|their|its) (brief|argument|view|position)\b",
    r"\b(counsel|attorney) (for|argues?|contends?)\b",
]

# Dissent patterns
DISSENT_PATTERNS = [
    r"\b(justice|judge|chief justice)\s+\w+[,.]?\s+dissenting\b",
    r"\bdissenting opinion\b",
    r"\bi (dissent|respectfully dissent|would (affirm|reverse))\b",
    r"\bthe (majority|court) (errs?|is wrong|misreads?|misconstrues?)\b",
    r"\bwith (due|all) respect[,.]?\s+(?:i|the majority)\b",
    r"\bdissent(ing)?\b.*\bwould\b",
]

# Concurrence patterns
CONCURRENCE_PATTERNS = [
    r"\bconcurring\b",
    r"\bi (concur|write separately|join)\b",
    r"\bconcurrence\b",
    r"\bwhile i agree with the (result|judgment|majority)\b",
]

# Syllabus patterns
SYLLABUS_PATTERNS = [
    r"\bsyllabus\b",
    r"\bnote:\s+where it is feasible\b",
    r"\bthe syllabus\b",
]

# Headnote patterns
HEADNOTE_PATTERNS = [
    r"^\[?\d+\]?\s+[A-Z][a-z]+",  # numbered headnotes
    r"\bheadnote\b",
    r"^[A-Z][A-Z\s]+\d+[.:]",     # ALL CAPS section with number
]

# Dicta patterns — authoritative but not the holding
DICTA_PATTERNS = [
    r"\bit (is|has been|was) (well[- ])?established\b",
    r"\blong[- ]settled\b",
    r"\bwe (note|observe|recognize|acknowledge)\b",
    r"\bit (bears?|is worth) (noting|mentioning|observing)\b",
    r"\bwe (have previously|have long|have consistently)\b",
    r"\bas we (said|stated|noted|observed|held) in\b",
    r"\bour (precedent|cases|decisions) (make clear|establish|recognize)\b",
]


def _matches_any(text: str, patterns: list[str]) -> bool:
    """Check if text matches any pattern in the list."""
    text_lower = text.lower()
    for pattern in patterns:
        if re.search(pattern, text_lower, re.IGNORECASE | re.MULTILINE):
            return True
    return False


def _count_matches(text: str, patterns: list[str]) -> int:
    """Count how many patterns match."""
    text_lower = text.lower()
    count = 0
    for pattern in patterns:
        if re.search(pattern, text_lower, re.IGNORECASE | re.MULTILINE):
            count += 1
    return count


def classify_chunk_role(
    text: str,
    doc_type: str = "case_law",
    position: Optional[int] = None,
    total_chunks: Optional[int] = None,
) -> str:
    """
    Classify the structural role of a chunk within a legal document.

    Args:
        text            The chunk text
        doc_type        Document type: case_law, statute, regulation
        position        Chunk position within the document (0-indexed)
        total_chunks    Total chunks in the document

    Returns:
        Role string constant
    """
    if not text or len(text.strip()) < 20:
        return UNKNOWN

    # Normalize whitespace — OCR text has embedded newlines
    # that break pattern matching
    text = " ".join(text.split())

    # Constitutional documents get highest authority role
    if doc_type in ("constitutional_text", "constitutional_commentary"):
        # Still pattern match — but default to CONSTITUTIONAL_TEXT
        # if no more specific pattern matches
        pass
    elif doc_type == "statute":
        return STATUTORY_TEXT
    elif doc_type == "regulation":
        return STATUTORY_TEXT

    # For case law, use pattern matching with priority ordering
    # Check most specific/authoritative first

    # Syllabus and headnotes — editorial, lowest weight
    if _matches_any(text, SYLLABUS_PATTERNS):
        return SYLLABUS

    # Dissent — check early, important to weight correctly
    if _matches_any(text, DISSENT_PATTERNS):
        return DISSENT

    # Concurrence
    if _matches_any(text, CONCURRENCE_PATTERNS):
        return CONCURRENCE

    # Constitutional text — highest weight alongside holding
    if _matches_any(text, CONSTITUTIONAL_TEXT_PATTERNS):
        # But check it's not just citing the constitution in argument
        holding_matches = _count_matches(text, HOLDING_PATTERNS)
        if holding_matches > 0 or _count_matches(text, RECITATION_PATTERNS) == 0:
            return CONSTITUTIONAL_TEXT

    # Statutory text
    if _matches_any(text, STATUTORY_TEXT_PATTERNS):
        return STATUTORY_TEXT

    # Holding — the court's actual decision
    holding_count = _count_matches(text, HOLDING_PATTERNS)
    if holding_count >= 1:
        # Make sure it's not a recitation of another court's holding
        recitation_count = _count_matches(text, RECITATION_PATTERNS)
        quoted_count     = _count_matches(text, QUOTED_ARGUMENT_PATTERNS)
        if recitation_count == 0 and quoted_count == 0:
            return HOLDING
        elif holding_count > recitation_count:
            return HOLDING

    # Quoted argument — opposing counsel recitation
    if _matches_any(text, QUOTED_ARGUMENT_PATTERNS):
        # Only if no holding language present
        if not _matches_any(text, HOLDING_PATTERNS):
            return QUOTED_ARGUMENT

    # Recitation of facts or lower court findings
    if _matches_any(text, RECITATION_PATTERNS):
        return RECITATION

    # Dicta — authoritative but peripheral statements
    if _matches_any(text, DICTA_PATTERNS):
        return DICTA

    # Position-based fallback for case law
    # Early chunks tend to be syllabus/facts, late chunks tend to be holding
    if position is not None and total_chunks is not None and total_chunks > 0:
        position_ratio = position / total_chunks
        if position_ratio > 0.75:
            return DICTA  # Late chunks often contain the holding or dicta
        elif position_ratio < 0.15:
            return RECITATION  # Early chunks often contain facts/syllabus

    # Constitutional documents default to CONSTITUTIONAL_TEXT
    if doc_type in ("constitutional_text",):
        return CONSTITUTIONAL_TEXT

    return UNKNOWN


def get_role_weight(role: str) -> float:
    """Return the stance scoring weight for a given chunk role."""
    return CHUNK_ROLE_WEIGHTS.get(role, 0.50)


if __name__ == "__main__":
    # Test on known examples
    tests = [
        ("We hold that the defendant's Fourth Amendment rights were violated.", "case_law"),
        ("The petitioner argues that the statute is unconstitutional.", "case_law"),
        ("The district court found that plaintiff had failed to state a claim.", "case_law"),
        ("Congress shall make no law respecting an establishment of religion.", "case_law"),
        ("Justice Brennan, dissenting. I respectfully dissent from the majority.", "case_law"),
        ("It is well established that due process requires notice and opportunity to be heard.", "case_law"),
        ("42 U.S.C. § 1983 provides a cause of action for civil rights violations.", "statute"),
    ]

    print("Chunk Role Classification Tests:\n")
    for text, doc_type in tests:
        role   = classify_chunk_role(text, doc_type)
        weight = get_role_weight(role)
        print(f"Role: {role:<20} Weight: {weight:.2f} | {text[:70]}...")
