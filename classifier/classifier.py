"""
Verixia — Document Type Classifier
Routes retrieved documents to the correct type-specific ingestor.
Source-based classification is primary. Content-based is fallback.
"""

import re
import logging

logger = logging.getLogger(__name__)

# Known source → type mappings (most reliable)
SOURCE_TYPE_MAP = {
    "courtlistener":    "case_law",
    "congress_gov":     "statute",
    "regulations_gov":  "regulation",
}

# Content-based fallback patterns
CONTENT_PATTERNS = {
    "case_law": [
        r"SUPREME COURT OF THE UNITED STATES",
        r"UNITED STATES COURT OF APPEALS",
        r"UNITED STATES DISTRICT COURT",
        r"\bv\.\s+[A-Z][a-z]+",           # "Smith v. Jones" pattern
        r"No\.\s+\d{2}-\d+",              # Docket number
        r"CERTIORARI TO THE",
        r"Per Curiam",
        r"Justice\s+[A-Z][a-z]+ delivered",
    ],
    "statute": [
        r"BE IT ENACTED BY THE CONGRESS",
        r"PUBLIC LAW\s+\d+-\d+",
        r"U\.S\.C\.\s*§",
        r"STAT\.\s+\d+",
        r"An Act to\b",
        r"TITLE\s+[IVXLC]+\b",
    ],
    "regulation": [
        r"CODE OF FEDERAL REGULATIONS",
        r"C\.F\.R\.\s*§",
        r"FEDERAL REGISTER",
        r"Fed\.\s*Reg\.",
        r"AGENCY:\s+",
        r"ACTION:\s+(Final|Proposed|Interim)\s+Rule",
        r"SUMMARY:\s+This\s+(rule|regulation)",
    ],
}


def classify_document(doc: dict) -> str:
    """
    Classify a document by type.
    Returns: 'case_law' | 'statute' | 'regulation' | 'unknown'

    doc dict expected keys:
        source      str   — where it was fetched from
        raw_text    str   — first 1000 chars is sufficient for classification
        metadata    dict  — optional additional metadata
    """
    source = doc.get("source", "").lower().replace("-", "_").replace(".", "_")
    raw_text = doc.get("raw_text", "")[:1000]

    # --- Pass 1: Source-based (most reliable) ---
    for source_key, doc_type in SOURCE_TYPE_MAP.items():
        if source_key in source:
            logger.debug(f"Classified as '{doc_type}' via source: {source}")
            return doc_type

    # --- Pass 2: Content-based fallback ---
    scores = {doc_type: 0 for doc_type in CONTENT_PATTERNS}

    for doc_type, patterns in CONTENT_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, raw_text, re.IGNORECASE):
                scores[doc_type] += 1

    best_type = max(scores, key=scores.get)
    best_score = scores[best_type]

    if best_score >= 2:
        logger.debug(
            f"Classified as '{best_type}' via content "
            f"(score: {best_score}) — source: {source}"
        )
        return best_type

    # --- Pass 3: Unknown — routes to generic ingestor ---
    logger.warning(
        f"Could not classify document from source '{source}'. "
        f"Routing to generic ingestor. Scores: {scores}"
    )
    return "unknown"


def classify_batch(docs: list[dict]) -> list[dict]:
    """
    Classify a list of documents.
    Attaches 'doc_type' key to each doc dict in place.
    Returns the same list with doc_type populated.
    """
    for doc in docs:
        doc["doc_type"] = classify_document(doc)
    return docs
