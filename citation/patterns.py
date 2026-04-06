"""
Verixia — Legal Citation Pattern Library
Regex patterns for extracting citations from legal documents.
Organized by document type.
"""

import re

# ── Case Law Citations ─────────────────────────────────────────
CASE_LAW_PATTERNS = [
    r'\d+\s+U\.S\.?\s+\d+',              # 347 U.S. 483
    r'\d+\s+S\.?\s*Ct\.?\s+\d+',         # 123 S. Ct. 456
    r'\d+\s+L\.?\s*Ed\.?\s*2d\s+\d+',    # 98 L. Ed. 2d 12
    r'\d+\s+F\.\d[a-z]*\s+\d+',          # 42 F.3d 1188
    r'\d+\s+F\.\s*Supp\.?\s*\d*\s+\d+',  # 123 F. Supp. 2d 456
    r'\d+\s+F\.\s*App\'?x\s+\d+',        # 45 F. App'x 123
]

# ── Statute Citations ──────────────────────────────────────────
STATUTE_PATTERNS = [
    r'\d+\s+U\.S\.C\.?\s*§+\s*[\d\w\-]+',    # 42 U.S.C. § 1983
    r'Pub\.?\s*L\.?\s*\d+-\d+',               # Pub. L. 107-56
    r'\d+\s+Stat\.?\s+\d+',                   # 115 Stat. 272
    r'§+\s*\d+[\.\d]*\s*\([a-z]\)',           # § 42.1(a)
]

# ── Regulation Citations ───────────────────────────────────────
REGULATION_PATTERNS = [
    r'\d+\s+C\.F\.R\.?\s*§+\s*[\d\.]+',      # 47 C.F.R. § 73.3555
    r'\d+\s+Fed\.?\s*Reg\.?\s+\d+',           # 86 Fed. Reg. 1234
    r'Executive\s+Order\s+\d+',               # Executive Order 13769
]

# ── Combined — all patterns ────────────────────────────────────
ALL_PATTERNS = CASE_LAW_PATTERNS + STATUTE_PATTERNS + REGULATION_PATTERNS

# ── Type classification hints ──────────────────────────────────
PATTERN_TYPE_MAP = {
    "case_law":   CASE_LAW_PATTERNS,
    "statute":    STATUTE_PATTERNS,
    "regulation": REGULATION_PATTERNS,
}


def extract_citations(text: str, doc_type: str = None) -> list[dict]:
    """
    Extract all legal citations from a block of text.

    Args:
        text        The document text to scan
        doc_type    If provided, also run type-specific patterns first

    Returns:
        List of dicts: {raw, normalized, citation_type}
    """
    found = []
    seen  = set()

    # Run all patterns
    for ctype, patterns in PATTERN_TYPE_MAP.items():
        for pattern in patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                raw = match.group(0).strip()
                normalized = normalize_citation(raw)
                if normalized not in seen:
                    seen.add(normalized)
                    found.append({
                        "raw":           raw,
                        "normalized":    normalized,
                        "citation_type": ctype,
                    })

    return found


def normalize_citation(raw: str) -> str:
    """
    Normalize a citation string for deduplication and registry lookup.
    Strips extra whitespace, standardizes spacing around punctuation.
    """
    normalized = raw.strip()
    normalized = re.sub(r'\s+', ' ', normalized)
    normalized = re.sub(r'\s*§\s*', ' § ', normalized)
    normalized = re.sub(r'\s*\.\s*', '.', normalized)
    normalized = normalized.upper()
    return normalized
