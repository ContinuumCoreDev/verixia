"""
Verixia — Structural Marker Detector
Layer 1 of the multi-layer chunking system.
Detects hard boundary markers in legal text —
section numbers, statute references, clause openers,
all-caps headers, subsection markers.
These are high-confidence breaks.
"""

import re
from dataclasses import dataclass

@dataclass
class StructuralBreak:
    sentence_index: int
    marker_type: str
    matched_text: str
    confidence: float = 1.0


# ── Pattern library by document type ──────────────────────────

CASE_LAW_PATTERNS = [
    (r"^\s{0,4}[IVX]+\.\s+[A-Z]",                    "roman_section"),
    (r"^\s{0,4}[A-Z][A-Z\s]{4,}$",                   "allcaps_header"),
    (r"\bJustice\s+[A-Z][a-z]+\s+(delivered|wrote|dissenting|concurring)", "opinion_marker"),
    (r"\bPer Curiam\b",                                "per_curiam"),
    (r"^\s{0,4}[A-Z]\.\s+[A-Z]",                      "lettered_section"),
    (r"\bIT IS (HEREBY )?(ORDERED|ADJUDGED|DECREED)\b","order_marker"),
    (r"^\s{0,4}DISSENT(ING)?(\s+OPINION)?\b",         "dissent_marker"),
    (r"^\s{0,4}CONCURR(ING|ENCE)(\s+OPINION)?\b",     "concurrence_marker"),
    (r"^\s{0,4}SYLLABUS\b",                            "syllabus_marker"),
    (r"^\s{0,4}BACKGROUND\b",                          "background_marker"),
    (r"^\s{0,4}(ANALYSIS|DISCUSSION|CONCLUSION)\b",    "section_header"),
    (r"^\s{0,4}\*{1,3}\d+\s",                         "page_break_marker"),
]

STATUTE_PATTERNS = [
    (r"^\s{0,4}(ARTICLE|SECTION|PART)\s+[IVXLC\d]+",  "article_section"),
    (r"^\s{0,4}§\s*[\d\.]+",                           "section_symbol"),
    (r"^\s{0,4}\d+\.\s+[A-Z]",                         "numbered_section"),
    (r"^\s{0,4}\([a-z]\)\s",                            "subsection_lower"),
    (r"^\s{0,4}\(\d+\)\s",                              "subsection_number"),
    (r"\bWHEREAS\b",                                    "whereas_clause"),
    (r"\bNOW,?\s+THEREFORE\b",                         "therefore_clause"),
    (r"\bBE IT ENACTED\b",                              "enactment_clause"),
    (r"^\s{0,4}TITLE\s+[IVXLC\d]+",                   "title_marker"),
    (r"^\s{0,4}Sec\.\s*\d+",                           "sec_marker"),
]

REGULATION_PATTERNS = [
    (r"^\s{0,4}§\s*[\d\.]+",                           "cfr_section"),
    (r"^\s{0,4}PART\s+\d+",                            "part_marker"),
    (r"^\s{0,4}Subpart\s+[A-Z]",                       "subpart_marker"),
    (r"\bAGENCY:\s+",                                   "agency_marker"),
    (r"\bACTION:\s+",                                   "action_marker"),
    (r"\bSUMMARY:\s+",                                  "summary_marker"),
    (r"\bSUPPLEMENTARY INFORMATION:\s*",               "supplementary_marker"),
    (r"^\s{0,4}[A-Z][A-Z\s]{4,}:\s*$",                "allcaps_label"),
]

GENERIC_PATTERNS = [
    (r"^\s{0,4}[A-Z][A-Z\s]{4,}$",                    "allcaps_header"),
    (r"^\s{0,4}\d+\.\s+[A-Z]",                         "numbered_item"),
    (r"^\s{0,4}[IVX]+\.\s+[A-Z]",                     "roman_numeral"),
]

PATTERN_MAP = {
    "case_law":   CASE_LAW_PATTERNS + GENERIC_PATTERNS,
    "statute":    STATUTE_PATTERNS + GENERIC_PATTERNS,
    "regulation": REGULATION_PATTERNS + GENERIC_PATTERNS,
    "unknown":    GENERIC_PATTERNS,
}


def detect_structural_breaks(
    sentences: list[str],
    doc_type: str = "unknown"
) -> list[StructuralBreak]:
    """
    Scan a list of sentences for structural break markers.
    Returns a list of StructuralBreak objects at the detected indices.

    Args:
        sentences   List of sentence strings
        doc_type    Document type for pattern selection

    Returns:
        List of StructuralBreak — one per detected marker
    """
    patterns = PATTERN_MAP.get(doc_type, GENERIC_PATTERNS)
    breaks   = []

    for i, sentence in enumerate(sentences):
        if i == 0:
            continue  # Never break before the first sentence

        for pattern, marker_type in patterns:
            match = re.search(pattern, sentence, re.IGNORECASE | re.MULTILINE)
            if match:
                breaks.append(StructuralBreak(
                    sentence_index = i,
                    marker_type    = marker_type,
                    matched_text   = match.group(0).strip()[:80],
                    confidence     = 1.0
                ))
                break  # One break per sentence max

    return breaks
