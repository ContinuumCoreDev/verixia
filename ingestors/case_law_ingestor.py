"""
Probatum — Case Law Ingestor
Specialized ingestor for federal court opinions.
Tuned to the structure of CourtListener documents:
  - Opinion text in multiple HTML/text fields
  - Citation extraction from opinions_cited array
  - Court, date, judge metadata
  - Structural markers: Justice delivery lines, dissents, per curiam
"""

import re
import logging
from datetime import datetime, timezone

from ingestors.base_ingestor import BaseIngestor

logger = logging.getLogger(__name__)


class CaseLawIngestor(BaseIngestor):

    doc_type = "case_law"

    # Text fields in preference order
    TEXT_FIELDS = [
        "html_with_citations",
        "html_lawbox",
        "xml_harvard",
        "html",
        "plain_text",
        "html_columbia",
        "_probatum_full_text",  # pre-resolved full text
    ]

    # Citation patterns specific to case law
    CITATION_PATTERNS = [
        r"\d+\s+U\.S\.?\s+\d+",
        r"\d+\s+S\.?\s*Ct\.?\s+\d+",
        r"\d+\s+L\.?\s*Ed\.?\s*2d\s+\d+",
        r"\d+\s+F\.\d[a-z]*\s+\d+",
        r"\d+\s+F\.\s*Supp\.?\s*\d*\s+\d+",
    ]

    def _extract_text(self, doc: dict) -> dict:
        """
        Extract opinion text from CourtListener document.
        Tries multiple fields in preference order.
        Falls back to raw_text if already populated.
        """
        # Already has substantial text from resolver
        existing = doc.get("raw_text", "")
        if existing and len(existing.strip()) > 500:
            doc["raw_text"] = self._clean_text(existing)
            return doc

        # Try each field
        raw_data = doc.get("_raw_data", {})
        best_text = ""

        for field in self.TEXT_FIELDS:
            text = raw_data.get(field, "") or doc.get(field, "") or ""
            if text and len(text.strip()) > len(best_text):
                best_text = text.strip()

        if best_text and "<" in best_text:
            best_text = self._strip_html(best_text)

        if best_text:
            doc["raw_text"] = self._clean_text(best_text)

        return doc

    def _extract_metadata(self, doc: dict) -> dict:
        """
        Extract case law specific metadata.
        Normalizes court names, dates, judge names.
        """
        raw_data = doc.get("_raw_data", {})

        # Normalize court name
        court = doc.get("court", "") or raw_data.get("court", "")
        doc["court_normalized"] = self._normalize_court(court)

        # Extract judge names
        judges = raw_data.get("judges", "") or doc.get("judge", "")
        doc["judges"] = self._extract_judges(judges)

        # Determine if SCOTUS
        doc["is_scotus"] = any(
            term in court.upper()
            for term in ["SUPREME COURT", "SCOTUS"]
        )

        # Extract syllabus if present
        syllabus = raw_data.get("syllabus", "")
        if syllabus:
            doc["syllabus"] = self._strip_html(syllabus)[:500]

        # Precedential status
        doc["precedential_status"] = raw_data.get(
            "precedential_status", "Unknown"
        )

        # Extract additional citations from text
        text = doc.get("raw_text", "")
        if text:
            inline_citations = self._extract_inline_citations(text)
            existing_cites = doc.get("cites", [])
            doc["inline_citations"] = inline_citations
            doc["total_citation_count"] = len(existing_cites) + len(inline_citations)

        return doc

    def _normalize_court(self, court: str) -> str:
        """Normalize court name to standard form."""
        court_upper = court.upper()
        if "SUPREME COURT" in court_upper:
            return "SCOTUS"
        if "CIRCUIT" in court_upper:
            match = re.search(r"(\w+)\s+CIRCUIT", court_upper)
            if match:
                return f"{match.group(1)} CIRCUIT"
        if "DISTRICT" in court_upper:
            return "DISTRICT COURT"
        return court

    def _extract_judges(self, judges_str: str) -> list[str]:
        """Extract individual judge names from a judges string."""
        if not judges_str:
            return []
        judges = re.split(r"[,;]|\band\b", judges_str)
        return [j.strip() for j in judges if j.strip()]

    def _extract_inline_citations(self, text: str) -> list[str]:
        """Extract legal citations from opinion text."""
        found = []
        seen  = set()
        for pattern in self.CITATION_PATTERNS:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                raw = match.group(0).strip()
                normalized = re.sub(r"\s+", " ", raw).upper()
                if normalized not in seen:
                    seen.add(normalized)
                    found.append(raw)
        return found[:50]  # cap at 50 inline citations
