"""
Probatum — Statute Ingestor
Specialized ingestor for federal statutes from Congress.gov.
Tuned to the structure of US Code and bill text:
  - Section-based structure with § markers
  - Defined terms blocks
  - Cross-references to other USC sections
  - Effective dates and amendment history
"""

import re
import logging
from datetime import datetime, timezone

from ingestors.base_ingestor import BaseIngestor

logger = logging.getLogger(__name__)


class StatuteIngestor(BaseIngestor):

    doc_type = "statute"

    # Structural markers specific to statutes
    SECTION_PATTERNS = [
        r"^§\s*[\d\.]+",
        r"^Sec(?:tion|\.)\s*\d+",
        r"^SECTION\s+\d+",
        r"^TITLE\s+[IVXLC\d]+",
        r"^SUBTITLE\s+[A-Z]",
        r"^CHAPTER\s+\d+",
        r"^PART\s+[IVXLC\d]+",
        r"^\([a-z]\)\s+",
        r"^\(\d+\)\s+",
    ]

    # USC citation patterns
    USC_PATTERNS = [
        r"\d+\s+U\.S\.C\.?\s*§+\s*[\d\w\-]+",
        r"Pub\.?\s*L\.?\s*\d+-\d+",
        r"\d+\s+Stat\.?\s+\d+",
    ]

    def _extract_text(self, doc: dict) -> dict:
        """
        Extract statute text from Congress.gov document.
        Bill text comes in various formats — prefer plain text.
        """
        existing = doc.get("raw_text", "")
        if existing and len(existing.strip()) > 200:
            doc["raw_text"] = self._clean_statute_text(existing)
            return doc

        raw_data = doc.get("_raw_data", {})

        # Try text fields
        for field in ["text", "body", "content", "plain_text"]:
            text = raw_data.get(field, "")
            if text and len(text.strip()) > 200:
                doc["raw_text"] = self._clean_statute_text(text)
                return doc

        return doc

    def _clean_statute_text(self, text: str) -> str:
        """
        Clean statute text — preserve section structure
        but remove formatting artifacts.
        """
        if "<" in text:
            text = self._strip_html(text)

        # Normalize section markers
        text = re.sub(r"Â§", "§", text)
        text = re.sub(r"\u00a7", "§", text)

        # Normalize whitespace while preserving paragraph breaks
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)

        return text.strip()

    def _extract_metadata(self, doc: dict) -> dict:
        """
        Extract statute-specific metadata.
        """
        raw_data = doc.get("_raw_data", {})

        # Congress number
        doc["congress_number"] = doc.get("congress") or raw_data.get("congress", "")

        # Bill type and number
        doc["bill_type"]   = doc.get("bill_type", "").upper()
        doc["bill_number"] = doc.get("bill_number", "")

        # Origin chamber
        doc["origin_chamber"] = raw_data.get("originChamber", "") or \
                                 doc.get("origin_chamber", "")

        # Sponsors
        sponsors = raw_data.get("sponsors", []) or []
        doc["sponsors"] = [
            s.get("fullName", s.get("name", ""))
            for s in sponsors[:3]
        ]

        # Extract USC cross-references from text
        text = doc.get("raw_text", "")
        if text:
            doc["usc_references"] = self._extract_usc_refs(text)

        # Policy area
        doc["policy_area"] = raw_data.get("policyArea", {}).get("name", "") \
                             if isinstance(raw_data.get("policyArea"), dict) \
                             else ""

        return doc

    def _extract_usc_refs(self, text: str) -> list[str]:
        """Extract USC cross-references from statute text."""
        found = []
        seen  = set()
        for pattern in self.USC_PATTERNS:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                raw = match.group(0).strip()
                normalized = re.sub(r"\s+", " ", raw).upper()
                if normalized not in seen:
                    seen.add(normalized)
                    found.append(raw)
        return found[:30]
