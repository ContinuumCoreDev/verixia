"""
Verixia — Regulation Ingestor
Specialized ingestor for federal regulations from regulations.gov.
Tuned to the structure of CFR documents and Federal Register notices:
  - Agency identification
  - CFR part and section references
  - Preamble, operative text, appendices
  - Effective dates and comment periods
"""

import re
import logging
from datetime import datetime, timezone

from ingestors.base_ingestor import BaseIngestor

logger = logging.getLogger(__name__)


class RegulationIngestor(BaseIngestor):

    doc_type = "regulation"

    # CFR citation patterns
    CFR_PATTERNS = [
        r"\d+\s+C\.F\.R\.?\s*§+\s*[\d\.]+",
        r"\d+\s+Fed\.?\s*Reg\.?\s+\d+",
        r"Executive\s+Order\s+\d+",
    ]

    # Standard FR document sections
    FR_SECTIONS = [
        "AGENCY", "ACTION", "SUMMARY", "DATES",
        "ADDRESSES", "FOR FURTHER INFORMATION",
        "SUPPLEMENTARY INFORMATION", "BACKGROUND",
        "DISCUSSION", "REGULATORY ANALYSIS",
        "STATUTORY AUTHORITY", "EFFECTIVE DATE",
    ]

    def _extract_text(self, doc: dict) -> dict:
        """
        Extract regulation text from regulations.gov document.
        Federal Register documents have structured sections.
        """
        existing = doc.get("raw_text", "")
        if existing and len(existing.strip()) > 200:
            doc["raw_text"] = self._clean_regulation_text(existing)
            return doc

        raw_data = doc.get("_raw_data", {})
        attrs    = raw_data.get("attributes", {}) if raw_data else {}

        # Try text fields in order
        for field in ["fullTextXml", "abstract", "body", "text"]:
            text = attrs.get(field, "") or raw_data.get(field, "")
            if text and len(text.strip()) > 100:
                doc["raw_text"] = self._clean_regulation_text(text)
                return doc

        return doc

    def _clean_regulation_text(self, text: str) -> str:
        """
        Clean regulation text — preserve FR section structure.
        """
        if "<" in text:
            text = self._strip_html(text)

        # Normalize CFR symbols
        text = re.sub(r"Â§", "§", text)
        text = re.sub(r"\u00a7", "§", text)

        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)

        return text.strip()

    def _extract_metadata(self, doc: dict) -> dict:
        """
        Extract regulation-specific metadata.
        """
        raw_data = doc.get("_raw_data", {})
        attrs    = raw_data.get("attributes", {}) if raw_data else {}

        # Agency
        doc["agency"] = doc.get("agency") or \
                        attrs.get("agencyId", "") or \
                        attrs.get("agency", "")

        # Document type
        doc["regulation_type"] = attrs.get("documentType", "") or \
                                  doc.get("document_type", "")

        # Docket
        doc["docket_id"] = doc.get("docket_id") or \
                           attrs.get("docketId", "")

        # CFR parts affected
        cfr_parts = attrs.get("cfrPart", []) or []
        doc["cfr_parts"] = cfr_parts if isinstance(cfr_parts, list) else [cfr_parts]

        # Comment period
        doc["comment_end_date"] = attrs.get("commentEndDate", "")

        # Extract CFR cross-references from text
        text = doc.get("raw_text", "")
        if text:
            doc["cfr_references"] = self._extract_cfr_refs(text)

        # Detect document subtype from text
        doc["fr_document_type"] = self._detect_fr_type(
            doc.get("regulation_type", ""),
            text[:500] if text else ""
        )

        return doc

    def _extract_cfr_refs(self, text: str) -> list[str]:
        """Extract CFR cross-references from regulation text."""
        found = []
        seen  = set()
        for pattern in self.CFR_PATTERNS:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                raw = match.group(0).strip()
                normalized = re.sub(r"\s+", " ", raw).upper()
                if normalized not in seen:
                    seen.add(normalized)
                    found.append(raw)
        return found[:30]

    def _detect_fr_type(self, doc_type: str, text_preview: str) -> str:
        """Classify FR document as Final Rule, Proposed Rule, Notice, etc."""
        combined = (doc_type + " " + text_preview).upper()
        if "FINAL RULE" in combined:
            return "Final Rule"
        if "PROPOSED RULE" in combined or "NPRM" in combined:
            return "Proposed Rule"
        if "INTERIM RULE" in combined:
            return "Interim Rule"
        if "NOTICE" in combined:
            return "Notice"
        return doc_type or "Unknown"
