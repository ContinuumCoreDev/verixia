"""
Verixia — Generic Ingestor
Fallback ingestor for documents that couldn't be classified
or don't match a known document type.

Applies basic text extraction and normalization.
Logs every use — patterns in what falls through here
indicate when a new type-specific ingestor should be built.
"""

import re
import logging
from datetime import datetime, timezone

from ingestors.base_ingestor import BaseIngestor

logger = logging.getLogger(__name__)


class GenericIngestor(BaseIngestor):

    doc_type = "unknown"

    def _extract_text(self, doc: dict) -> dict:
        """
        Best-effort text extraction for unknown document types.
        Tries all common text fields and takes the longest.
        """
        existing = doc.get("raw_text", "")
        if existing and len(existing.strip()) > 200:
            doc["raw_text"] = self._clean_text(existing)
            return doc

        raw_data = doc.get("_raw_data", {})
        best_text = ""

        # Try every possible text field
        text_fields = [
            "plain_text", "html_with_citations", "html",
            "html_lawbox", "xml_harvard", "text", "body",
            "content", "abstract", "summary", "fullTextXml",
            "_verixia_full_text",
        ]

        for field in text_fields:
            text = raw_data.get(field, "") or doc.get(field, "") or ""
            if text and len(text.strip()) > len(best_text):
                best_text = text.strip()

        if best_text and "<" in best_text:
            best_text = self._strip_html(best_text)

        if best_text:
            doc["raw_text"] = self._clean_text(best_text)

        # Log for pattern detection
        logger.warning(
            f"GenericIngestor used for {doc.get('doc_id', 'unknown')} "
            f"from source {doc.get('source', 'unknown')} — "
            f"consider building a type-specific ingestor for this source"
        )

        return doc

    def _extract_metadata(self, doc: dict) -> dict:
        """
        Minimal metadata extraction for unknown types.
        Preserves whatever fields already exist.
        """
        doc["ingestor_note"] = (
            "Generic ingestor used — document type could not be classified. "
            "Metadata extraction is minimal."
        )
        return doc


# ── Ingestor Router ───────────────────────────────────────────

def get_ingestor(doc_type: str) -> BaseIngestor:
    """
    Route a document to the correct type-specific ingestor.
    Falls back to GenericIngestor for unknown types.

    Args:
        doc_type    Document type string from classifier

    Returns:
        Instantiated ingestor ready to call .ingest(doc)
    """
    from ingestors.case_law_ingestor    import CaseLawIngestor
    from ingestors.statute_ingestor     import StatuteIngestor
    from ingestors.regulation_ingestor  import RegulationIngestor

    router = {
        "case_law":   CaseLawIngestor,
        "statute":    StatuteIngestor,
        "regulation": RegulationIngestor,
    }

    ingestor_class = router.get(doc_type, GenericIngestor)
    return ingestor_class()
