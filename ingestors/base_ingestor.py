"""
Probatum — Base Ingestor
Abstract base class that all type-specific ingestors inherit from.
Defines the contract: every ingestor receives a raw Probatum document
and returns a normalized document with full text and metadata.
Output schema is identical across all ingestors.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class BaseIngestor(ABC):
    """
    Abstract base for all type-specific document ingestors.
    Subclasses implement _extract_text() and _extract_metadata().
    """

    doc_type: str = "unknown"

    def ingest(self, doc: dict) -> dict:
        """
        Main entry point. Receives a raw Probatum document,
        enriches it with extracted text and metadata,
        returns normalized document ready for chunking.
        """
        try:
            doc = self._extract_text(doc)
            doc = self._extract_metadata(doc)
            doc["doc_type"]     = self.doc_type
            doc["ingested_at"]  = datetime.now(timezone.utc).isoformat()
            doc["ingestor"]     = self.__class__.__name__

            if doc.get("raw_text") and len(doc["raw_text"].strip()) > 50:
                doc["parse_status"] = "ok"
            else:
                doc["parse_status"] = "empty"
                doc["error_notes"]  = "No text extracted by ingestor"

            logger.info(
                f"{self.__class__.__name__}: ingested {doc.get('doc_id')} "
                f"— {len(doc.get('raw_text', ''))} chars"
            )

        except Exception as e:
            doc["parse_status"] = "failed"
            doc["error_notes"]  = str(e)
            logger.error(f"{self.__class__.__name__} error on {doc.get('doc_id')}: {e}")

        return doc

    @abstractmethod
    def _extract_text(self, doc: dict) -> dict:
        """Extract and clean full text from the raw document."""
        pass

    @abstractmethod
    def _extract_metadata(self, doc: dict) -> dict:
        """Extract structured metadata fields from the document."""
        pass

    def _strip_html(self, text: str) -> str:
        """Strip HTML tags and normalize whitespace."""
        import re
        if not text:
            return ""
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _clean_text(self, text: str) -> str:
        """General text cleaning — normalize whitespace, remove control chars."""
        import re
        if not text:
            return ""
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()
