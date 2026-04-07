"""
Verixia — Citation Extractor
Extracts citations from ingested documents and
feeds new ones into the scrape queue.
Handles both text-based extraction and
CourtListener's pre-parsed cites[] array.
"""

import logging
from datetime import datetime, timezone

from citation.patterns import extract_citations

logger = logging.getLogger(__name__)


def extract_from_doc(doc: dict) -> list[dict]:
    """
    Extract all citations from a Verixia document.
    Uses pre-parsed cites[] if available (CourtListener),
    falls back to regex extraction on raw_text.

    Returns list of citation dicts ready for queue_manager.
    """
    citations = []

    # ── Path 1: CourtListener pre-parsed cites array ──────────
    # These are opinion IDs — convert to fetchable references
    cl_cites = doc.get("cites", [])
    if cl_cites:
        for cite_id in cl_cites:
            # Strip full URLs down to numeric ID
            # e.g. https://www.courtlistener.com/api/rest/v4/opinions/9420759/
            import re
            if isinstance(cite_id, str) and "courtlistener.com" in cite_id:
                match = re.search(r"/opinions/(\d+)/", cite_id)
                cite_id = int(match.group(1)) if match else None
            if not cite_id:
                continue
            citations.append({
                "raw":            f"cl_opinion_{cite_id}",
                "normalized":     f"CL_OPINION_{cite_id}",
                "citation_type":  "case_law",
                "source_doc_id":  doc["doc_id"],
                "resolution":     "courtlistener_id",
                "cl_opinion_id":  cite_id,
            })
        logger.debug(
            f"{doc['doc_id']}: {len(cl_cites)} citations "
            f"from CourtListener cites array"
        )

    # ── Path 2: Regex extraction from raw text ────────────────
    raw_text = doc.get("raw_text", "")
    if raw_text:
        text_citations = extract_citations(raw_text, doc.get("doc_type"))
        for c in text_citations:
            c["source_doc_id"] = doc["doc_id"]
            c["resolution"]    = "regex"
            c["cl_opinion_id"] = None
            citations.append(c)
        logger.debug(
            f"{doc['doc_id']}: {len(text_citations)} citations "
            f"from text extraction"
        )

    logger.info(
        f"{doc['doc_id']}: {len(citations)} total citations extracted"
    )
    return citations
