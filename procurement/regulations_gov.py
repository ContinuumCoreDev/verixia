"""
Verixia — regulations.gov Document Fetcher
Retrieves federal regulatory filings from the regulations.gov API v4.
Covers final rules, proposed rules, notices, and supporting documents.
"""

import re
import json
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

BASE_URL    = _cfg["sources"]["regulations_gov"]["base_url"]
RATE_DELAY  = _cfg["sources"]["regulations_gov"]["rate_limit_delay"]
API_KEY     = _cfg["sources"]["regulations_gov"]["api_key"]
RAW_DIR     = Path(_cfg["storage"]["corpus_raw"]) / "regulations"
FAILED_DIR  = Path(_cfg["storage"]["corpus_failed"])

HEADERS = {
    "X-Api-Key":   API_KEY,
    "Content-Type": "application/json",
}

MAX_RETRIES   = 3
RETRY_BACKOFF = [2, 5, 10]

# Document types to prioritize
PRIORITY_TYPES = {"Rule", "Proposed Rule", "Notice"}


def _get(url: str, params: dict = None) -> dict | None:
    """GET with retry and rate limiting."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS,
                             params=params, timeout=30)
            if r.status_code == 200:
                time.sleep(RATE_DELAY)
                return r.json()
            elif r.status_code == 429:
                wait = int(r.headers.get("Retry-After", RETRY_BACKOFF[attempt]))
                logger.warning(f"Rate limited. Waiting {wait}s.")
                time.sleep(wait)
            elif r.status_code == 404:
                logger.warning(f"Not found: {url}")
                return None
            else:
                logger.error(f"HTTP {r.status_code} attempt {attempt + 1}: {url}")
                time.sleep(RETRY_BACKOFF[attempt])
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout attempt {attempt + 1}: {url}")
            time.sleep(RETRY_BACKOFF[attempt])
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {e}")
            time.sleep(RETRY_BACKOFF[attempt])
    logger.error(f"All retries failed: {url}")
    return None


def _save_raw(doc: dict, doc_id: str) -> Path:
    """Save raw document JSON to corpus/raw/regulations/."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{doc_id}.json"
    with open(path, "w") as f:
        json.dump(doc, f, indent=2)
    return path


def _save_failed(doc_id: str, reason: str):
    """Log a failed fetch."""
    FAILED_DIR.mkdir(parents=True, exist_ok=True)
    path = FAILED_DIR / f"regulations_{doc_id}.json"
    with open(path, "w") as f:
        json.dump({
            "doc_id":    doc_id,
            "source":    "regulations_gov",
            "reason":    reason,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }, f, indent=2)


def _fetch_full_text(document_id: str) -> str:
    """
    Fetch full text content for a document.
    Tries the document detail endpoint for extracted text,
    then falls back to the abstract/summary.
    """
    url  = f"{BASE_URL}/documents/{document_id}"
    data = _get(url)

    if not data:
        return ""

    attrs = data.get("data", {}).get("attributes", {})

    # Try full text fields in preference order
    for field in ["fullTextXml", "fileFormats"]:
        val = attrs.get(field, "")
        if val and isinstance(val, str) and len(val.strip()) > 100:
            if "<" in val:
                val = re.sub(r"<[^>]+>", " ", val)
                val = re.sub(r"\s+", " ", val).strip()
            return val

    # Fall back to abstract/summary
    abstract = attrs.get("abstract", "") or ""
    if abstract and len(abstract.strip()) > 50:
        return abstract.strip()

    return ""


def build_verixia_doc(item: dict, full_text: str = "") -> dict:
    """
    Normalize a regulations.gov document into Verixia's document schema.
    """
    attrs     = item.get("attributes", {})
    doc_id_raw = item.get("id", "unknown")
    doc_id    = f"rg_{doc_id_raw.replace('-', '_').lower()}"

    posted_date = attrs.get("postedDate", "")
    if posted_date:
        posted_date = posted_date[:10]  # trim to YYYY-MM-DD

    # Build citation from CFR references if available
    cfr_refs = attrs.get("cfrPart", []) or []
    if cfr_refs:
        citation = f"{cfr_refs[0]} C.F.R."
    else:
        citation = attrs.get("docketId", doc_id_raw)

    return {
        "doc_id":           doc_id,
        "source":           "regulations_gov",
        "doc_type":         "regulation",
        "title":            attrs.get("title", "Unknown"),
        "citation":         citation,
        "docket_id":        attrs.get("docketId", ""),
        "document_type":    attrs.get("documentType", ""),
        "agency":           attrs.get("agencyId", ""),
        "published_date":   posted_date,
        "effective_date":   posted_date,
        "ingested_date":    datetime.now(timezone.utc).isoformat(),
        "superseded_date":  None,
        "raw_text":         full_text,
        "source_url":       f"https://www.regulations.gov/document/{doc_id_raw}",
        "raw_path":         None,
        "parse_status":     "ok" if full_text else "empty",
        "error_notes":      "" if full_text else "No text retrieved",
        "cites":            [],
    }


def fetch_regulations_by_query(
    query: str,
    agency: str = None,
    doc_type: str = "Rule",
    max_results: int = 10
) -> list[dict]:
    """
    Search regulations.gov for documents matching a query.

    Args:
        query       Search string
        agency      Agency ID filter e.g. 'EPA', 'FCC', 'FDA'
        doc_type    Document type: 'Rule', 'Proposed Rule', 'Notice'
        max_results Max documents to return

    Returns:
        List of normalized Verixia documents
    """
    params = {
        "filter[searchTerm]": query,
        "filter[documentType]": doc_type,
        "page[size]": max(min(max_results, 25), 5),
        "sort": "postedDate",
    }
    if agency:
        params["filter[agencyId]"] = agency

    url  = f"{BASE_URL}/documents"
    data = _get(url, params)

    if not data or not data.get("data"):
        logger.warning(f"No documents found for query: '{query}'")
        return []

    docs = []
    for item in data["data"][:max_results]:
        doc_id_raw = item.get("id", "")
        if not doc_id_raw:
            continue

        full_text = _fetch_full_text(doc_id_raw)
        doc       = build_verixia_doc(item, full_text)
        path      = _save_raw(item, doc["doc_id"])
        doc["raw_path"] = str(path)

        if doc["parse_status"] == "ok":
            logger.info(
                f"Fetched: {doc['title'][:60]} "
                f"[{doc['doc_id']}] — {len(doc['raw_text'])} chars"
            )
        else:
            logger.warning(f"No text: {doc['title'][:60]} [{doc['doc_id']}]")

        docs.append(doc)

    logger.info(
        f"Query '{query}' ({doc_type}) returned {len(docs)} regulations."
    )
    return docs


def fetch_from_cfr_citation(cfr_citation: str) -> dict | None:
    """
    Attempt to fetch a regulation from a CFR citation string.
    e.g. '47 C.F.R. § 73.3555'
    Used by the citation queue worker.
    """
    clean = re.sub(r"[§\.\s]+", " ", cfr_citation).strip()
    docs  = fetch_regulations_by_query(clean, max_results=1)
    return docs[0] if docs else None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Testing regulations.gov fetcher...")

    docs = fetch_regulations_by_query(
        query    = "clean air",
        agency   = "EPA",
        doc_type = "Rule",
        max_results = 2
    )

    if docs:
        for d in docs:
            print(f"\nTitle:   {d['title'][:70]}")
            print(f"Doc ID:  {d['doc_id']}")
            print(f"Agency:  {d['agency']}")
            print(f"Date:    {d['published_date']}")
            print(f"Text:    {len(d['raw_text'])} chars")
            print(f"Saved:   {d['raw_path']}")
    else:
        print("No results — check API key in config.")
