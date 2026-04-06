"""
Verixia — Full-Text Resolver
Fetches complete opinion text from CourtListener cluster endpoint.
The search API returns snippets only. This fetches the full document.
Called after initial procurement to enrich documents before chunking.

v4 API notes:
  - Cluster uses 'sub_opinions' (not 'opinions')
  - Best text fields: html_with_citations, html_lawbox, xml_harvard
  - Cluster-level fallbacks: summary, headmatter, headnotes
"""

import re
import json
import time
import logging
import requests
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

BASE_URL      = _cfg["sources"]["courtlistener"]["base_url"]
RATE_DELAY    = _cfg["sources"]["courtlistener"]["rate_limit_delay"]
API_KEY       = _cfg["sources"]["courtlistener"]["api_key"]
RAW_DIR       = Path(_cfg["storage"]["corpus_raw"]) / "case_law"

HEADERS = {
    "Authorization": f"Token {API_KEY}",
    "Content-Type":  "application/json",
}

MAX_RETRIES   = 3
RETRY_BACKOFF = [2, 5, 10]

# Opinion-level fields in preference order
OPINION_TEXT_FIELDS = [
    "html_with_citations",
    "html_lawbox",
    "xml_harvard",
    "html",
    "plain_text",
    "html_columbia",
    "html_anon_2020",
]

# Cluster-level fallback fields
CLUSTER_TEXT_FIELDS = [
    "summary",
    "headmatter",
    "headnotes",
    "syllabus",
]


def _get(url: str) -> dict | None:
    """GET with retry and rate limiting."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
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


def _strip_html(text: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _best_text(data: dict, fields: list) -> str:
    """Return the longest non-empty text from a list of fields."""
    best = ""
    for field in fields:
        val = data.get(field, "") or ""
        if len(val.strip()) > len(best):
            best = val.strip()
    return best


def _fetch_opinion_text(sub_opinion_url: str) -> str:
    """
    Fetch a sub_opinion and return the best available full text.
    Strips HTML if needed.
    """
    data = _get(sub_opinion_url)
    if not data:
        return ""

    text = _best_text(data, OPINION_TEXT_FIELDS)
    if text and "<" in text:
        text = _strip_html(text)
    return text


def resolve_full_text(doc: dict) -> dict:
    """
    Enrich a Verixia document with full opinion text.
    Fetches from the cluster endpoint using the cluster_id.

    Strategy:
      1. Fetch cluster
      2. Try each sub_opinion URL for full opinion text
      3. Fall back to cluster-level summary/headmatter if opinions empty
      4. Update raw file on disk with resolved text

    Args:
        doc     Verixia document dict from courtlistener.py

    Returns:
        Same doc with raw_text replaced by full text.
    """
    cluster_id  = doc["doc_id"].replace("cl_", "")
    cluster_url = f"{BASE_URL}/clusters/{cluster_id}/"

    logger.info(f"Resolving {doc['doc_id']}: {doc['title']}")

    cluster = _get(cluster_url)
    if not cluster:
        doc["parse_status"] = "failed"
        doc["error_notes"]  = f"Cluster fetch failed: {cluster_url}"
        return doc

    full_text = ""

    # ── Step 1: Try sub_opinions (v4 field name) ──────────────
    sub_opinions = cluster.get("sub_opinions", [])
    for opinion_url in sub_opinions:
        if isinstance(opinion_url, dict):
            opinion_url = opinion_url.get("resource_uri", "")
        if not opinion_url:
            continue
        text = _fetch_opinion_text(opinion_url)
        if len(text) > len(full_text):
            full_text = text

    # ── Step 2: Cluster-level fallback ────────────────────────
    if len(full_text) < 500:
        cluster_text = _best_text(cluster, CLUSTER_TEXT_FIELDS)
        if cluster_text and "<" in cluster_text:
            cluster_text = _strip_html(cluster_text)
        if len(cluster_text) > len(full_text):
            full_text = cluster_text
            logger.info(f"{doc['doc_id']}: using cluster-level text")

    # ── Step 3: Update doc ────────────────────────────────────
    if full_text:
        original_len        = len(doc.get("raw_text", ""))
        doc["raw_text"]     = full_text
        doc["parse_status"] = "ok"
        doc["error_notes"]  = ""

        # Update raw file on disk
        raw_path = doc.get("raw_path")
        if raw_path and Path(raw_path).exists():
            with open(raw_path) as f:
                raw_data = json.load(f)
            raw_data["_verixia_full_text"] = full_text
            raw_data["_verixia_resolved"]  = True
            with open(raw_path, "w") as f:
                json.dump(raw_data, f, indent=2)

        logger.info(
            f"{doc['doc_id']}: {original_len} chars "
            f"→ {len(full_text)} chars"
        )
    else:
        doc["parse_status"] = "empty"
        doc["error_notes"]  = "No text found in opinion or cluster fields"
        logger.warning(f"{doc['doc_id']}: no full text available")

    return doc


def resolve_from_opinion_id(cl_opinion_id: int) -> dict | None:
    """
    Resolve a document directly from a CourtListener opinion ID.
    Used by the citation queue worker to fetch cited opinions.

    Returns a fully resolved Verixia document or None.
    """
    from procurement.courtlistener import build_verixia_doc, _save_raw

    opinion_url  = f"{BASE_URL}/opinions/{cl_opinion_id}/"
    opinion_data = _get(opinion_url)

    if not opinion_data:
        logger.warning(f"Opinion {cl_opinion_id} not found.")
        return None

    # Get cluster metadata
    cluster_data = None
    cluster_url  = opinion_data.get("cluster", "")
    if cluster_url:
        cluster_data = _get(cluster_url)

    cluster_id = cluster_data.get("id") if cluster_data else cl_opinion_id

    # Build minimal result dict matching search result format
    result = {
        "cluster_id":   cluster_id,
        "caseName":     cluster_data.get("case_name", f"Opinion {cl_opinion_id}") if cluster_data else f"Opinion {cl_opinion_id}",
        "caseNameFull": cluster_data.get("case_name_full", "") if cluster_data else "",
        "citation":     [c.get("citation", "") for c in cluster_data.get("citations", [])] if cluster_data else [],
        "court":        "",
        "court_id":     "",
        "dateFiled":    cluster_data.get("date_filed") if cluster_data else None,
        "dateArgued":   None,
        "docketNumber": "",
        "absolute_url": f"/opinion/{cluster_id}/",
        "opinions":     [{"id": cl_opinion_id, "cites": [], "snippet": ""}],
    }

    doc = build_verixia_doc(result)

    # Extract full text from opinion
    text = _best_text(opinion_data, OPINION_TEXT_FIELDS)
    if text and "<" in text:
        text = _strip_html(text)

    # Fall back to cluster text fields
    if len(text) < 500 and cluster_data:
        cluster_text = _best_text(cluster_data, CLUSTER_TEXT_FIELDS)
        if cluster_text and "<" in cluster_text:
            cluster_text = _strip_html(cluster_text)
        if len(cluster_text) > len(text):
            text = cluster_text

    if text:
        doc["raw_text"]     = text
        doc["parse_status"] = "ok"
    else:
        doc["parse_status"] = "empty"
        doc["error_notes"]  = "No text in opinion or cluster fields"

    # Preserve cited opinion IDs for graph traversal
    doc["cites"] = opinion_data.get("opinions_cited", [])

    # Save raw to archive
    path = _save_raw(opinion_data, doc["doc_id"])
    doc["raw_path"] = str(path)

    logger.info(
        f"Resolved opinion {cl_opinion_id}: "
        f"{doc['title']} — {len(doc['raw_text'])} chars"
    )
    return doc


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Testing full-text resolver...")

    from procurement.courtlistener import fetch_opinions_by_query

    docs = fetch_opinions_by_query(
        query="Marbury v Madison",
        court="scotus",
        max_results=1
    )

    if not docs:
        print("No documents fetched.")
        exit()

    doc = docs[0]
    print(f"\nBefore resolution:")
    print(f"  Title:  {doc['title']}")
    print(f"  Text:   {len(doc['raw_text'])} chars")
    print(f"  Status: {doc['parse_status']}")

    doc = resolve_full_text(doc)

    print(f"\nAfter resolution:")
    print(f"  Text:   {len(doc['raw_text'])} chars")
    print(f"  Status: {doc['parse_status']}")

    if doc["raw_text"]:
        print(f"\nFirst 500 chars:")
        print(doc["raw_text"][:500])
