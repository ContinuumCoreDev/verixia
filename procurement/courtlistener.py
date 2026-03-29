"""
Probatum — CourtListener Document Fetcher
Retrieves federal case law from the CourtListener REST API v4.
Handles rate limiting, retries, pagination, and raw storage.
"""

import json
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# Load config
_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

BASE_URL        = _cfg["sources"]["courtlistener"]["base_url"]
RATE_DELAY      = _cfg["sources"]["courtlistener"]["rate_limit_delay"]
API_KEY         = _cfg["sources"]["courtlistener"]["api_key"]
RAW_DIR         = Path(_cfg["storage"]["corpus_raw"]) / "case_law"
FAILED_DIR      = Path(_cfg["storage"]["corpus_failed"])

HEADERS = {
    "Authorization": f"Token {API_KEY}",
    "Content-Type":  "application/json",
}

MAX_RETRIES  = 3
RETRY_BACKOFF = [2, 5, 10]


def _get(url: str, params: dict = None) -> dict | None:
    """GET with retry logic and rate limiting."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                url,
                headers=HEADERS,
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                time.sleep(RATE_DELAY)
                return response.json()

            elif response.status_code == 429:
                wait = int(response.headers.get("Retry-After", RETRY_BACKOFF[attempt]))
                logger.warning(f"Rate limited. Waiting {wait}s.")
                time.sleep(wait)

            elif response.status_code == 404:
                logger.warning(f"Not found: {url}")
                return None

            else:
                logger.error(f"HTTP {response.status_code} attempt {attempt + 1}: {url}")
                time.sleep(RETRY_BACKOFF[attempt])

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout attempt {attempt + 1}: {url}")
            time.sleep(RETRY_BACKOFF[attempt])

        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error attempt {attempt + 1}: {e}")
            time.sleep(RETRY_BACKOFF[attempt])

    logger.error(f"All {MAX_RETRIES} attempts failed: {url}")
    return None


def _save_raw(doc: dict, doc_id: str) -> Path:
    """Save raw document JSON to corpus/raw/case_law/."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{doc_id}.json"
    with open(path, "w") as f:
        json.dump(doc, f, indent=2)
    return path


def _save_failed(doc_id: str, reason: str):
    """Log a failed fetch for later review."""
    FAILED_DIR.mkdir(parents=True, exist_ok=True)
    path = FAILED_DIR / f"courtlistener_{doc_id}.json"
    with open(path, "w") as f:
        json.dump({
            "doc_id":    doc_id,
            "source":    "courtlistener",
            "reason":    reason,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }, f, indent=2)


def build_probatum_doc(result: dict) -> dict:
    """
    Normalize a CourtListener v4 search result into Probatum's document schema.
    v4 returns opinion text inline in the search result — no second fetch needed.
    """
    # Extract opinion text from embedded opinions array
    opinion_text = ""
    opinion_ids  = []
    cites        = []

    for opinion in result.get("opinions", []):
        opinion_ids.append(opinion.get("id"))
        opinion_text += opinion.get("snippet", "") or ""
        cites.extend(opinion.get("cites", []))

    # Use cluster_id as our primary identifier
    cluster_id = result.get("cluster_id", "unknown")
    doc_id     = f"cl_{cluster_id}"

    # Extract citation — v4 returns a list
    citations = result.get("citation", [])
    primary_citation = citations[0] if citations else None

    return {
        "doc_id":           doc_id,
        "source":           "courtlistener",
        "doc_type":         "case_law",
        "title":            result.get("caseName") or result.get("caseNameFull", "Unknown"),
        "citation":         primary_citation,
        "all_citations":    citations,
        "court":            result.get("court", "unknown"),
        "court_id":         result.get("court_id", "unknown"),
        "date_filed":       result.get("dateFiled"),
        "date_argued":      result.get("dateArgued"),
        "published_date":   result.get("dateFiled"),
        "effective_date":   result.get("dateFiled"),
        "ingested_date":    datetime.now(timezone.utc).isoformat(),
        "superseded_date":  None,
        "docket_number":    result.get("docketNumber"),
        "opinion_ids":      opinion_ids,
        "cites":            cites,          # raw citation IDs for graph traversal
        "raw_text":         opinion_text.strip(),
        "source_url":       f"https://www.courtlistener.com{result.get('absolute_url', '')}",
        "raw_path":         None,
        "parse_status":     "ok" if opinion_text.strip() else "empty",
        "error_notes":      "" if opinion_text.strip() else "No opinion text in result",
    }


def fetch_opinions_by_query(
    query: str,
    court: str = None,
    date_min: str = None,
    date_max: str = None,
    max_results: int = 20
) -> list[dict]:
    """
    Search CourtListener opinions by keyword query.
    Returns list of normalized Probatum documents.

    Args:
        query       Search string
        court       Court ID filter e.g. 'scotus', 'ca9', 'dcd'
        date_min    ISO date string e.g. '1900-01-01'
        date_max    ISO date string e.g. '2024-12-31'
        max_results Max documents to return
    """
    params = {
        "q":         query,
        "type":      "o",
        "page_size": min(max_results, 20),
    }
    if court:
        params["court"] = court
    if date_min:
        params["filed_after"] = date_min
    if date_max:
        params["filed_before"] = date_max

    url  = f"{BASE_URL}/search/"
    docs = []

    while url and len(docs) < max_results:
        result = _get(url, params if "cursor" not in url else None)
        if not result:
            break

        for item in result.get("results", []):
            if len(docs) >= max_results:
                break

            doc = build_probatum_doc(item)
            path = _save_raw(item, doc["doc_id"])
            doc["raw_path"] = str(path)

            docs.append(doc)
            logger.info(f"Fetched: {doc['title']} [{doc['doc_id']}]")

        # Pagination — v4 uses cursor-based
        url    = result.get("next")
        params = None

    logger.info(f"Query '{query}' returned {len(docs)} documents.")
    return docs


def fetch_from_citation(citation_raw: str) -> dict | None:
    """
    Attempt to fetch a document from a raw legal citation string.
    Used by the citation queue to resolve discovered references.
    e.g. '347 U.S. 483' — fetches Brown v. Board of Education
    """
    params = {
        "q":         citation_raw,
        "type":      "o",
        "page_size": 1,
    }

    result = _get(f"{BASE_URL}/search/", params)

    if not result or not result.get("results"):
        logger.warning(f"Citation not found: {citation_raw}")
        _save_failed(
            citation_raw.replace(" ", "_"),
            f"citation_not_found: {citation_raw}"
        )
        return None

    doc  = build_probatum_doc(result["results"][0])
    path = _save_raw(result["results"][0], doc["doc_id"])
    doc["raw_path"] = str(path)
    return doc


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Testing CourtListener fetcher...")

    docs = fetch_opinions_by_query(
        query="Marbury v Madison",
        court="scotus",
        max_results=3
    )

    if docs:
        for d in docs:
            print(f"\nTitle:    {d['title']}")
            print(f"Doc ID:   {d['doc_id']}")
            print(f"Citation: {d['citation']}")
            print(f"Court:    {d['court']}")
            print(f"Filed:    {d['date_filed']}")
            print(f"Saved:    {d['raw_path']}")
            print(f"Text:     {len(d['raw_text'])} chars")
            print(f"Cites:    {len(d['cites'])} references found")
    else:
        print("No results returned.")
