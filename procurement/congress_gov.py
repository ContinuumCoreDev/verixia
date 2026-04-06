"""
Verixia — Congress.gov Document Fetcher
Retrieves federal statutes and bill text from the Congress.gov API v3.
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

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

BASE_URL    = _cfg["sources"]["congress_gov"]["base_url"]
RATE_DELAY  = _cfg["sources"]["congress_gov"]["rate_limit_delay"]
API_KEY     = _cfg["sources"]["congress_gov"]["api_key"]
RAW_DIR     = Path(_cfg["storage"]["corpus_raw"]) / "statutes"
FAILED_DIR  = Path(_cfg["storage"]["corpus_failed"])

HEADERS = {"X-API-Key": API_KEY}

MAX_RETRIES   = 3
RETRY_BACKOFF = [2, 5, 10]


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
    """Save raw document JSON to corpus/raw/statutes/."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{doc_id}.json"
    with open(path, "w") as f:
        json.dump(doc, f, indent=2)
    return path


def _save_failed(doc_id: str, reason: str):
    """Log a failed fetch."""
    FAILED_DIR.mkdir(parents=True, exist_ok=True)
    path = FAILED_DIR / f"congress_{doc_id}.json"
    with open(path, "w") as f:
        json.dump({
            "doc_id":    doc_id,
            "source":    "congress_gov",
            "reason":    reason,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }, f, indent=2)


def _fetch_bill_text(congress: int, bill_type: str, bill_number: int) -> str:
    """
    Fetch full text of a bill from the text versions endpoint.
    Returns the best available plain text or empty string.
    """
    url  = f"{BASE_URL}/bill/{congress}/{bill_type}/{bill_number}/text"
    data = _get(url)

    if not data or not data.get("textVersions"):
        return ""

    # Try each text version — prefer HTML or plain text
    for version in data["textVersions"]:
        for fmt in version.get("formats", []):
            fmt_type = fmt.get("type", "").lower()
            if "htm" in fmt_type or "plain" in fmt_type:
                text_url = fmt.get("url", "")
                if text_url:
                    r = requests.get(text_url, timeout=30)
                    if r.status_code == 200:
                        text = r.text
                        # Strip HTML if needed
                        if "<" in text:
                            import re
                            text = re.sub(r"<[^>]+>", " ", text)
                            text = re.sub(r"\s+", " ", text).strip()
                        return text
    return ""


def build_verixia_doc(bill: dict, full_text: str = "") -> dict:
    """
    Normalize a Congress.gov bill into Verixia's document schema.
    """
    congress    = bill.get("congress", "")
    bill_type   = bill.get("type", "").lower()
    bill_number = bill.get("number", "")
    doc_id      = f"cg_{congress}_{bill_type}{bill_number}"

    # Extract latest action date as published date
    latest_action = bill.get("latestAction", {})
    action_date   = latest_action.get("actionDate", "")

    # Build citation string
    citation = f"{bill_type.upper()} {bill_number}, {congress}th Congress"

    return {
        "doc_id":           doc_id,
        "source":           "congress_gov",
        "doc_type":         "statute",
        "title":            bill.get("title", "Unknown"),
        "citation":         citation,
        "congress":         congress,
        "bill_type":        bill_type,
        "bill_number":      bill_number,
        "origin_chamber":   bill.get("originChamber", ""),
        "published_date":   action_date,
        "effective_date":   action_date,
        "ingested_date":    datetime.now(timezone.utc).isoformat(),
        "superseded_date":  None,
        "raw_text":         full_text,
        "source_url":       bill.get("url", ""),
        "raw_path":         None,
        "parse_status":     "ok" if full_text else "empty",
        "error_notes":      "" if full_text else "No text retrieved",
        "cites":            [],
    }


def fetch_statutes_by_query(
    query: str,
    congress: int = None,
    max_results: int = 10
) -> list[dict]:
    """
    Search Congress.gov for bills matching a query.
    Fetches full text for each result.

    Args:
        query       Search string
        congress    Congress number (e.g. 117 for 117th Congress)
        max_results Max documents to return

    Returns:
        List of normalized Verixia documents
    """
    params = {
        "query":  query,
        "limit":  min(max_results, 20),
        "format": "json",
    }
    if congress:
        params["congress"] = congress

    url  = f"{BASE_URL}/bill"
    data = _get(url, params)

    if not data or not data.get("bills"):
        logger.warning(f"No bills found for query: '{query}'")
        return []

    docs = []
    for bill in data["bills"][:max_results]:
        congress_num = bill.get("congress")
        bill_type    = bill.get("type", "").lower()
        bill_number  = bill.get("number")

        if not all([congress_num, bill_type, bill_number]):
            continue

        # Fetch full text
        full_text = _fetch_bill_text(congress_num, bill_type, bill_number)

        doc  = build_verixia_doc(bill, full_text)
        path = _save_raw(bill, doc["doc_id"])
        doc["raw_path"] = str(path)

        if doc["parse_status"] == "ok":
            logger.info(f"Fetched: {doc['title'][:60]} [{doc['doc_id']}]")
        else:
            logger.warning(f"No text: {doc['title'][:60]} [{doc['doc_id']}]")

        docs.append(doc)

    logger.info(f"Query '{query}' returned {len(docs)} statutes.")
    return docs


def fetch_by_usc_citation(usc_citation: str) -> dict | None:
    """
    Attempt to fetch a statute from a U.S.C. citation string.
    e.g. '42 U.S.C. § 1983'
    Used by the citation queue worker.
    """
    # Extract searchable terms from citation
    import re
    clean = re.sub(r"[§\.\s]+", " ", usc_citation).strip()
    docs  = fetch_statutes_by_query(clean, max_results=1)
    return docs[0] if docs else None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Testing Congress.gov fetcher...")

    docs = fetch_statutes_by_query(
        query="civil rights",
        congress=88,
        max_results=2
    )

    if docs:
        for d in docs:
            print(f"\nTitle:    {d['title'][:70]}")
            print(f"Doc ID:   {d['doc_id']}")
            print(f"Citation: {d['citation']}")
            print(f"Date:     {d['published_date']}")
            print(f"Text:     {len(d['raw_text'])} chars")
            print(f"Saved:    {d['raw_path']}")
    else:
        print("No results.")
