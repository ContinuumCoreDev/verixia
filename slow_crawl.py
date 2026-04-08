"""
Verixia — Slow Overnight Crawl
Conservative rate limiting to avoid alarming source hosts.
Designed to run unattended for 8-12 hours.
"""
import time
import logging
import signal
import sys
from datetime import datetime

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s %(levelname)s — %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("verixia.crawl")

# Graceful shutdown on Ctrl+C
def shutdown(sig, frame):
    logger.info("Crawl interrupted — shutting down cleanly.")
    sys.exit(0)
signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

from citation.queue_manager import initialize_queue, queue_stats, get_next_batch, mark_fetched, mark_failed
from procurement.resolver import resolve_from_opinion_id
from classifier.classifier import classify_document
from chunker.chunker import chunk_document
from engine.ingest import ingest_chunks, collection_stats
from citation.extractor import extract_from_doc
from citation.queue_manager import process_citations
from ingestors.generic_ingestor import get_ingestor

initialize_queue()

BATCH_SIZE      = 3     # fetch 3 citations at a time
DELAY_BETWEEN   = 8     # seconds between each fetch
BATCH_PAUSE     = 45    # seconds between batches
MAX_CONSECUTIVE_FAIL = 5

logger.info("Starting slow crawl...")
session_ingested = 0
session_fetched  = 0
consecutive_fail = 0
run = 0

while True:
    run += 1
    q = queue_stats()
    remaining = q.get("queued", 0)
    col = collection_stats()
    points = col.get("points_count", 0)

    logger.info(f"Run {run} — Queue: {remaining} | Graph: {points} points | Session: {session_ingested} chunks ingested")

    if remaining == 0:
        logger.info("Queue empty — sleeping 10 minutes then checking again.")
        time.sleep(600)
        continue

    batch = get_next_batch(BATCH_SIZE)

    for cite in batch:
        try:
            # Skip regex citations — no direct resolution path
            if cite["resolution"] != "courtlistener_id":
                mark_failed(cite["normalized"], "regex citation — no resolution path")
                continue
            doc = None
            if cite["resolution"] == "courtlistener_id" and cite["cl_opinion_id"]:
                time.sleep(DELAY_BETWEEN)
                doc = resolve_from_opinion_id(cite["cl_opinion_id"])

            if doc and doc.get("parse_status") == "ok":
                doc["doc_type"] = classify_document(doc)
                ingestor = get_ingestor(doc["doc_type"])
                doc = ingestor.ingest(doc)
                chunks = chunk_document(doc)

                if chunks:
                    ingested = ingest_chunks(chunks)
                    session_ingested += ingested
                    citations = extract_from_doc(doc)
                    process_citations(citations)
                    mark_fetched(cite["normalized"], doc["doc_id"])
                    session_fetched += 1
                    consecutive_fail = 0
                    logger.info(f"  ✓ {doc.get('doc_id')} — {ingested} chunks, {len(citations)} citations extracted")
                else:
                    mark_failed(cite["normalized"], "no chunks produced")
            else:
                mark_failed(cite["normalized"], "no text retrieved")
                consecutive_fail += 1
                logger.warning(f"  ✗ Failed: {cite['normalized'][:50]}")

        except Exception as e:
            consecutive_fail += 1
            logger.error(f"  Error: {cite['normalized'][:50]}: {e}")
            mark_failed(cite["normalized"], str(e))

        if consecutive_fail >= MAX_CONSECUTIVE_FAIL:
            logger.warning(f"  {MAX_CONSECUTIVE_FAIL} consecutive failures — pausing 5 minutes.")
            time.sleep(300)
            consecutive_fail = 0

    logger.info(f"  Batch complete — sleeping {BATCH_PAUSE}s before next batch.")
    time.sleep(BATCH_PAUSE)
