"""
Verixia — Re-ingest from Qdrant Cloud
Extracts documents from cloud Qdrant, reconstructs full text
by joining chunks in position order, re-chunks with role
classification, and ingests to local verixia_legal collection.

No CourtListener calls. No rate limiting. Clean re-ingest.
"""

import logging
import time
from collections import defaultdict
from pathlib import Path

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s %(levelname)s — %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("verixia.reingest")

import yaml
with open("config/config.yaml") as f:
    cfg = yaml.safe_load(f)

CLOUD_URL = "d98b1c4b-cb98-4006-90aa-064f43a6c2dc.us-east-1-1.aws.cloud.qdrant.io"
CLOUD_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.9skpxOtq3x8-VwDTEZgooecvuwwY9q5qQHILSHCFMnM"

from qdrant_client import QdrantClient
from engine.ingest import (
    ensure_collection, ingest_chunks,
    collection_stats, COLLECTION, _get_client
)
from chunker.chunker import chunk_document
from citation.extractor import extract_from_doc
from citation.queue_manager import initialize_queue, process_citations


def scroll_all_points(cloud: QdrantClient) -> list:
    """Scroll all points from cloud collection."""
    all_points = []
    offset     = None
    batch      = 0

    while True:
        result = cloud.scroll(
            collection_name = "verixia_legal",
            limit           = 100,
            offset          = offset,
            with_vectors    = False,
            with_payload    = True,
        )
        points, next_offset = result
        if not points:
            break

        all_points.extend(points)
        offset = next_offset
        batch += 1

        if batch % 10 == 0:
            logger.info(f"  Scrolled {len(all_points)} points...")

        if next_offset is None:
            break

    return all_points


def reconstruct_documents(points: list) -> list[dict]:
    """
    Group chunks by doc_id and reconstruct documents
    by joining text in position order.
    """
    # Group by doc_id
    doc_chunks = defaultdict(list)
    for p in points:
        payload = p.payload
        doc_id  = payload.get("doc_id")
        if doc_id:
            doc_chunks[doc_id].append(payload)

    # Reconstruct each document
    docs = []
    for doc_id, chunks in doc_chunks.items():
        # Sort by position
        chunks.sort(key=lambda c: c.get("position", 0))

        # Take metadata from first chunk
        first = chunks[0]

        # Join text in order — use space between chunks
        full_text = " ".join(
            c.get("text", "") for c in chunks
            if c.get("text", "").strip()
        )

        if len(full_text.strip()) < 100:
            continue

        doc = {
            "doc_id":         doc_id,
            "doc_type":       first.get("doc_type", "unknown"),
            "source":         first.get("source", "unknown"),
            "raw_text":       full_text,
            "published_date": first.get("published_date"),
            "parse_status":   "ok",
            "title":          doc_id,
            "cites":          [],
        }
        docs.append(doc)

    return docs


def reingest_from_cloud(batch_report_interval: int = 25):
    """
    Main re-ingest pipeline.
    """
    initialize_queue()

    # Connect to cloud
    cloud = QdrantClient(
        url     = f"https://{CLOUD_URL}",
        api_key = CLOUD_KEY,
        timeout = 60,
    )

    cloud_count = cloud.get_collection("verixia_legal").points_count
    logger.info(f"Cloud collection: {cloud_count} points")

    # Drop and recreate local collection
    local = _get_client()
    existing = [c.name for c in local.get_collections().collections]
    if COLLECTION in existing:
        local.delete_collection(COLLECTION)
        logger.info(f"Dropped local '{COLLECTION}'.")

    ensure_collection()
    logger.info(f"Local '{COLLECTION}' recreated.")

    # Scroll all points from cloud
    logger.info("Scrolling all points from cloud...")
    points = scroll_all_points(cloud)
    logger.info(f"Retrieved {len(points)} points from cloud.")

    # Reconstruct documents
    logger.info("Reconstructing documents from chunks...")
    docs = reconstruct_documents(points)
    logger.info(f"Reconstructed {len(docs)} documents.")

    # Re-chunk with role classification and ingest
    ingested_docs   = 0
    ingested_chunks = 0
    failed_docs     = 0
    citations_found = 0

    for i, doc in enumerate(docs):
        try:
            chunks = chunk_document(doc)

            if not chunks:
                failed_docs += 1
                continue

            # Verify roles are being assigned
            roles = set(c.chunk_role for c in chunks)
            if roles == {"UNKNOWN"}:
                logger.debug(f"{doc['doc_id']}: all chunks UNKNOWN role")

            ingested = ingest_chunks(chunks)
            ingested_chunks += ingested
            ingested_docs   += 1

            citations = extract_from_doc(doc)
            process_citations(citations)
            citations_found += len(citations)

            if (i + 1) % batch_report_interval == 0:
                stats = collection_stats()
                logger.info(
                    f"Progress: {i+1}/{len(docs)} docs — "
                    f"{ingested_chunks} chunks — "
                    f"{stats.get('points_count', 0)} points"
                )

        except Exception as e:
            failed_docs += 1
            logger.error(f"Failed {doc.get('doc_id', 'unknown')}: {e}")

    final = collection_stats()
    logger.info(f"Re-ingest complete.")
    logger.info(f"  Docs processed: {ingested_docs}")
    logger.info(f"  Docs failed:    {failed_docs}")
    logger.info(f"  Chunks:         {ingested_chunks}")
    logger.info(f"  Citations:      {citations_found}")
    logger.info(f"  Points:         {final.get('points_count', 0)}")

    return final.get("points_count", 0)


if __name__ == "__main__":
    logger.info("Starting re-ingest from Qdrant Cloud...")
    points = reingest_from_cloud()
    logger.info(f"Final: {points} points in local verixia_legal")
