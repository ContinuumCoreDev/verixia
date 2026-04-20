"""
Verixia — Re-ingest from Qdrant Cloud + Founding Documents
Extracts documents from cloud Qdrant, reconstructs full text,
re-chunks with updated role classification, and ingests to
local verixia_legal collection.
Includes founding documents from saved corpus.
"""

import json
import logging
import time
import sys
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
FOUNDING_DIR = Path("/mnt/kayla_archive/verixia/corpus/founding")

from qdrant_client import QdrantClient
from engine.ingest import (
    ensure_collection, ingest_chunks,
    collection_stats, COLLECTION, _get_client
)
from chunker.chunker import chunk_document
from citation.extractor import extract_from_doc
from citation.queue_manager import initialize_queue, process_citations


def progress_bar(current, total, label="", width=50):
    pct     = current / total if total > 0 else 0
    filled  = int(width * pct)
    bar     = "█" * filled + "░" * (width - filled)
    sys.stdout.write(f"\r  [{bar}] {current}/{total} {pct:.0%} {label}")
    sys.stdout.flush()
    if current >= total:
        sys.stdout.write("\n")
        sys.stdout.flush()


def scroll_all_points(cloud: QdrantClient) -> list:
    all_points = []
    offset     = None

    logger.info("Scrolling all points from Qdrant Cloud...")
    cloud_count = cloud.get_collection("verixia_legal").points_count

    while True:
        result = cloud.scroll(
            collection_name = "verixia_legal",
            limit           = 200,
            offset          = offset,
            with_vectors    = False,
            with_payload    = True,
        )
        points, next_offset = result
        if not points:
            break

        all_points.extend(points)
        progress_bar(len(all_points), cloud_count, "points scrolled")
        offset = next_offset

        if next_offset is None:
            break

    print()
    return all_points


def reconstruct_documents(points: list) -> list[dict]:
    doc_chunks = defaultdict(list)
    for p in points:
        payload = p.payload
        doc_id  = payload.get("doc_id")
        if doc_id:
            doc_chunks[doc_id].append(payload)

    docs = []
    for doc_id, chunks in doc_chunks.items():
        chunks.sort(key=lambda c: c.get("position", 0))
        first     = chunks[0]
        full_text = " ".join(
            c.get("text", "") for c in chunks
            if c.get("text", "").strip()
        )
        if len(full_text.strip()) < 100:
            continue
        docs.append({
            "doc_id":         doc_id,
            "doc_type":       first.get("doc_type", "unknown"),
            "source":         first.get("source", "unknown"),
            "raw_text":       full_text,
            "published_date": first.get("published_date"),
            "parse_status":   "ok",
            "title":          doc_id,
            "cites":          [],
        })

    return docs


def load_founding_docs() -> list[dict]:
    """Load founding documents from saved JSON."""
    founding_json = FOUNDING_DIR / "founding_docs.json"
    if not founding_json.exists():
        logger.warning("Founding docs JSON not found — skipping.")
        return []

    with open(founding_json) as f:
        docs = json.load(f)

    logger.info(f"Loaded {len(docs)} founding documents.")
    return docs


def reingest_all():
    initialize_queue()

    # Connect to cloud
    cloud = QdrantClient(
        url     = f"https://{CLOUD_URL}",
        api_key = CLOUD_KEY,
        timeout = 60,
    )

    # Drop and recreate local collection
    local    = _get_client()
    existing = [c.name for c in local.get_collections().collections]
    if COLLECTION in existing:
        local.delete_collection(COLLECTION)
        logger.info(f"Dropped '{COLLECTION}' — starting clean.")

    ensure_collection()
    logger.info(f"Collection '{COLLECTION}' recreated.")

    # Scroll all points from cloud
    points = scroll_all_points(cloud)
    logger.info(f"Retrieved {len(points)} points from cloud.")

    # Reconstruct documents
    logger.info("Reconstructing documents from chunks...")
    cloud_docs = reconstruct_documents(points)
    logger.info(f"Reconstructed {len(cloud_docs)} documents from cloud.")

    # Load founding documents
    founding_docs = load_founding_docs()

    # Combine — founding docs first so they anchor the collection
    all_docs   = founding_docs + cloud_docs
    total_docs = len(all_docs)
    logger.info(f"Total documents to re-ingest: {total_docs} "
                f"({len(founding_docs)} founding + {len(cloud_docs)} case law)")

    # Re-chunk with updated role classification and ingest
    ingested_docs   = 0
    ingested_chunks = 0
    failed_docs     = 0
    citations_found = 0
    role_counts     = {}

    logger.info(f"Re-ingesting with updated role classification...")
    print()

    for i, doc in enumerate(all_docs):
        progress_bar(i + 1, total_docs,
                     f"| {ingested_chunks} chunks | {doc.get('doc_id','?')[:20]}")
        try:
            chunks = chunk_document(doc)
            if not chunks:
                failed_docs += 1
                continue

            # Track role distribution
            for c in chunks:
                role_counts[c.chunk_role] = role_counts.get(c.chunk_role, 0) + 1

            ingested = ingest_chunks(chunks)
            ingested_chunks += ingested
            ingested_docs   += 1

            citations = extract_from_doc(doc)
            process_citations(citations)
            citations_found += len(citations)

        except Exception as e:
            failed_docs += 1
            logger.error(f"Failed {doc.get('doc_id','?')}: {e}")

    print()

    # Final stats
    final = collection_stats()
    logger.info(f"")
    logger.info(f"═══ Re-ingest Complete ═══")
    logger.info(f"  Documents processed: {ingested_docs}")
    logger.info(f"  Documents failed:    {failed_docs}")
    logger.info(f"  Chunks ingested:     {ingested_chunks}")
    logger.info(f"  Citations extracted: {citations_found}")
    logger.info(f"  Collection points:   {final.get('points_count', 0)}")
    logger.info(f"")
    logger.info(f"  Role distribution:")
    for role, count in sorted(role_counts.items(), key=lambda x: -x[1]):
        pct = count / ingested_chunks * 100 if ingested_chunks > 0 else 0
        logger.info(f"    {role:<25} {count:>6} ({pct:.1f}%)")

    return final.get("points_count", 0)


if __name__ == "__main__":
    logger.info("Starting Verixia re-ingest with updated role patterns...")
    logger.info("Founding documents will be ingested first.")
    start = time.time()
    points = reingest_all()
    elapsed = (time.time() - start) / 60
    logger.info(f"Total time: {elapsed:.1f} minutes")
    logger.info(f"Final collection: {points} points in {COLLECTION}")
