"""
Verixia — Re-ingest from saved raw corpus
Re-normalizes, re-chunks and re-ingests all saved documents
with chunk_role tagging. Does not re-download anything.
"""

import json
import logging
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

RAW_DIR = Path(cfg["storage"]["corpus_raw"])

from engine.ingest import (
    ensure_collection, ingest_chunks,
    collection_stats, COLLECTION, _get_client
)
from procurement.courtlistener  import build_verixia_doc as build_cl_doc
from procurement.congress_gov   import build_verixia_doc as build_cg_doc
from procurement.regulations_gov import build_verixia_doc as build_rg_doc
from procurement.resolver       import resolve_full_text
from classifier.classifier      import classify_document
from chunker.chunker            import chunk_document
from citation.extractor         import extract_from_doc
from citation.queue_manager     import initialize_queue, process_citations


def reingest_all(batch_report_interval: int = 50):
    initialize_queue()

    # Drop and recreate collection with correct name
    client   = _get_client()
    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION in existing:
        client.delete_collection(COLLECTION)
        logger.info(f"Dropped collection '{COLLECTION}'.")

    ensure_collection()
    logger.info(f"Collection '{COLLECTION}' recreated.")

    # Find all raw JSON files by subdirectory
    case_law_files   = list((RAW_DIR / "case_law").glob("*.json"))
    statute_files    = list((RAW_DIR / "statutes").glob("*.json"))
    regulation_files = list((RAW_DIR / "regulations").glob("*.json"))

    logger.info(f"Files found:")
    logger.info(f"  Case law:    {len(case_law_files)}")
    logger.info(f"  Statutes:    {len(statute_files)}")
    logger.info(f"  Regulations: {len(regulation_files)}")

    all_files = (
        [(p, "case_law")   for p in case_law_files] +
        [(p, "statute")    for p in statute_files] +
        [(p, "regulation") for p in regulation_files]
    )

    total_files     = len(all_files)
    ingested_docs   = 0
    ingested_chunks = 0
    failed_docs     = 0
    citations_found = 0

    for i, (path, source_type) in enumerate(all_files):
        try:
            with open(path) as f:
                raw = json.load(f)

            # Normalize using the appropriate builder
            if source_type == "case_law":
                doc = build_cl_doc(raw)
            elif source_type == "statute":
                doc = build_cg_doc(raw, "")
            else:
                doc = build_rg_doc(raw, "")

            # Skip if no doc_id
            if not doc.get("doc_id"):
                failed_docs += 1
                continue

            # Resolve full text if short
            if len(doc.get("raw_text", "")) < 500:
                doc = resolve_full_text(doc)

            if doc.get("parse_status") != "ok":
                failed_docs += 1
                continue

            doc["doc_type"] = classify_document(doc)
            chunks = chunk_document(doc)

            if not chunks:
                failed_docs += 1
                continue

            ingested = ingest_chunks(chunks)
            ingested_chunks += ingested
            ingested_docs   += 1

            citations = extract_from_doc(doc)
            process_citations(citations)
            citations_found += len(citations)

            if (i + 1) % batch_report_interval == 0:
                stats = collection_stats()
                logger.info(
                    f"Progress: {i+1}/{total_files} — "
                    f"{ingested_docs} docs — "
                    f"{ingested_chunks} chunks — "
                    f"{stats.get('points_count', 0)} points"
                )

        except Exception as e:
            failed_docs += 1
            logger.error(f"Failed {path.name}: {e}")

    final = collection_stats()
    logger.info(f"Re-ingest complete.")
    logger.info(f"  Docs processed: {ingested_docs}")
    logger.info(f"  Docs failed:    {failed_docs}")
    logger.info(f"  Chunks:         {ingested_chunks}")
    logger.info(f"  Points:         {final.get('points_count', 0)}")

    return final.get("points_count", 0)


if __name__ == "__main__":
    logger.info("Starting re-ingest...")
    points = reingest_all()
    logger.info(f"Final collection size: {points} points")
