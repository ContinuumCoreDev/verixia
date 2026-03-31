"""
Probatum — Daily Pipeline Runner
Orchestrates the full document procurement and ingest cycle.
Run manually or triggered by n8n scheduler.

Pipeline:
  1. Process citation scrape queue (highest priority first)
  2. Fetch new documents from all three sources
  3. Resolve full text for each document
  4. Classify document type
  5. Chunk each document
  6. Ingest chunks into Qdrant
  7. Extract citations and feed back into queue
  8. Write daily journal entry
"""

import logging
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

JOURNAL_DIR = Path(_cfg["storage"]["journal_dir"])

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("probatum.pipeline")


def run_pipeline(
    fetch_queries: list[dict] = None,
    queue_batch_size: int = 20,
    dry_run: bool = False,
) -> dict:
    """
    Run the full daily pipeline.

    Args:
        fetch_queries   List of {source, query, kwargs} dicts for new fetches
                        Defaults to a standard daily set if None
        queue_batch_size  Citations to process from scrape queue per run
        dry_run         If True, fetch and chunk but do not ingest or update queue

    Returns:
        Journal entry dict with run statistics
    """
    from procurement.courtlistener  import fetch_opinions_by_query
    from procurement.congress_gov   import fetch_statutes_by_query
    from procurement.regulations_gov import fetch_regulations_by_query
    from procurement.resolver        import resolve_full_text
    from classifier.classifier       import classify_document
    from chunker.chunker             import chunk_document
    from engine.ingest               import ingest_chunks, collection_stats
    from citation.extractor          import extract_from_doc
    from citation.queue_manager      import (initialize_queue, process_citations,
                                             get_next_batch, mark_fetched,
                                             mark_failed, queue_stats)

    start_time = datetime.now(timezone.utc)
    logger.info(f"=== Probatum Daily Pipeline — {start_time.strftime('%Y-%m-%d')} ===")

    initialize_queue()

    stats = {
        "run_date":         start_time.isoformat(),
        "dry_run":          dry_run,
        "docs_fetched":     0,
        "docs_resolved":    0,
        "docs_failed":      0,
        "chunks_ingested":  0,
        "citations_found":  0,
        "citations_queued": 0,
        "queue_processed":  0,
        "errors":           [],
    }

    # ── Default daily fetch queries ───────────────────────────
    if fetch_queries is None:
        fetch_queries = [
            {
                "source": "courtlistener",
                "query":  "constitutional law",
                "kwargs": {"court": "scotus", "max_results": 5}
            },
            {
                "source": "courtlistener",
                "query":  "civil rights federal",
                "kwargs": {"max_results": 5}
            },
            {
                "source": "congress_gov",
                "query":  "public law",
                "kwargs": {"max_results": 3}
            },
            {
                "source": "regulations_gov",
                "query":  "federal regulation rule",
                "kwargs": {"doc_type": "Rule", "max_results": 3}
            },
        ]

    # ── Step 1: Process citation queue ────────────────────────
    logger.info(f"Processing up to {queue_batch_size} queued citations...")
    queued = get_next_batch(queue_batch_size)

    for cite in queued:
        try:
            doc = None

            if cite["resolution"] == "courtlistener_id" and cite["cl_opinion_id"]:
                from procurement.resolver import resolve_from_opinion_id
                doc = resolve_from_opinion_id(cite["cl_opinion_id"])

            if doc and doc.get("parse_status") == "ok":
                doc["doc_type"] = classify_document(doc)

            # Route through type-specific ingestor
            from ingestors.generic_ingestor import get_ingestor
            ingestor = get_ingestor(doc["doc_type"])
            doc = ingestor.ingest(doc)
                chunks = chunk_document(doc)
                if chunks and not dry_run:
                    ingested = ingest_chunks(chunks)
                    stats["chunks_ingested"] += ingested
                    citations = extract_from_doc(doc)
                    process_citations(citations)
                    stats["citations_found"]  += len(citations)
                    mark_fetched(cite["normalized"], doc["doc_id"])
                stats["queue_processed"] += 1
                stats["docs_resolved"]   += 1
            else:
                if not dry_run:
                    mark_failed(
                        cite["normalized"],
                        "no text or fetch failed"
                    )

        except Exception as e:
            err = f"Queue item {cite['normalized'][:40]}: {e}"
            logger.error(err)
            stats["errors"].append(err)

    # ── Step 2: Fetch new documents ───────────────────────────
    logger.info(f"Fetching from {len(fetch_queries)} query sources...")

    source_map = {
        "courtlistener":   fetch_opinions_by_query,
        "congress_gov":    fetch_statutes_by_query,
        "regulations_gov": fetch_regulations_by_query,
    }

    all_docs = []
    for fq in fetch_queries:
        source  = fq["source"]
        query   = fq["query"]
        kwargs  = fq.get("kwargs", {})
        fetcher = source_map.get(source)

        if not fetcher:
            logger.warning(f"Unknown source: {source}")
            continue

        try:
            docs = fetcher(query, **kwargs)
            all_docs.extend(docs)
            stats["docs_fetched"] += len(docs)
        except Exception as e:
            err = f"Fetch error [{source}] '{query}': {e}"
            logger.error(err)
            stats["errors"].append(err)

    # ── Step 3: Resolve, classify, chunk, ingest ──────────────
    logger.info(f"Processing {len(all_docs)} fetched documents...")

    for doc in all_docs:
        try:
            # Resolve full text if not already present
            if len(doc.get("raw_text", "")) < 500:
                doc = resolve_full_text(doc)

            if doc.get("parse_status") != "ok":
                stats["docs_failed"] += 1
                continue

            stats["docs_resolved"] += 1

            # Classify
            doc["doc_type"] = classify_document(doc)

            # Route through type-specific ingestor
            from ingestors.generic_ingestor import get_ingestor
            ingestor = get_ingestor(doc["doc_type"])
            doc = ingestor.ingest(doc)

            # Chunk
            chunks = chunk_document(doc)
            if not chunks:
                continue

            # Ingest
            if not dry_run:
                ingested = ingest_chunks(chunks)
                stats["chunks_ingested"] += ingested

            # Extract citations
            citations = extract_from_doc(doc)
            stats["citations_found"] += len(citations)

            if not dry_run:
                process_citations(citations)
                stats["citations_queued"] += len([
                    c for c in citations
                    if c.get("resolution") == "courtlistener_id"
                ])

        except Exception as e:
            err = f"Processing error [{doc.get('doc_id', 'unknown')}]: {e}"
            logger.error(err)
            stats["docs_failed"] += 1
            stats["errors"].append(err)

    # ── Step 4: Final stats ───────────────────────────────────
    try:
        col_stats = collection_stats()
        stats["collection_points"] = col_stats.get("points_count", 0)
    except Exception:
        stats["collection_points"] = "unknown"

    try:
        q_stats = queue_stats()
        stats["queue_remaining"] = q_stats.get("queued", 0)
        stats["queue_fetched"]   = q_stats.get("fetched", 0)
        stats["queue_failed"]    = q_stats.get("failed", 0)
    except Exception:
        pass

    elapsed = (datetime.now(timezone.utc) - start_time).seconds
    stats["elapsed_seconds"] = elapsed

    # ── Step 5: Write journal entry ───────────────────────────
    if not dry_run:
        _write_journal(stats)

    logger.info(f"=== Pipeline complete in {elapsed}s ===")
    logger.info(f"  Docs fetched:    {stats['docs_fetched']}")
    logger.info(f"  Docs resolved:   {stats['docs_resolved']}")
    logger.info(f"  Chunks ingested: {stats['chunks_ingested']}")
    logger.info(f"  Citations found: {stats['citations_found']}")
    logger.info(f"  Queue remaining: {stats.get('queue_remaining', '?')}")
    if stats["errors"]:
        logger.warning(f"  Errors: {len(stats['errors'])}")

    return stats


def _write_journal(stats: dict):
    """Write a daily markdown journal entry to the archive."""
    JOURNAL_DIR.mkdir(parents=True, exist_ok=True)
    date_str = stats["run_date"][:10]
    path     = JOURNAL_DIR / f"{date_str}.md"

    lines = [
        f"# Probatum Pipeline Journal — {date_str}",
        f"",
        f"## Run Statistics",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Documents fetched | {stats['docs_fetched']} |",
        f"| Documents resolved | {stats['docs_resolved']} |",
        f"| Documents failed | {stats['docs_failed']} |",
        f"| Chunks ingested | {stats['chunks_ingested']} |",
        f"| Citations found | {stats['citations_found']} |",
        f"| Queue remaining | {stats.get('queue_remaining', '?')} |",
        f"| Queue fetched (total) | {stats.get('queue_fetched', '?')} |",
        f"| Collection points | {stats.get('collection_points', '?')} |",
        f"| Elapsed seconds | {stats.get('elapsed_seconds', '?')} |",
        f"",
    ]

    if stats.get("errors"):
        lines += [
            f"## Errors ({len(stats['errors'])})",
            f"",
        ]
        for err in stats["errors"]:
            lines.append(f"- {err}")
        lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))

    logger.info(f"Journal written: {path}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Probatum daily pipeline")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and chunk but do not ingest")
    parser.add_argument("--queue-only", action="store_true",
                        help="Process queue only, no new fetches")
    args = parser.parse_args()

    fetch_queries = [] if args.queue_only else None

    stats = run_pipeline(
        fetch_queries    = fetch_queries,
        queue_batch_size = 20,
        dry_run          = args.dry_run,
    )

    if stats["errors"]:
        print(f"\nErrors encountered: {len(stats['errors'])}")
        for e in stats["errors"]:
            print(f"  {e}")
