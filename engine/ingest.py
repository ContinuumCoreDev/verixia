"""
Verixia — Qdrant Ingest Layer
Takes chunked documents and upserts them into the
Qdrant vector store with full metadata payloads.
Handles collection creation, embedding, and upsert.
The temporal constraint layer lives here —
every chunk carries its published_date and published_ts
for date-filtered verification queries.
"""

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    PayloadSchemaType,
    Filter,
    FieldCondition,
    MatchValue,
    Range,
)
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

import os
QDRANT_HOST     = os.environ.get("QDRANT_HOST", _cfg["qdrant"]["host"])
QDRANT_PORT     = int(os.environ.get("QDRANT_PORT", _cfg["qdrant"]["port"]))
QDRANT_API_KEY  = os.environ.get("QDRANT_API_KEY", None)
COLLECTION      = _cfg["qdrant"]["collection"]
EMBEDDING_MODEL = _cfg["embedding"]["model"]
EMBEDDING_DEV   = _cfg["embedding"]["device"]

VECTOR_SIZE = 384  # all-MiniLM-L6-v2 output dimension

_client = None
_model  = None


def _get_client() -> QdrantClient:
    global _client
    if _client is None:
        if QDRANT_API_KEY:
            _client = QdrantClient(
                host    = QDRANT_HOST,
                port    = QDRANT_PORT,
                api_key = QDRANT_API_KEY,
                https   = True,
                timeout = 30,
            )
            logger.info(f"Qdrant connected (cloud): {QDRANT_HOST}")
        else:
            _client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, timeout=30)
            logger.info(f"Qdrant connected (local): {QDRANT_HOST}:{QDRANT_PORT}")
    return _client


def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBEDDING_MODEL, device=EMBEDDING_DEV)
        logger.info(f"Embedding model loaded: {EMBEDDING_MODEL}")
    return _model


def _date_to_ts(date_str: Optional[str]) -> Optional[float]:
    """
    Convert ISO date string to Unix timestamp for Qdrant numeric range filter.
    Qdrant Range requires numeric values — dates stored as float timestamps.
    Returns None if date_str is None or unparseable.
    """
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(str(date_str).strip(), fmt)
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    return None


def ensure_collection():
    """
    Create the Verixia collection if it doesn't exist.
    Sets up vector config and payload indexes for
    efficient filtered retrieval.
    """
    client   = _get_client()
    existing = [c.name for c in client.get_collections().collections]

    if COLLECTION in existing:
        logger.info(f"Collection '{COLLECTION}' already exists.")
        return

    client.create_collection(
        collection_name = COLLECTION,
        vectors_config  = VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
    )

    for field, schema in [
        ("published_date", PayloadSchemaType.KEYWORD),
        ("published_ts",   PayloadSchemaType.FLOAT),
        ("doc_type",       PayloadSchemaType.KEYWORD),
        ("source",         PayloadSchemaType.KEYWORD),
        ("break_type",     PayloadSchemaType.KEYWORD),
        ("doc_id",         PayloadSchemaType.KEYWORD),
    ]:
        client.create_payload_index(
            collection_name = COLLECTION,
            field_name      = field,
            field_schema    = schema,
        )

    logger.info(f"Collection '{COLLECTION}' created with payload indexes.")


def ingest_chunks(chunks: list, batch_size: int = 64) -> int:
    """
    Embed and upsert a list of Chunk objects into Qdrant.

    Args:
        chunks      List of Chunk dataclass objects from chunker.py
        batch_size  Number of chunks per batch

    Returns:
        Number of chunks successfully upserted
    """
    if not chunks:
        logger.warning("ingest_chunks called with empty list.")
        return 0

    ensure_collection()
    client = _get_client()
    model  = _get_model()
    total  = 0

    for i in range(0, len(chunks), batch_size):
        batch      = chunks[i : i + batch_size]
        texts      = [c.text for c in batch]
        embeddings = model.encode(
            texts,
            batch_size        = batch_size,
            show_progress_bar = False,
            convert_to_numpy  = True,
        )

        points = []
        for chunk, embedding in zip(batch, embeddings):
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk.chunk_id))
            payload  = {
                "chunk_id":       chunk.chunk_id,
                "doc_id":         chunk.doc_id,
                "text":           chunk.text,
                "token_count":    chunk.token_count,
                "position":       chunk.position,
                "break_type":     chunk.break_type,
                "break_marker":   chunk.break_marker,
                "similarity":     chunk.similarity,
                "source":         chunk.source,
                "doc_type":       chunk.doc_type,
                "published_date": chunk.published_date,
                "published_ts":   _date_to_ts(chunk.published_date),
                "section_label":  chunk.section_label,
                "chunk_role":     getattr(chunk, "chunk_role", "UNKNOWN"),
            }
            points.append(PointStruct(
                id      = point_id,
                vector  = embedding.tolist(),
                payload = payload,
            ))

        client.upsert(collection_name=COLLECTION, points=points, wait=True)
        total += len(batch)
        logger.info(f"Upserted batch {i // batch_size + 1}: {len(batch)} chunks ({total}/{len(chunks)})")

    logger.info(f"Ingest complete: {total} chunks from {chunks[0].doc_id}")
    return total


def collection_stats() -> dict:
    """Return current collection statistics."""
    client = _get_client()
    try:
        info = client.get_collection(COLLECTION)
        return {
            "points_count": info.points_count,
            "status":       str(info.status),
        }
    except Exception as e:
        return {"error": str(e)}


def search(
    query: str,
    top_k: int = 10,
    doc_type: Optional[str] = None,
    as_of_date: Optional[str] = None,
) -> list[dict]:
    """
    Semantic search across the knowledge graph.
    Temporal constraint via as_of_date enforces ex post facto prohibition —
    only returns chunks from documents published on or before that date.

    Args:
        query       Claim or query text
        top_k       Number of results
        doc_type    Filter by document type
        as_of_date  ISO date string — only sources on or before this date

    Returns:
        List of payload dicts for matching chunks
    """
    client       = _get_client()
    model        = _get_model()
    query_vector = model.encode(query, convert_to_numpy=True).tolist()

    conditions = []

    if doc_type:
        conditions.append(
            FieldCondition(key="doc_type", match=MatchValue(value=doc_type))
        )

    if as_of_date:
        ts = _date_to_ts(as_of_date)
        if ts is not None:
            conditions.append(
                FieldCondition(
                    key   = "published_ts",
                    range = Range(lte=ts)
                )
            )

    query_filter = Filter(must=conditions) if conditions else None

    results = client.query_points(
        collection_name = COLLECTION,
        query           = query_vector,
        limit           = top_k,
        query_filter    = query_filter,
        with_payload    = True,
    )

    return [
        {"score": round(r.score, 4), "payload": r.payload}
        for r in results.points
    ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    print("Testing Qdrant ingest...")

    from procurement.courtlistener import fetch_opinions_by_query
    from procurement.resolver      import resolve_full_text
    from chunker.chunker           import chunk_document

    # Verify date converter
    print(f"Date converter test:")
    print(f"  1803-02-24 → {_date_to_ts('1803-02-24')}")
    print(f"  1800-01-01 → {_date_to_ts('1800-01-01')}")
    print(f"  None       → {_date_to_ts(None)}")

    # Drop and recreate collection
    client = _get_client()
    if COLLECTION in [c.name for c in client.get_collections().collections]:
        client.delete_collection(COLLECTION)
        print(f"\nDropped '{COLLECTION}'.")

    ensure_collection()
    print(f"Collection recreated.")

    # Fetch, resolve, chunk, ingest
    docs   = fetch_opinions_by_query("Marbury v Madison", court="scotus", max_results=1)
    doc    = resolve_full_text(docs[0])
    chunks = chunk_document(doc)
    ingest_chunks(chunks)
    print(f"Ingested {len(chunks)} chunks.")

    # Verify payload
    r = client.query_points(
        collection_name = COLLECTION,
        query           = [0.0] * VECTOR_SIZE,
        limit           = 1,
        with_payload    = True,
    )
    if r.points:
        p = r.points[0].payload
        print(f"\nPayload check:")
        print(f"  published_date: {p.get('published_date')}")
        print(f"  published_ts:   {p.get('published_ts')}")

    # Temporal tests
    print("\n--- Temporal constraint tests ---")

    r1 = search("judicial review", top_k=3, as_of_date="1800-01-01")
    print(f"\nBefore Marbury (1800-01-01): {len(r1)} results (expected 0)")

    r2 = search("judicial review", top_k=3, as_of_date="1803-12-31")
    print(f"After Marbury  (1803-12-31): {len(r2)} results (expected 3)")

    r3 = search("judicial review power of courts", top_k=3)
    print(f"No date filter:              {len(r3)} results (expected 3)")
    for r in r3:
        print(f"  [{r['score']}] {r['payload']['break_type']} | {r['payload']['text'][:120]}...")
