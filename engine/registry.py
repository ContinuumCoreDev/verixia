"""
Verixia — Verification Registry
Persistent store of every claim ever scored.
Tracks score history, graph versions, and re-scoring triggers.
This is what makes Verixia defensible in due diligence —
every claim has a dated, versioned verification record.
"""

import sqlite3
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

REGISTRY_DB = _cfg["storage"]["registry_db"]


@dataclass
class ScoreRecord:
    score:              float
    confidence:         str
    graph_version:      str
    scored_at:          str
    chunks_evaluated:   int
    supporting_count:   int
    contradicting_count:int
    top_citation:       Optional[str]  # doc_id of best supporting chunk


@dataclass
class RegistryEntry:
    claim_id:           str
    claim_text:         str
    domain:             str
    first_scored:       str
    last_scored:        str
    current_score:      float
    current_confidence: str
    score_history:      list[ScoreRecord]
    source_system:      str
    as_of_date:         Optional[str]


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(REGISTRY_DB)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_registry():
    """Create registry tables if they don't exist."""
    Path(REGISTRY_DB).parent.mkdir(parents=True, exist_ok=True)
    with _conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS claims (
                claim_id            TEXT PRIMARY KEY,
                claim_text          TEXT NOT NULL,
                domain              TEXT NOT NULL DEFAULT '',
                source_system       TEXT NOT NULL DEFAULT 'verixia',
                as_of_date          TEXT,
                first_scored        TEXT NOT NULL,
                last_scored         TEXT NOT NULL,
                current_score       REAL NOT NULL,
                current_confidence  TEXT NOT NULL,
                score_history       TEXT NOT NULL DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS score_events (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                claim_id            TEXT NOT NULL,
                score               REAL NOT NULL,
                confidence          TEXT NOT NULL,
                graph_version       TEXT NOT NULL,
                scored_at           TEXT NOT NULL,
                chunks_evaluated    INTEGER DEFAULT 0,
                supporting_count    INTEGER DEFAULT 0,
                contradicting_count INTEGER DEFAULT 0,
                top_citation        TEXT,
                FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
            );

            CREATE INDEX IF NOT EXISTS idx_claims_domain
                ON claims(domain);
            CREATE INDEX IF NOT EXISTS idx_claims_confidence
                ON claims(current_confidence);
            CREATE INDEX IF NOT EXISTS idx_events_claim
                ON score_events(claim_id);
        """)
    logger.info("Verification registry initialized.")


def _claim_id(claim_text: str) -> str:
    """Generate a deterministic claim ID from claim text."""
    import hashlib
    return hashlib.sha256(claim_text.strip().lower().encode()).hexdigest()[:16]


def _graph_version() -> str:
    """
    Generate a graph version string based on current timestamp.
    In production this would reflect the actual graph state —
    document count, last ingest date, etc.
    """
    from engine.ingest import collection_stats
    try:
        stats = collection_stats()
        points = stats.get("points_count", 0)
        now    = datetime.now(timezone.utc).strftime("%Y%m%d")
        return f"v{now}.{points}"
    except Exception:
        return datetime.now(timezone.utc).strftime("v%Y%m%d.0")


def record_verification(
    result,  # VerificationResult from confidence.py
    source_system: str = "verixia",
) -> str:
    """
    Store a verification result in the registry.
    Creates a new entry or updates existing with score history.

    Args:
        result          VerificationResult from confidence.py
        source_system   Which system submitted this claim

    Returns:
        claim_id string
    """
    now       = datetime.now(timezone.utc).isoformat()
    claim_id  = _claim_id(result.claim)
    graph_ver = _graph_version()
    top_cit   = result.citations[0].doc_id if result.citations else None

    score_event = {
        "score":               result.score,
        "confidence":          result.confidence,
        "graph_version":       graph_ver,
        "scored_at":           now,
        "chunks_evaluated":    result.chunks_evaluated,
        "supporting_count":    result.supporting_count,
        "contradicting_count": result.contradicting_count,
        "top_citation":        top_cit,
    }

    with _conn() as conn:
        existing = conn.execute(
            "SELECT claim_id, score_history FROM claims WHERE claim_id = ?",
            (claim_id,)
        ).fetchone()

        if existing:
            # Update existing entry
            history = json.loads(existing["score_history"])
            history.append(score_event)

            conn.execute("""
                UPDATE claims
                SET last_scored       = ?,
                    current_score     = ?,
                    current_confidence= ?,
                    score_history     = ?
                WHERE claim_id = ?
            """, (
                now,
                result.score,
                result.confidence,
                json.dumps(history),
                claim_id,
            ))

            logger.info(
                f"Updated claim {claim_id[:8]}... "
                f"score: {result.score} ({result.confidence}) "
                f"history: {len(history)} entries"
            )

        else:
            # New claim
            conn.execute("""
                INSERT INTO claims
                    (claim_id, claim_text, domain, source_system,
                     as_of_date, first_scored, last_scored,
                     current_score, current_confidence, score_history)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                claim_id,
                result.claim,
                result.domain or "",
                source_system,
                result.as_of_date,
                now,
                now,
                result.score,
                result.confidence,
                json.dumps([score_event]),
            ))

            logger.info(
                f"New claim {claim_id[:8]}... "
                f"score: {result.score} ({result.confidence})"
            )

        # Always insert the score event
        conn.execute("""
            INSERT INTO score_events
                (claim_id, score, confidence, graph_version,
                 scored_at, chunks_evaluated, supporting_count,
                 contradicting_count, top_citation)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            claim_id,
            result.score,
            result.confidence,
            graph_ver,
            now,
            result.chunks_evaluated,
            result.supporting_count,
            result.contradicting_count,
            top_cit,
        ))

    return claim_id


def get_claim(claim_text: str) -> Optional[dict]:
    """
    Retrieve a claim's full registry entry by text.
    Returns None if not found.
    """
    claim_id = _claim_id(claim_text)
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM claims WHERE claim_id = ?",
            (claim_id,)
        ).fetchone()

    if not row:
        return None

    entry = dict(row)
    entry["score_history"] = json.loads(entry["score_history"])
    return entry


def get_claim_by_id(claim_id: str) -> Optional[dict]:
    """Retrieve a claim by its ID."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM claims WHERE claim_id = ?",
            (claim_id,)
        ).fetchone()

    if not row:
        return None

    entry = dict(row)
    entry["score_history"] = json.loads(entry["score_history"])
    return entry


def registry_stats() -> dict:
    """Return registry statistics."""
    with _conn() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM claims"
        ).fetchone()[0]

        by_confidence = {}
        for conf in ("HIGH", "MEDIUM", "LOW", "UNVERIFIABLE"):
            count = conn.execute(
                "SELECT COUNT(*) FROM claims WHERE current_confidence = ?",
                (conf,)
            ).fetchone()[0]
            by_confidence[conf] = count

        total_events = conn.execute(
            "SELECT COUNT(*) FROM score_events"
        ).fetchone()[0]

    return {
        "total_claims":   total,
        "total_events":   total_events,
        "by_confidence":  by_confidence,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Testing verification registry...\n")

    initialize_registry()

    # Import and run a full verification to get a result
    import os, warnings
    os.environ["TRANSFORMERS_VERBOSITY"] = "error"
    warnings.filterwarnings("ignore")
    import transformers
    transformers.logging.set_verbosity_error()

    from engine.confidence import verify

    test_claims = [
        "The Supreme Court established the principle of judicial review in Marbury v. Madison.",
        "The Supreme Court held that Congress has unlimited power to expand its own jurisdiction.",
        "The weather in Washington DC is typically cold in February.",
    ]

    print("Running verifications and storing results...\n")
    claim_ids = []

    for claim in test_claims:
        result   = verify(claim, top_k=10)
        claim_id = record_verification(result)
        claim_ids.append(claim_id)
        print(f"Stored: {claim_id[:8]}... "
              f"score={result.score} "
              f"confidence={result.confidence}")

    print("\nRetrieving stored claims...\n")
    for i, (claim, cid) in enumerate(zip(test_claims, claim_ids)):
        entry = get_claim_by_id(cid)
        if entry:
            history = entry["score_history"]
            print(f"Claim {i+1}: {claim[:60]}...")
            print(f"  ID:         {entry['claim_id'][:8]}...")
            print(f"  Score:      {entry['current_score']}")
            print(f"  Confidence: {entry['current_confidence']}")
            print(f"  First:      {entry['first_scored'][:19]}")
            print(f"  History:    {len(history)} entries")
            print(f"  Graph ver:  {history[-1]['graph_version']}")
            print()

    # Re-score one claim to test history accumulation
    print("Re-scoring first claim to test history accumulation...")
    result2  = verify(test_claims[0], top_k=10)
    claim_id = record_verification(result2)
    entry    = get_claim_by_id(claim_id)
    print(f"History entries after re-score: {len(entry['score_history'])}")

    print("\nRegistry stats:")
    stats = registry_stats()
    print(f"  Total claims:  {stats['total_claims']}")
    print(f"  Total events:  {stats['total_events']}")
    print(f"  By confidence: {stats['by_confidence']}")
