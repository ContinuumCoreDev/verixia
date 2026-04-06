"""
Verixia — Scrape Queue Manager
Manages the citation scrape queue in SQLite.
Handles deduplication, priority scoring,
cross-reference logging, and status tracking.
"""

import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

QUEUE_DB = _cfg["storage"]["scrape_queue_db"]


def _conn() -> sqlite3.Connection:
    """Return a database connection with row factory."""
    conn = sqlite3.connect(QUEUE_DB)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_queue():
    """Create queue and cross-reference tables if they don't exist."""
    Path(QUEUE_DB).parent.mkdir(parents=True, exist_ok=True)
    with _conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS scrape_queue (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                normalized      TEXT    UNIQUE NOT NULL,
                raw             TEXT    NOT NULL,
                citation_type   TEXT    NOT NULL,
                resolution      TEXT    NOT NULL DEFAULT 'regex',
                cl_opinion_id   INTEGER,
                priority        INTEGER NOT NULL DEFAULT 1,
                status          TEXT    NOT NULL DEFAULT 'queued',
                discovered_at   TEXT    NOT NULL,
                fetched_at      TEXT,
                resolved_doc_id TEXT,
                error_notes     TEXT
            );

            CREATE TABLE IF NOT EXISTS cross_references (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                normalized      TEXT    NOT NULL,
                source_doc_id   TEXT    NOT NULL,
                discovered_at   TEXT    NOT NULL,
                UNIQUE(normalized, source_doc_id)
            );

            CREATE INDEX IF NOT EXISTS idx_queue_status
                ON scrape_queue(status);
            CREATE INDEX IF NOT EXISTS idx_queue_priority
                ON scrape_queue(priority DESC);
            CREATE INDEX IF NOT EXISTS idx_xref_normalized
                ON cross_references(normalized);
        """)
    logger.info("Queue database initialized.")


def process_citations(citations: list[dict]):
    """
    Process a list of extracted citations.
    - New citations → added to queue at priority 1
    - Existing queued citations → priority bumped
    - Already resolved citations → cross-reference logged only
    - Deduplication enforced via normalized key

    Args:
        citations   List of citation dicts from extractor.py
    """
    now = datetime.now(timezone.utc).isoformat()
    added = bumped = skipped = 0

    with _conn() as conn:
        for cite in citations:
            normalized    = cite["normalized"]
            source_doc_id = cite.get("source_doc_id", "unknown")

            # Always log the cross-reference
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO cross_references
                        (normalized, source_doc_id, discovered_at)
                    VALUES (?, ?, ?)
                """, (normalized, source_doc_id, now))
            except sqlite3.Error as e:
                logger.error(f"Cross-reference insert error: {e}")

            # Check queue status
            existing = conn.execute("""
                SELECT id, status, priority
                FROM scrape_queue
                WHERE normalized = ?
            """, (normalized,)).fetchone()

            if existing:
                if existing["status"] == "queued":
                    # Bump priority — more docs cite this, more important
                    conn.execute("""
                        UPDATE scrape_queue
                        SET priority = priority + 1
                        WHERE normalized = ?
                    """, (normalized,))
                    bumped += 1
                else:
                    skipped += 1
            else:
                # New citation — add to queue
                try:
                    conn.execute("""
                        INSERT INTO scrape_queue
                            (normalized, raw, citation_type, resolution,
                             cl_opinion_id, priority, status, discovered_at)
                        VALUES (?, ?, ?, ?, ?, 1, 'queued', ?)
                    """, (
                        normalized,
                        cite["raw"],
                        cite["citation_type"],
                        cite.get("resolution", "regex"),
                        cite.get("cl_opinion_id"),
                        now
                    ))
                    added += 1
                except sqlite3.IntegrityError:
                    pass  # Race condition — already inserted

    logger.info(
        f"Citations processed — "
        f"added: {added}, priority bumped: {bumped}, "
        f"already resolved: {skipped}"
    )


def get_next_batch(batch_size: int = 10) -> list[dict]:
    """
    Get the next batch of queued citations, highest priority first.
    Returns list of row dicts ready for procurement workers.
    """
    with _conn() as conn:
        rows = conn.execute("""
            SELECT * FROM scrape_queue
            WHERE status = 'queued'
            ORDER BY priority DESC, discovered_at ASC
            LIMIT ?
        """, (batch_size,)).fetchall()
    return [dict(row) for row in rows]


def mark_fetched(normalized: str, resolved_doc_id: str):
    """Mark a citation as successfully fetched and resolved."""
    with _conn() as conn:
        conn.execute("""
            UPDATE scrape_queue
            SET status          = 'fetched',
                fetched_at      = ?,
                resolved_doc_id = ?
            WHERE normalized = ?
        """, (datetime.now(timezone.utc).isoformat(), resolved_doc_id, normalized))


def mark_failed(normalized: str, reason: str):
    """Mark a citation fetch as failed with a reason."""
    with _conn() as conn:
        conn.execute("""
            UPDATE scrape_queue
            SET status      = 'failed',
                fetched_at  = ?,
                error_notes = ?
            WHERE normalized = ?
        """, (datetime.now(timezone.utc).isoformat(), reason, normalized))


def queue_stats() -> dict:
    """Return current queue statistics."""
    with _conn() as conn:
        stats = {}
        for status in ("queued", "fetched", "failed"):
            count = conn.execute("""
                SELECT COUNT(*) FROM scrape_queue WHERE status = ?
            """, (status,)).fetchone()[0]
            stats[status] = count

        top = conn.execute("""
            SELECT raw, priority FROM scrape_queue
            WHERE status = 'queued'
            ORDER BY priority DESC
            LIMIT 5
        """).fetchall()
        stats["top_priority"] = [dict(r) for r in top]

    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    initialize_queue()
    print("Queue initialized.")
    print(f"Stats: {queue_stats()}")
