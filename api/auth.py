"""
Verixia — API Key Authentication
Simple but production-grade key management.
Keys are stored as SHA-256 hashes — never in plaintext.
Each key has a tier, rate limit, and audit trail.
"""

import hashlib
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Optional

import yaml
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

logger = logging.getLogger(__name__)

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

AUTH_DB   = str(Path(_cfg_path).parent.parent / "data" / "auth.db")
API_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# Tiers
TIER_TRIAL      = "trial"       # 100 requests/day
TIER_STANDARD   = "standard"    # 10,000 requests/day
TIER_ENTERPRISE = "enterprise"  # unlimited

TIER_LIMITS = {
    TIER_TRIAL:      100,
    TIER_STANDARD:   10_000,
    TIER_ENTERPRISE: None,  # unlimited
}


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(AUTH_DB)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_auth():
    """Create auth database and tables."""
    Path(AUTH_DB).parent.mkdir(parents=True, exist_ok=True)
    with _conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                key_hash        TEXT    UNIQUE NOT NULL,
                key_prefix      TEXT    NOT NULL,
                customer        TEXT    NOT NULL,
                tier            TEXT    NOT NULL DEFAULT 'trial',
                created_at      TEXT    NOT NULL,
                expires_at      TEXT,
                is_active       INTEGER NOT NULL DEFAULT 1,
                daily_limit     INTEGER,
                notes           TEXT
            );

            CREATE TABLE IF NOT EXISTS usage_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                key_prefix      TEXT    NOT NULL,
                endpoint        TEXT    NOT NULL,
                timestamp       TEXT    NOT NULL,
                response_ms     INTEGER,
                confidence      TEXT,
                status_code     INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_keys_hash
                ON api_keys(key_hash);
            CREATE INDEX IF NOT EXISTS idx_usage_prefix
                ON usage_log(key_prefix);
            CREATE INDEX IF NOT EXISTS idx_usage_timestamp
                ON usage_log(timestamp);
        """)
    logger.info("Auth database initialized.")


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def create_api_key(
    customer: str,
    tier: str = TIER_TRIAL,
    expires_at: Optional[str] = None,
    notes: str = "",
) -> str:
    """
    Generate a new API key for a customer.
    Returns the plaintext key — store it securely, it won't be shown again.
    """
    import secrets
    key    = f"vx_{secrets.token_urlsafe(32)}"
    prefix = key[:12]

    with _conn() as conn:
        conn.execute("""
            INSERT INTO api_keys
                (key_hash, key_prefix, customer, tier, created_at,
                 expires_at, is_active, daily_limit, notes)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
        """, (
            _hash_key(key),
            prefix,
            customer,
            tier,
            datetime.now(timezone.utc).isoformat(),
            expires_at,
            TIER_LIMITS.get(tier),
            notes,
        ))

    logger.info(f"API key created for {customer} — tier: {tier}")
    return key


def validate_key(key: str) -> Optional[dict]:
    """Validate an API key and return its metadata."""
    key_hash = _hash_key(key)

    with _conn() as conn:
        row = conn.execute("""
            SELECT * FROM api_keys
            WHERE key_hash = ? AND is_active = 1
        """, (key_hash,)).fetchone()

    if not row:
        return None

    # Check expiry
    if row["expires_at"]:
        expires = datetime.fromisoformat(row["expires_at"])
        if datetime.now(timezone.utc) > expires:
            return None

    return dict(row)


def check_rate_limit(key_prefix: str, daily_limit: Optional[int]) -> bool:
    """Check if key is within daily rate limit."""
    if daily_limit is None:
        return True

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with _conn() as conn:
        count = conn.execute("""
            SELECT COUNT(*) FROM usage_log
            WHERE key_prefix = ?
            AND timestamp LIKE ?
        """, (key_prefix, f"{today}%")).fetchone()[0]

    return count < daily_limit


def log_request(
    key_prefix: str,
    endpoint: str,
    response_ms: int,
    confidence: str = "",
    status_code: int = 200,
):
    """Log an API request for billing and analytics."""
    with _conn() as conn:
        conn.execute("""
            INSERT INTO usage_log
                (key_prefix, endpoint, timestamp, response_ms,
                 confidence, status_code)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            key_prefix,
            endpoint,
            datetime.now(timezone.utc).isoformat(),
            response_ms,
            confidence,
            status_code,
        ))


async def require_api_key(api_key: str = Security(API_HEADER)) -> dict:
    """
    FastAPI dependency — validates API key on every request.
    Raises 401 if missing, 403 if invalid or rate limited.
    """
    # Allow bypass in development mode
    dev_mode = os.environ.get("VERIXIA_DEV_MODE", "").lower() == "true"
    if dev_mode:
        return {
            "customer":    "dev",
            "tier":        TIER_ENTERPRISE,
            "key_prefix":  "dev_00000000",
            "daily_limit": None,
        }

    if not api_key:
        raise HTTPException(
            status_code = status.HTTP_401_UNAUTHORIZED,
            detail      = "API key required. Include X-API-Key header.",
        )

    key_data = validate_key(api_key)
    if not key_data:
        raise HTTPException(
            status_code = status.HTTP_403_FORBIDDEN,
            detail      = "Invalid or expired API key.",
        )

    if not check_rate_limit(key_data["key_prefix"], key_data["daily_limit"]):
        raise HTTPException(
            status_code = status.HTTP_429_TOO_MANY_REQUESTS,
            detail      = f"Daily rate limit exceeded for tier {key_data['tier']}.",
        )

    return key_data
