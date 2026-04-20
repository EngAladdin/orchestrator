"""
pool_manager.py — Container pool manager for the Orchestrator.

Pools
-----
level1 : 5 containers  (low-interaction, default)
level2 : 3 containers  (medium-interaction)
level3 : 1 container   (high-interaction)

State is persisted to SQLite so the Orchestrator survives restarts.

Assignment algorithm
--------------------
1. If session already has a container, return it (idempotent).
2. Among containers at the requested level, prefer IDLE nodes.
3. Fallback: BUSY containers at the same level (over-subscription).
4. Containers are released via explicit /release or TTL expiry.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

logger = logging.getLogger(__name__)

DB_PATH: str = os.environ.get("ORCHESTRATOR_DB_PATH", "/data/orchestrator.db")
SESSION_TTL_SECONDS: int = int(os.environ.get("SESSION_TTL_SECONDS", str(3600 * 4)))  # 4 hours


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


@contextmanager
def _cursor() -> Generator[sqlite3.Cursor, None, None]:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


DDL = """
CREATE TABLE IF NOT EXISTS containers (
    id          TEXT PRIMARY KEY,
    level       INTEGER NOT NULL,
    host        TEXT NOT NULL,
    port        INTEGER NOT NULL,
    state       TEXT NOT NULL DEFAULT 'idle',   -- idle | busy | unhealthy
    session_id  TEXT,
    assigned_at REAL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id      TEXT PRIMARY KEY,
    container_id    TEXT,
    level           INTEGER NOT NULL DEFAULT 1,
    assigned_at     REAL,
    released_at     REAL,
    released        INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (container_id) REFERENCES containers(id)
);

CREATE INDEX IF NOT EXISTS idx_containers_level ON containers(level, state);
CREATE INDEX IF NOT EXISTS idx_sessions_active  ON sessions(released);
"""


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
    conn = _get_conn()
    try:
        conn.executescript(DDL)
        conn.commit()
    finally:
        conn.close()
    logger.info("Orchestrator DB initialised at %s", DB_PATH)


# ---------------------------------------------------------------------------
# Pool seeding (called at startup)
# ---------------------------------------------------------------------------

_DEFAULT_POOL: List[Dict[str, Any]] = [
    # level1 — 5 containers
    {"id": "l1c1", "level": 1, "host": "honeytrap-l1-1", "port": 8080},
    {"id": "l1c2", "level": 1, "host": "honeytrap-l1-2", "port": 8080},
    {"id": "l1c3", "level": 1, "host": "honeytrap-l1-3", "port": 8080},
    {"id": "l1c4", "level": 1, "host": "honeytrap-l1-4", "port": 8080},
    {"id": "l1c5", "level": 1, "host": "honeytrap-l1-5", "port": 8080},
    # level2 — 3 containers
    {"id": "l2c1", "level": 2, "host": "honeytrap-l2-1", "port": 8080},
    {"id": "l2c2", "level": 2, "host": "honeytrap-l2-2", "port": 8080},
    {"id": "l2c3", "level": 2, "host": "honeytrap-l2-3", "port": 8080},
    # level3 — 1 container
    {"id": "l3c1", "level": 3, "host": "honeytrap-l3-1", "port": 8080},
]


def seed_pool() -> None:
    """Insert default containers if not already present."""
    with _cursor() as cur:
        for c in _DEFAULT_POOL:
            cur.execute(
                "INSERT OR IGNORE INTO containers (id, level, host, port, state) VALUES (?,?,?,?,?)",
                (c["id"], c["level"], c["host"], c["port"], "idle"),
            )
    logger.info("Pool seeded (%d containers)", len(_DEFAULT_POOL))


# ---------------------------------------------------------------------------
# Assignment
# ---------------------------------------------------------------------------

def assign(session_id: str, level: int) -> Dict[str, Any]:
    """
    Assign a container to a session at the given level.
    Returns the container dict. Raises RuntimeError if no container is available.
    """
    with _cursor() as cur:
        # Check if session already assigned
        cur.execute(
            "SELECT sessions.*, containers.host, containers.port FROM sessions "
            "JOIN containers ON sessions.container_id = containers.id "
            "WHERE sessions.session_id = ? AND sessions.released = 0",
            (session_id,),
        )
        existing = cur.fetchone()
        if existing:
            logger.debug("Session %s already assigned to %s", session_id, existing["container_id"])
            return dict(existing)

        # Prefer idle at requested level
        cur.execute(
            "SELECT * FROM containers WHERE level = ? AND state = 'idle' LIMIT 1",
            (level,),
        )
        container = cur.fetchone()

        # Fallback: any container at the level
        if not container:
            cur.execute(
                "SELECT * FROM containers WHERE level = ? AND state != 'unhealthy' LIMIT 1",
                (level,),
            )
            container = cur.fetchone()

        if not container:
            raise RuntimeError(f"No available container at level {level}")

        now = time.time()
        # Mark container busy
        cur.execute(
            "UPDATE containers SET state='busy', session_id=?, assigned_at=? WHERE id=?",
            (session_id, now, container["id"]),
        )
        # Upsert session record
        cur.execute(
            """
            INSERT INTO sessions (session_id, container_id, level, assigned_at)
            VALUES (?,?,?,?)
            ON CONFLICT(session_id) DO UPDATE SET
              container_id=excluded.container_id,
              level=excluded.level,
              assigned_at=excluded.assigned_at,
              released=0,
              released_at=NULL
            """,
            (session_id, container["id"], level, now),
        )
        logger.info("Assigned session %s → container %s (level %d)", session_id, container["id"], level)
        return {
            **dict(container),
            "upstream": f"{container['host']}:{container['port']}",
        }


def release(session_id: str) -> bool:
    """Release a session and mark its container idle. Returns True if found."""
    with _cursor() as cur:
        cur.execute(
            "SELECT container_id FROM sessions WHERE session_id = ? AND released = 0",
            (session_id,),
        )
        row = cur.fetchone()
        if not row:
            return False
        container_id = row["container_id"]
        cur.execute(
            "UPDATE sessions SET released=1, released_at=? WHERE session_id=?",
            (time.time(), session_id),
        )
        cur.execute(
            "UPDATE containers SET state='idle', session_id=NULL, assigned_at=NULL WHERE id=?",
            (container_id,),
        )
        logger.info("Released session %s → container %s now idle", session_id, container_id)
        return True


def expire_stale_sessions() -> int:
    """Release sessions that have exceeded SESSION_TTL_SECONDS. Returns count released."""
    cutoff = time.time() - SESSION_TTL_SECONDS
    with _cursor() as cur:
        cur.execute(
            "SELECT session_id FROM sessions WHERE released=0 AND assigned_at < ?",
            (cutoff,),
        )
        stale = [r["session_id"] for r in cur.fetchall()]
    count = 0
    for sid in stale:
        if release(sid):
            count += 1
    if count:
        logger.info("TTL expiry: released %d stale sessions", count)
    return count


# ---------------------------------------------------------------------------
# Pool status queries
# ---------------------------------------------------------------------------

def get_pool_status() -> Dict[str, Any]:
    with _cursor() as cur:
        cur.execute("SELECT level, state, COUNT(*) as cnt FROM containers GROUP BY level, state")
        rows = cur.fetchall()
    pools: Dict[int, Dict] = {}
    for row in rows:
        lv = row["level"]
        if lv not in pools:
            pools[lv] = {"level": lv, "idle": 0, "busy": 0, "unhealthy": 0, "total": 0}
        pools[lv][row["state"]] = row["cnt"]
        pools[lv]["total"] += row["cnt"]
    return {"pools": list(pools.values())}


def get_session_state(session_id: str) -> Optional[Dict[str, Any]]:
    with _cursor() as cur:
        cur.execute(
            """
            SELECT s.*, c.host, c.port, c.level as container_level
            FROM sessions s
            LEFT JOIN containers c ON s.container_id = c.id
            WHERE s.session_id = ?
            """,
            (session_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def mark_container_unhealthy(container_id: str) -> None:
    with _cursor() as cur:
        cur.execute("UPDATE containers SET state='unhealthy' WHERE id=?", (container_id,))
    logger.warning("Container %s marked unhealthy", container_id)
