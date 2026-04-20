"""
main.py — Orchestrator service (FastAPI).

Endpoints
---------
POST /escalate                   Receive Cerebrum decision, assign container, update nginx
GET  /session/{session_id}       Session state + container assignment
POST /session/{session_id}/release  Release session → free container
GET  /pools                      Container pool status
GET  /healthz                    Liveness probe
GET  /metrics                    Prometheus metrics
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import pool_manager as pm
import nginx_writer as nw

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("orchestrator")

# ---------------------------------------------------------------------------
# HMAC
# ---------------------------------------------------------------------------
import sys, os as _os
sys.path.insert(0, _os.path.dirname(__file__))
from hmac_utils import require_hmac  # noqa: E402

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class Decision(BaseModel):
    session_id: str
    action: str
    rule_id: str
    skill_score_after: int
    explanation: str


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

_counters: Dict[str, int] = {
    "escalations": 0,
    "releases": 0,
    "nginx_reloads": 0,
    "errors": 0,
}

# ---------------------------------------------------------------------------
# TTL expiry background task
# ---------------------------------------------------------------------------

async def _ttl_worker() -> None:
    """Periodically expire stale sessions."""
    interval = int(os.environ.get("TTL_CHECK_INTERVAL", "300"))
    while True:
        try:
            count = pm.expire_stale_sessions()
            if count:
                nw.rebuild_nginx_map()
                _counters["nginx_reloads"] += 1
        except Exception as exc:
            logger.error("TTL worker error: %s", exc)
        await asyncio.sleep(interval)

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Orchestrator starting up")
    pm.init_db()
    pm.seed_pool()
    # Rebuild nginx map from any persisted sessions
    try:
        nw.rebuild_nginx_map()
    except Exception as exc:
        logger.warning("Initial nginx map rebuild failed (nginx may not be running): %s", exc)
    ttl_task = asyncio.create_task(_ttl_worker(), name="ttl-worker")
    yield
    ttl_task.cancel()
    try:
        await ttl_task
    except asyncio.CancelledError:
        pass
    logger.info("Orchestrator shut down")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="dynamic-labyrinth Orchestrator",
    version="1.0.0",
    description="Container pool manager + nginx routing controller.",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.post("/escalate", dependencies=[Depends(require_hmac)])
async def escalate(decision: Decision) -> Dict[str, Any]:
    """
    Receive a Cerebrum escalation decision.
    Assigns a container and updates the nginx routing map.
    """
    _counters["escalations"] += 1
    action = decision.action

    level_map = {
        "escalate_to_level_2": 2,
        "escalate_to_level_3": 3,
    }

    if action not in level_map:
        logger.info("Orchestrator no-op for action=%s session=%s", action, decision.session_id)
        return {"ok": True, "note": f"no-op for action={action!r}"}

    target_level = level_map[action]

    try:
        container = pm.assign(decision.session_id, level=target_level)
    except RuntimeError as exc:
        _counters["errors"] += 1
        logger.error("Pool exhausted for session %s level %d: %s", decision.session_id, target_level, exc)
        raise HTTPException(status_code=503, detail=str(exc))

    # Update nginx map
    try:
        upstreams = nw.load_active_upstreams()
        nw.update_nginx(upstreams)
        _counters["nginx_reloads"] += 1
    except Exception as exc:
        logger.warning("Nginx update failed (non-fatal): %s", exc)

    logger.info(
        "Escalated session=%s level=%d container=%s rule=%s score=%d",
        decision.session_id, target_level, container.get("id"), decision.rule_id, decision.skill_score_after,
    )
    return {
        "ok": True,
        "session_id": decision.session_id,
        "container": container.get("id"),
        "upstream": container.get("upstream"),
        "level": target_level,
    }


@app.get("/session/{session_id}")
async def get_session(session_id: str) -> Dict[str, Any]:
    state = pm.get_session_state(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")
    return state


@app.post("/session/{session_id}/release", dependencies=[Depends(require_hmac)])
async def release_session(session_id: str) -> Dict[str, Any]:
    released = pm.release(session_id)
    if not released:
        raise HTTPException(status_code=404, detail="Session not found or already released")

    _counters["releases"] += 1
    try:
        upstreams = nw.load_active_upstreams()
        nw.update_nginx(upstreams)
        _counters["nginx_reloads"] += 1
    except Exception as exc:
        logger.warning("Nginx update after release failed: %s", exc)

    return {"ok": True, "session_id": session_id, "released": True}


@app.get("/pools")
async def get_pools() -> Dict[str, Any]:
    return pm.get_pool_status()


@app.get("/healthz")
async def health() -> Dict[str, Any]:
    try:
        status = pm.get_pool_status()
        db_ok = True
    except Exception:
        db_ok = False
        status = {}
    return {
        "status": "ok" if db_ok else "degraded",
        "db_ok": db_ok,
        "pools": status,
    }


@app.get("/metrics")
async def metrics():
    from fastapi.responses import PlainTextResponse
    pool = pm.get_pool_status()
    idle_total = sum(p.get("idle", 0) for p in pool.get("pools", []))
    busy_total = sum(p.get("busy", 0) for p in pool.get("pools", []))
    lines = [
        "# HELP dl_orchestrator_escalations_total Total escalation decisions received",
        "# TYPE dl_orchestrator_escalations_total counter",
        f"dl_orchestrator_escalations_total {_counters['escalations']}",
        "# HELP dl_orchestrator_releases_total Total sessions released",
        "# TYPE dl_orchestrator_releases_total counter",
        f"dl_orchestrator_releases_total {_counters['releases']}",
        "# HELP dl_orchestrator_pool_idle_containers Idle containers",
        "# TYPE dl_orchestrator_pool_idle_containers gauge",
        f"dl_orchestrator_pool_idle_containers {idle_total}",
        "# HELP dl_orchestrator_pool_busy_containers Busy containers",
        "# TYPE dl_orchestrator_pool_busy_containers gauge",
        f"dl_orchestrator_pool_busy_containers {busy_total}",
        "# HELP dl_orchestrator_nginx_reloads_total nginx reload count",
        "# TYPE dl_orchestrator_nginx_reloads_total counter",
        f"dl_orchestrator_nginx_reloads_total {_counters['nginx_reloads']}",
    ]
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


@app.get("/")
async def root():
    return {"service": "dynamic-labyrinth orchestrator", "version": "1.0.0"}
