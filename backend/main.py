"""
main.py — FastAPI application entry point

Routes:
  GET       /                         → serves index.html
  GET       /auth/status              → { authenticated: bool }
  WebSocket /auth/terminal            → live agy PTY terminal (xterm.js compatible)
  POST      /upload                   → multipart upload → { session_id }
  GET       /pipeline/{sid}/stream    → SSE: full pipeline events
  GET       /pipeline/{sid}/download  → file download of fixed playbook
  GET       /pipeline/{sid}/original  → original uploaded playbook (for diff)
  GET       /health                   → { status: "ok" }
"""

import asyncio
import json
import logging
import os
import mimetypes
from pathlib import Path

from fastapi import FastAPI, File, UploadFile, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from agy_client import is_authenticated
from terminal import run_auth_terminal
from pipeline import create_session, run_pipeline, get_fixed_path, SESSIONS_DIR

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Antigravity Playbook Fixer", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files (frontend)
STATIC_DIR = Path("/app/static")
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# In-memory session store (session_id → Session object)
_sessions: dict = {}


# ---------------------------------------------------------------------------
# SSE helper
# ---------------------------------------------------------------------------

async def _sse_stream(generator):
    """Wrap an async generator in SSE framing (data: ...\n\n)."""
    try:
        async for event_json in generator:
            yield f"data: {event_json}\n\n"
        # Final keep-alive + close signal
        yield "data: {\"stage\":\"STREAM_END\",\"status\":\"done\"}\n\n"
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception("SSE generator error")
        yield f"data: {{\"stage\":\"ERROR\",\"status\":\"error\",\"message\":\"{str(e)[:200]}\"}}\n\n"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
async def serve_index():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return JSONResponse({"error": "Frontend not found"}, status_code=404)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/auth/status")
async def auth_status():
    authenticated = await is_authenticated()
    return {"authenticated": authenticated}



@app.websocket("/auth/terminal")
async def auth_terminal_ws(websocket: WebSocket):
    """
    WebSocket endpoint: bridges the browser to a live `agy` PTY session.

    Binary frames  → raw terminal bytes (rendered by xterm.js in the browser)
    Text frames    → JSON control messages (auth_url, auth_complete, etc.)
    """
    await websocket.accept()
    await run_auth_terminal(websocket)



@app.post("/upload")
async def upload_playbook(file: UploadFile = File(...)):
    """Accept a YAML playbook upload and create a session."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    # Basic validation
    if not file.filename.lower().endswith((".yml", ".yaml")):
        raise HTTPException(
            status_code=400,
            detail="Only .yml / .yaml files are accepted",
        )

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:  # 5 MB cap
        raise HTTPException(status_code=400, detail="File too large (max 5 MB)")

    session = create_session(content, file.filename)
    _sessions[session.session_id] = session

    logger.info(f"Created session {session.session_id} for {file.filename}")
    return {
        "session_id": session.session_id,
        "filename": file.filename,
        "size": len(content),
    }


@app.get("/pipeline/{session_id}/stream")
async def pipeline_stream(session_id: str, request: Request):
    """SSE endpoint streaming all pipeline events for a session."""
    session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    return StreamingResponse(
        _sse_stream(run_pipeline(session)),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/pipeline/{session_id}/download")
async def download_fixed(session_id: str):
    """Download the final verified playbook."""
    session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    fixed = get_fixed_path(session)
    if not fixed or not fixed.exists():
        raise HTTPException(
            status_code=404,
            detail="Fixed playbook not ready yet (pipeline may not have completed)",
        )

    return FileResponse(
        str(fixed),
        filename=f"fixed_playbook.yml",
        media_type="application/x-yaml",
    )


@app.get("/pipeline/{session_id}/original")
async def get_original(session_id: str):
    """Return the original uploaded playbook content."""
    session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"content": session.original_path.read_text()}


@app.get("/pipeline/{session_id}/current")
async def get_current(session_id: str):
    """Return the current (latest fixed) playbook content."""
    session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"content": session.current_path.read_text()}
