"""
FastAPI backend for the React frontend.

Run from project root: uvicorn backend.server:app --reload --port 8000
"""
import sys
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from agent import QueryWriter, FALLBACK_SQL
from db.dataset import resolve_db_path
from backend.chat_store import (
    add_turn as store_add_turn,
    create_chat as store_create_chat,
    get_chat as store_get_chat,
    list_chats as store_list_chats,
    set_current as store_set_current,
    ensure_current as store_ensure_current,
)

DB_PATH = resolve_db_path()
agent: QueryWriter | None = None

# Polling-based progress: job_id -> { status, steps, result?, error?, _finished_at? }
_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()
_JOBS_TTL_SECONDS = 300  # evict completed jobs after 5 minutes


def _evict_old_jobs() -> None:
    """Remove completed jobs older than _JOBS_TTL_SECONDS. Must be called with _jobs_lock held."""
    now = time.monotonic()
    expired = [
        jid for jid, job in _jobs.items()
        if job.get("_finished_at") and now - job["_finished_at"] > _JOBS_TTL_SECONDS
    ]
    for jid in expired:
        del _jobs[jid]


def get_agent():
    global agent
    if agent is None:
        agent = QueryWriter(db_path=DB_PATH)
    return agent


def execute_query(sql: str):
    con = duckdb.connect(database=DB_PATH, read_only=True)
    try:
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        return cols, rows
    finally:
        con.close()


def _conversation_turns_for_chat(chat_id: str, max_turns: int = 6) -> list[dict]:
    """Return recent turns for context-aware text-to-SQL generation."""
    chat = store_get_chat(chat_id)
    if not chat:
        return []
    turns = chat.get("turns") or []
    if not isinstance(turns, list):
        return []
    return turns[-max_turns:]


app = FastAPI(title="SQL Query Writer API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AskRequest(BaseModel):
    prompt: str
    chat_id: str | None = None


class CurrentChatRequest(BaseModel):
    chat_id: str


@app.get("/api/chats")
def api_list_chats():
    chats, current_id = store_list_chats()
    return {"chats": chats, "current_chat_id": current_id}


@app.post("/api/chats")
def api_create_chat():
    chat = store_create_chat()
    return chat


@app.get("/api/chats/{chat_id}")
def api_get_chat(chat_id: str):
    chat = store_get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    return chat


@app.put("/api/current-chat")
def api_set_current_chat(body: CurrentChatRequest):
    ok = store_set_current(body.chat_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Chat not found")
    return {"current_chat_id": body.chat_id}


@app.post("/api/ask")
def api_ask(body: AskRequest):
    prompt = (body.prompt or "").strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt is required")
    chat_id = body.chat_id or store_ensure_current()
    try:
        ag = get_agent()
        sql, steps = ag.generate_query_with_steps(
            prompt,
            conversation_turns=_conversation_turns_for_chat(chat_id),
        )
        columns, rows = execute_query(sql)
        preview = [list(r) for r in rows[:100]]
        store_add_turn(
            chat_id, prompt, sql, len(rows), preview,
            results_columns=columns,
            steps=steps,
        )
        is_fallback = sql.strip().upper() == FALLBACK_SQL.upper()
        return {
            "sql": sql,
            "columns": columns,
            "rows": [list(r) for r in rows],
            "results_count": len(rows),
            "chat_id": chat_id,
            "steps": steps,
            "is_fallback": is_fallback,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _run_pipeline_for_job(job_id: str, prompt: str, chat_id: str) -> None:
    """Run pipeline in background; update _jobs[job_id] with steps and final result."""
    with _jobs_lock:
        _jobs[job_id] = {"status": "running", "steps": [], "result": None, "error": None}

    def on_step(step: dict) -> None:
        with _jobs_lock:
            if job_id in _jobs and _jobs[job_id]["status"] == "running":
                _jobs[job_id]["steps"] = _jobs[job_id]["steps"] + [step]

    try:
        ag = get_agent()
        sql, steps = ag.generate_query_with_steps(
            prompt,
            on_step=on_step,
            conversation_turns=_conversation_turns_for_chat(chat_id),
        )
        columns, rows = execute_query(sql)
        preview = [list(r) for r in rows[:100]]
        store_add_turn(
            chat_id, prompt, sql, len(rows), preview,
            results_columns=columns,
            steps=steps,
        )
        is_fallback = sql.strip().upper() == FALLBACK_SQL.upper()
        with _jobs_lock:
            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["_finished_at"] = time.monotonic()
            _jobs[job_id]["result"] = {
                "sql": sql,
                "columns": columns,
                "rows": [list(r) for r in rows],
                "results_count": len(rows),
                "chat_id": chat_id,
                "steps": steps,
                "is_fallback": is_fallback,
            }
    except Exception as e:
        with _jobs_lock:
            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["_finished_at"] = time.monotonic()
            _jobs[job_id]["error"] = str(e)


@app.post("/api/ask-start")
def api_ask_start(body: AskRequest):
    """Start pipeline in background; return job_id. Frontend polls GET /api/ask-status/:job_id for steps."""
    prompt = (body.prompt or "").strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt is required")
    chat_id = body.chat_id or store_ensure_current()
    job_id = uuid.uuid4().hex[:12]
    with _jobs_lock:
        _evict_old_jobs()
    thread = threading.Thread(target=_run_pipeline_for_job, args=(job_id, prompt, chat_id))
    thread.start()
    return {"job_id": job_id}


@app.get("/api/ask-status/{job_id}")
def api_ask_status(job_id: str):
    """Return current job state: { status, steps, result?, error? }. Poll until status is 'done'."""
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/api/health")
def health():
    return {"status": "ok"}
