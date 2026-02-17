"""Simple chat persistence for the API (JSON file)."""
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHAT_FILE = ROOT / "chat_history.json"
_file_lock = threading.Lock()


def _load():
    if not CHAT_FILE.exists():
        return {"chats": [], "current_chat_id": None}
    try:
        with open(CHAT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"chats": [], "current_chat_id": None}


def _save(data):
    CHAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHAT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def list_chats():
    with _file_lock:
        data = _load()
    return data.get("chats", []), data.get("current_chat_id")


def get_chat(chat_id):
    with _file_lock:
        data = _load()
    for c in data.get("chats", []):
        if c.get("id") == chat_id:
            return c
    return None


def create_chat():
    with _file_lock:
        data = _load()
        cid = str(uuid.uuid4())[:8]
        now = _now_iso()
        chat = {"id": cid, "title": "New chat", "created": now, "updated": now, "turns": []}
        data["chats"].append(chat)
        data["current_chat_id"] = cid
        _save(data)
    return chat


def set_current(chat_id):
    with _file_lock:
        data = _load()
        if chat_id and not any(c.get("id") == chat_id for c in data["chats"]):
            return False
        data["current_chat_id"] = chat_id
        _save(data)
    return True


def add_turn(chat_id, prompt, sql, results_count, results_preview, results_columns=None, steps=None):
    with _file_lock:
        data = _load()
        now = _now_iso()
        for c in data.get("chats", []):
            if c.get("id") == chat_id:
                c["turns"].append({
                    "prompt": prompt,
                    "sql": sql,
                    "results_count": results_count,
                    "results_preview": results_preview,
                    "results_columns": results_columns or [],
                    "steps": steps or [],
                })
                c["updated"] = now
                if c.get("title") == "New chat" and prompt:
                    c["title"] = (prompt[:47] + "...") if len(prompt) > 50 else prompt
                break
        _save(data)


def ensure_current():
    with _file_lock:
        data = _load()
        current_id = data.get("current_chat_id")
        if current_id and any(c.get("id") == current_id for c in data.get("chats", [])):
            return current_id
    # No lock needed here — create_chat acquires its own lock
    chat = create_chat()
    return chat["id"]
