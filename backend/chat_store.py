"""Simple chat persistence for the API (JSON file)."""
import json
import os
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHAT_FILE = ROOT / "chat_history.json"


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


def list_chats():
    data = _load()
    return data.get("chats", []), data.get("current_chat_id")


def get_chat(chat_id):
    chats, _ = list_chats()
    for c in chats:
        if c.get("id") == chat_id:
            return c
    return None


def create_chat():
    chats, _ = list_chats()
    cid = str(uuid.uuid4())[:8]
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    chat = {"id": cid, "title": "New chat", "created": now, "updated": now, "turns": []}
    chats.append(chat)
    _save({"chats": chats, "current_chat_id": cid})
    return chat


def set_current(chat_id):
    chats, _ = list_chats()
    if chat_id and not any(c.get("id") == chat_id for c in chats):
        return False
    _save({"chats": chats, "current_chat_id": chat_id})
    return True


def add_turn(chat_id, prompt, sql, results_count, results_preview, results_columns=None, steps=None):
    chats, current = list_chats()
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    for c in chats:
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
    _save({"chats": chats, "current_chat_id": current})


def ensure_current():
    chats, current_id = list_chats()
    if current_id and get_chat(current_id):
        return current_id
    chat = create_chat()
    return chat["id"]
