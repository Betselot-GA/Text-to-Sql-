# SQL Query Writer Agent — Project Report

This document describes the **current functionality** of the project and a **step-by-step guide** to run it successfully, per project guidelines.

---

## Table of Contents

1. [Current Functionality](#current-functionality)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Step-by-Step: Run the Project Successfully](#step-by-step-run-the-project-successfully)
5. [Running the React Frontend](#running-the-react-frontend)
6. [Troubleshooting](#troubleshooting)

---

## Current Functionality

### Core: Text-to-SQL Agent

- **Natural language → SQL**: Users ask questions in plain English (e.g. “What are the top 5 most expensive products?” or “Which product is the most expensive?”). The agent returns an executable SQL query and (when run through the app) the query results.
- **Database**: Downloads the Kaggle dataset and loads it into DuckDB. The dataset and DB name are configured in `.env` (`KAGGLE_DATASET`, `DB_NAME`). Handled by `db/dataset.py`.

### Multi-Agent Pipeline

The agent uses a **Planner → Selector → Decomposer → Critic → Refiner → Verifier** pipeline:

1. **Planner**: Extracts structured intent and constraints from the question.
2. **Selector**: Chooses which database tables are relevant.
3. **Decomposer**: Generates candidate SQL queries with chain-of-thought reasoning.
4. **Critic**: Ranks candidates using execution feedback and picks the best.
5. **Refiner**: Executes the SQL. If it fails, uses the error to ask the LLM for a fix and retries (up to 3 attempts). The fallback `SELECT NULL WHERE FALSE` is **not** treated as success.
6. **Verifier**: Semantic check — compares the actual result data against the original question.

### LLM (Ollama)

- All agent steps use **Ollama** (open-source LLMs). Configuration is via environment variables:
  - `OLLAMA_HOST`: Server URL (default: `http://localhost:11434`).
  - `OLLAMA_MODEL`: Model name (default: `llama3.2`).
- Supports either **local Ollama** or **Carleton University LLM server** (set `OLLAMA_HOST` to the provided URL).

### Chat History

- **Multiple chat sessions**: Users can create new chats, list previous chats, and switch between them.
- **Per-chat history**: Each chat stores the questions asked, the generated SQL, result row count, and (in the UI) a preview of results.
- **Pipeline steps**: Stored with each turn. In the UI, each turn has a **“Show pipeline steps”** link to expand and view pipeline details. During generation, pipeline steps are shown **in real time** (see below).
- **Chat name**: The chat title is set from the first prompt (optimistically in the UI; persisted by the backend when the turn is saved).
- **Delete chat**: Users can remove a chat from the list. Clicking the ✕ next to a chat shows an inline confirmation (“Delete? Yes / No”); only **Yes** deletes the chat. Backend: `DELETE /api/chats/{chat_id}`.

### Real-Time Pipeline Progress (React)

- While a question is being processed, the React app shows **live pipeline progress**: which step the model is on and the details of each step as they complete.
- Implemented with **polling**: the frontend calls `POST /api/ask-start` to start the pipeline, then polls `GET /api/ask-status/:job_id` every 400 ms. The backend updates the job’s `steps` array as each pipeline step finishes.
- The current stage label with a **three-dot animation** appears at the **bottom of the Pipeline progress card** so the user does not need to scroll up.
- After the answer is returned, the main view shows the question, generated SQL, and results. Pipeline steps remain available via **“Show pipeline steps”** in each saved turn.

### Interfaces

| Interface | Description |
|-----------|-------------|
| **CLI** (`main.py`) | Terminal interface: type questions, see SQL and results. Pipeline stage names (Selector, Decomposer, Refiner) with a three-dot animation show while each step runs. Commands: `/new`, `/list`, `/switch N`, `/history`, `/help`. |
| **React frontend** | Web UI at http://localhost:5173. Responsive: hamburger icon opens the chats sidebar; ✕ in the sidebar top-right closes it. Sticky header keeps the hamburger visible when scrolling. Real-time pipeline steps at the bottom of the card; chat title from first prompt; input clears on submit; delete chat with inline “Delete? Yes/No” confirmation. |
| **FastAPI backend** | Serves `/api/chats`, `POST /api/chats`, `GET /api/chats/{id}`, `DELETE /api/chats/{id}`, `PUT /api/current-chat`, `POST /api/ask`, `POST /api/ask-start`, `GET /api/ask-status/:id`, `GET /api/health`. Run with `uvicorn backend.server:app --reload --port 8000`. |

### Robustness and Validation

- **Schema from server**: Schema can be refreshed from the database; tables are validated against the live catalog before being sent to the Decomposer.
- **Single execution path**: All execution goes through DuckDB, so errors and fixes are consistent.
- **Clear fallback behaviour**: When the agent cannot produce a valid query, the UI shows a warning and suggests checking Ollama, model, and database.

---

## Project Structure

```
carleton_competition_winter_2026/
├── .env                  # KAGGLE_DATASET, DB_NAME, OLLAMA_HOST, OLLAMA_MODEL
├── agent.py              # QueryWriter + Planner/Selector/Decomposer/Critic/Refiner/Verifier pipeline
├── main.py               # CLI entry point (with per-stage “thinking” dots)
├── runtime.txt            # Python version (e.g. python-3.11.9)
├── requirements.txt       # Python dependencies (install in venv)
├── Report.md              # This file
├── README.md              # Competition and documentation
├── db/
│   ├── __init__.py
│   └── dataset.py        # Kaggle dataset loader → DuckDB
├── backend/
│   ├── __init__.py
│   ├── server.py         # FastAPI: chats, ask, ask-start, ask-status, DELETE chat
│   └── chat_store.py      # Chat persistence (JSON); create, list, get, set_current, delete_chat
└── frontend/              # React (Vite) web UI
    ├── package.json
    ├── vite.config.js     # Proxies /api to backend
    ├── index.html
    └── src/
        ├── main.jsx
        ├── App.jsx        # Chat list (with delete + confirmation), turns, live pipeline, ask form
        └── App.css
```

---

## Prerequisites

- **Python 3.11+** (see `runtime.txt`).
- **Node.js 18+** and **npm** (only if you run the React frontend).
- **Ollama**: Either [installed locally](https://ollama.com/download) and running, or access to Carleton’s LLM server (set `OLLAMA_HOST`).
- **Kaggle account** and **Kaggle API** set up (for first-time database download). Place `kaggle.json` in `~/.kaggle/` (Linux/macOS) or `C:\Users\<username>\.kaggle\` (Windows).

---

## Step-by-Step: Run the Project Successfully

Follow these steps in order. Use the project root as the working directory unless stated otherwise.

### Step 1: Open the project folder

```bash
cd "c:\Users\Sassy\Desktop\Carleton Competition\carleton_competition_winter_2026"
```

(Use your actual path to the project.)

---

### Step 2: Create and use a virtual environment (recommended)

**Windows (PowerShell):**

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

If you see “script execution disabled”, run once (as your user):

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Then run the activation again.

**Windows (Command Prompt):**

```cmd
python -m venv venv
venv\Scripts\activate.bat
```

**macOS/Linux:**

```bash
python3 -m venv venv
source venv/bin/activate
```

You should see `(venv)` in the prompt. All following `pip` and `python` commands assume this environment is active.

---

### Step 3: Install Python dependencies inside the venv

```bash
pip install -r requirements.txt
```

On Windows without activation:

```powershell
.\venv\Scripts\pip.exe install -r requirements.txt
```

Wait until all packages install without errors.

---

### Step 4: Set up Kaggle (first-time only)

1. Log in at [Kaggle](https://www.kaggle.com/).
2. Account → “Create New API Token”. This downloads `kaggle.json`.
3. Put `kaggle.json` in:
   - **Windows:** `C:\Users\<YourUsername>\.kaggle\`
   - **macOS/Linux:** `~/.kaggle/`

The first run of `main.py` will download the dataset using this file.

---

### Step 5: Create the database (first-time only)

From the project root, with the venv activated:

```bash
python main.py
```

- The first time, this downloads the dataset from Kaggle and creates the database (path/name from `DB_NAME` in `.env`, typically under `db/`).
- When you see the “Enter your question” prompt, you can type a test question or type `quit` to exit.

After the database exists, you do not need to run `main.py` again just to create the DB.

---

### Step 6: Start Ollama (local LLM)

If you use **local Ollama**:

1. Install from [ollama.com](https://ollama.com/download) if needed.
2. Start Ollama (e.g. run `ollama serve` or start the Ollama app).
3. Pull a model, e.g.:

   ```bash
   ollama pull llama3.2
   ```

If you use **Carleton’s LLM server**, set the environment variable as provided:

```bash
set OLLAMA_HOST=<provided-server-url>
```

(Use `export OLLAMA_HOST=...` on macOS/Linux.)

---

### Step 7a: Run the CLI (terminal interface)

With the venv activated, database created, and Ollama running:

```bash
python main.py
```

- Type natural language questions and press Enter to see generated SQL and results.
- Pipeline stage names (Selector, Decomposer, Refiner) with a three-dot animation appear while each step runs.
- Use `/new`, `/list`, `/switch N`, `/history`, `/help` as needed. Type `quit` or `exit` to stop.

---

### Step 7b: Run the React frontend (web UI)

You need **two** processes: the API backend and the React dev server.

**Terminal 1 — Backend (from project root, venv activated):**

```bash
uvicorn backend.server:app --reload --port 8000
```

Leave this running.

**Terminal 2 — React (from project root):**

```bash
cd frontend
npm install
npm run dev
```

Then open **http://localhost:5173** in your browser. You can ask questions, create/switch/delete chats (with inline “Delete? Yes/No” confirmation), see real-time pipeline steps at the bottom of the progress card, and view results. Use **“Show pipeline steps”** in each turn to view pipeline details. On small screens, use the hamburger icon to open the sidebar and ✕ to close it.

---

## Running the React Frontend

| Step | Command | Where |
|------|---------|--------|
| 1 | `uvicorn backend.server:app --reload --port 8000` | Project root, venv activated |
| 2 | `cd frontend && npm install && npm run dev` | Project root then `frontend/` |

Then open **http://localhost:5173**. If you see “Failed to load chats” or “Cannot reach the API”, ensure the backend is running on port 8000.

---

## Troubleshooting

| Issue | What to do |
|-------|------------|
| **“Failed to load chats” or “Cannot reach the API”** | Start the backend: `uvicorn backend.server:app --reload --port 8000` from the project root (venv active). |
| **Generated SQL is `SELECT NULL WHERE FALSE`** | The agent could not produce a valid query. Check: (1) Ollama is running and a model is pulled (e.g. `ollama pull llama3.2`). (2) The database exists (run `python main.py` once). (3) In the UI, open the turn, click **“Show pipeline steps”**, and read the red warning. |
| **`python` not recognized** | Use the venv’s executable: `.\venv\Scripts\python.exe main.py` (Windows). |
| **Venv activation does nothing (PowerShell)** | Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`, then run `.\venv\Scripts\Activate.ps1` again. |
| **Kaggle download fails** | Ensure `kaggle.json` is in the correct folder (`.kaggle` under your user directory) and that you are logged in to Kaggle. |
| **Packages install to the wrong Python** | Always activate the venv first, or call `.\venv\Scripts\pip.exe install -r requirements.txt` explicitly. |

---

## Quick Reference: Run Order

1. **One-time:** Create venv → `pip install -r requirements.txt` → set up Kaggle → `python main.py` (to create DB) → pull Ollama model.
2. **Every time (CLI):** Activate venv → ensure Ollama is running → `python main.py`.
3. **Every time (React):** Activate venv → start `uvicorn backend.server:app --reload --port 8000` → in another terminal run `cd frontend && npm run dev` → open http://localhost:5173.

This report reflects the project’s current functionality and run process as implemented.
