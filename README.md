# SQL Query Writer Agent — Project Report

This document describes the **current functionality** of the project and a **step-by-step guide** to run it successfully.

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
- **Database**: Downloads any Kaggle dataset and loads it into DuckDB. The dataset and DB name are configured in `.env` (`KAGGLE_DATASET`, `DB_NAME`). Handled by `db/dataset.py`.

### Multi-Agent Pipeline

The agent uses a **Planner → Selector → Decomposer → Critic → Refiner → Verifier** pipeline:

1. **Planner**: Extracts structured intent and constraints from the question.
2. **Selector**: Chooses which database tables are relevant.
3. **Decomposer**: Generates N candidate SQL queries with chain-of-thought reasoning.
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
- **Pipeline steps**: Stored with each turn. In the UI, each turn has a **“Show pipeline steps”** link to expand and view Selector tables, Decomposer SQL, and Refiner attempts. During generation, pipeline steps are shown **in real time** (see below).

### Real-Time Pipeline Progress (React)

- While a question is being processed, the React app shows **live pipeline progress**: which step the model is on (Selector → Decomposer → Refiner) and the details of each step as they complete.
- This is implemented with **polling**: the frontend calls `POST /api/ask-start` to start the pipeline, then polls `GET /api/ask-status/:job_id` every 400 ms. The backend updates the job’s `steps` array as each pipeline step finishes, so the UI can display progress without relying on streaming.
- After the answer is returned, the main view shows only the question, generated SQL, and results. Pipeline steps remain available via **“Show pipeline steps”** in each saved turn.

### Interfaces

| Interface | Description |
|-----------|-------------|
| **CLI** (`main.py`) | Terminal interface: type questions, see SQL and results. Commands: `/new`, `/list`, `/switch N`, `/history`, `/help`. |
| **React frontend** | Web UI at http://localhost:5173. Uses the FastAPI backend. Shows **real-time pipeline steps** while generating; question, SQL, and results in each turn; optional “Show pipeline steps” per turn; fallback warning when the agent could not produce a valid query. |
| **FastAPI backend** | Serves `/api/chats`, `/api/ask`, `POST /api/ask-start`, `GET /api/ask-status/:id`, etc. Required for the React frontend. Run with `uvicorn backend.server:app --reload --port 8000`. |
### Robustness and Validation

- **Schema from server**: Schema can be refreshed from the database; tables are validated against the live catalog before being sent to the Decomposer.
- **Single execution path**: All execution goes through DuckDB, so errors and fixes are consistent.
- **Clear fallback behaviour**: When the agent cannot produce a valid query, the UI shows a warning and suggests checking Ollama, model, and database.

---

## Project Structure

```
Text-to-Sql/
├── .env                  # KAGGLE_DATASET, DB_NAME, OLLAMA_HOST, OLLAMA_MODEL
├── agent.py              # QueryWriter + Planner/Selector/Decomposer/Critic/Refiner/Verifier pipeline
├── main.py               # CLI entry point
├── foreign_keys.json     # Cached foreign key relationships (auto-generated)
├── runtime.txt           # Python version (e.g. python-3.11.9)
├── requirements.txt      # Python dependencies (install in venv)
├── db/
│   ├── __init__.py
│   └── dataset.py        # Generic Kaggle dataset loader → DuckDB
├── backend/
│   ├── __init__.py
│   ├── server.py         # FastAPI: chats, ask, ask-start, ask-status (polling)
│   └── chat_store.py     # Chat persistence (JSON)
└── frontend/             # React (Vite) web UI
    ├── package.json
    ├── vite.config.js    # Proxies /api to backend
    ├── index.html
    └── src/
        ├── main.jsx
        ├── App.jsx       # Chat list, turns, live pipeline progress, ask form
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
cd /path/to/Text-to-Sql
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

You should see `(venv)` in the prompt. All following `pip` and `python` commands assume this environment is active. If you prefer not to activate, use `.\venv\Scripts\pip.exe` and `.\venv\Scripts\python.exe` (Windows) instead.

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

The first run of `main.py` will download the Kaggle dataset using this file.

---

### Step 5: Create the database (first-time only)

From the project root, with the venv activated:

```bash
python main.py
```

- The first time, this downloads the dataset from Kaggle and creates the `.db` file in the `db/` folder (name based on `DB_NAME` in `.env`).
- When you see the "Enter your question" prompt, you can type a test question or type `quit` to exit.

After the database exists, you do not need to run `main.py` again just to create the DB; you can start the API or React flow directly.

---

### Step 6: Start Ollama (local LLM)

If you use **local Ollama**:

1. Install from [ollama.com](https://ollama.com/download) if needed.
2. Start Ollama (e.g. run `ollama serve` or start the Ollama app so the server is running).
3. Pull a model, e.g.:

   ```bash
   ollama pull llama3.2
   ```

If you use **Carleton’s LLM server**, set the environment variable (and any auth) as provided:

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
- Use `/new`, `/list`, `/switch N`, `/history`, `/help` as needed. Type `quit` or `exit` to stop.

---

### Step 7b: Run the React frontend (web UI)

You need **two** processes: the API backend and the React dev server.

**Terminal 1 — Backend (from project root, venv activated):**

```bash
uvicorn backend.server:app --reload --port 8000
```

Leave this running. You should see something like “Uvicorn running on http://127.0.0.1:8000”.

**Terminal 2 — React (from project root):**

```bash
cd frontend
npm install
npm run dev
```

Then open **http://localhost:5173** in your browser. The app will proxy `/api` to the backend. You can ask questions, create/switch chats, see **real-time pipeline steps** while the answer is generating, and view results (or the fallback warning if the agent could not produce a valid query). In each saved turn, use **“Show pipeline steps”** to view the pipeline details.

---

## Running the React Frontend

Summary of the two commands:

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
| **Generated SQL is `SELECT NULL WHERE FALSE`** | The agent could not produce a valid query. Check: (1) Ollama is running and a model is pulled (e.g. `ollama pull llama3.2`). (2) The database exists in `db/` (run `python main.py` once). (3) In the UI, open the turn, click **"Show pipeline steps"**, and read the red warning for more context. |
| **`python` not recognized** | Use the venv's executable: `.\venv\Scripts\python.exe main.py` (Windows). |
| **Venv activation does nothing (PowerShell)** | Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`, then run `.\venv\Scripts\Activate.ps1` again. |
| **Kaggle download fails** | Ensure `kaggle.json` is in the correct folder (`.kaggle` under your user directory) and that you are logged in to Kaggle. |
| **Packages install to the wrong Python** | Always activate the venv first, or call `.\venv\Scripts\pip.exe install -r requirements.txt` explicitly. |

---

## Quick Reference: Run Order

1. **One-time:** Create venv → `pip install -r requirements.txt` → set up Kaggle → `python main.py` (to create DB) → pull Ollama model.
2. **Every time (CLI):** Activate venv → ensure Ollama is running → `python main.py`.
3. **Every time (React):** Activate venv → start `uvicorn backend.server:app --reload --port 8000` → in another terminal run `cd frontend && npm run dev` → open http://localhost:5173.

This report reflects the project’s current functionality and run process as implemented.
