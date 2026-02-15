# SQL Query Writer – React frontend

This is the React (Vite) frontend for the SQL Query Writer agent. It talks to the FastAPI backend.

## Prerequisites

- Node.js 18+ and npm
- Backend running (see below)
- Database initialized (`bike_store.db` exists; run `python main.py` once from project root if needed)

## Run the backend (from project root)

```bash
# Activate venv, then:
pip install fastapi uvicorn
uvicorn backend.server:app --reload --port 8000
```

Leave this running in one terminal.

## Run the React app

```bash
cd frontend
npm install
npm run dev
```

Open **http://localhost:5173** in your browser. The dev server proxies `/api` to the backend at port 8000.

## Build for production

```bash
npm run build
```

Static files are in `frontend/dist`. Serve them with any static host; point `/api` to your backend (e.g. with nginx or the same FastAPI app serving the built files).
