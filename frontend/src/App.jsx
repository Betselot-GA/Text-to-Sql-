import { useState, useEffect, useCallback } from 'react'

const API = '/api'

async function getChats() {
  let r
  try {
    r = await fetch(`${API}/chats`)
  } catch (e) {
    throw new Error(
      'Cannot reach the API. Start the backend from the project root: uvicorn backend.server:app --reload --port 8000'
    )
  }
  if (!r.ok) {
    const text = await r.text()
    throw new Error(`Failed to load chats (${r.status}). ${text || r.statusText}`)
  }
  return r.json()
}

async function createChat() {
  const r = await fetch(`${API}/chats`, { method: 'POST' })
  if (!r.ok) throw new Error('Failed to create chat')
  return r.json()
}

async function setCurrentChat(chatId) {
  const r = await fetch(`${API}/current-chat`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId }),
  })
  if (!r.ok) throw new Error('Failed to set chat')
}

async function ask(prompt, chatId = null) {
  const r = await fetch(`${API}/ask`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ prompt, chat_id: chatId }),
  })
  if (!r.ok) {
    const err = await r.json().catch(() => ({}))
    throw new Error(err.detail || r.statusText)
  }
  return r.json()
}

const POLL_INTERVAL_MS = 400

/**
 * Start pipeline via POST /api/ask-start, then poll GET /api/ask-status/:job_id.
 * onSteps(steps) is called every poll so the UI shows progress in real time.
 * Resolves with the final result when status is 'done'.
 */
function askWithProgress(prompt, chatId, onSteps) {
  return new Promise((resolve, reject) => {
    fetch(`${API}/ask-start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt, chat_id: chatId }),
    })
      .then((r) => {
        if (!r.ok) return r.json().then((err) => { throw new Error(err.detail || r.statusText) })
        return r.json()
      })
      .then(({ job_id }) => {
        const poll = () => {
          fetch(`${API}/ask-status/${job_id}`)
            .then((r) => {
              if (!r.ok) throw new Error('Job status failed')
              return r.json()
            })
            .then((data) => {
              if (data.steps && data.steps.length >= 0) onSteps(data.steps)
              if (data.status === 'done') {
                if (data.error) resolve({ error: data.error })
                else resolve(data.result || {})
              } else {
                setTimeout(poll, POLL_INTERVAL_MS)
              }
            })
            .catch(reject)
        }
        poll()
      })
      .catch(reject)
  })
}

function ResultsTable({ columns, rows }) {
  const data = rows || []
  const cols = columns && columns.length ? columns : data[0] ? data[0].map((_, i) => `Col ${i + 1}`) : []
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {cols.map((c, i) => (
              <th key={i}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.slice(0, 100).map((row, i) => (
            <tr key={i}>
              {row.map((cell, j) => (
                <td key={j}>{String(cell)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > 100 && <p className="more">… and {data.length - 100} more rows</p>}
    </div>
  )
}

const FALLBACK_SQL = 'SELECT NULL WHERE FALSE'

/** Renders pipeline steps (same shape for live and for saved turns). Handles selector, decomposer, refiner_attempt, refiner. */
function PipelineStepsDisplay({ steps, live = false }) {
  if (!steps?.length) return null
  return (
    <div className={`pipeline-steps ${live ? 'pipeline-steps-live' : ''}`}>
      <p><strong>{live ? 'Pipeline progress' : 'Pipeline steps'}</strong></p>
      {steps.map((s, i) => {
        if (s.step === 'error') {
          return <div key={i} className="step step-error">Error: {s.message}</div>
        }
        if (s.step === 'selector') {
          return (
            <div key={i} className="step step-selector">
              <span className="step-label">1. Selector</span>
              <span className="step-detail">Tables: {s.tables?.length ? s.tables.join(', ') : '…'} {s.message || ''}</span>
            </div>
          )
        }
        if (s.step === 'decomposer') {
          return (
            <div key={i} className="step step-decomposer">
              <span className="step-label">2. Decomposer</span>
              <pre className="step-sql">{s.sql || s.message || '—'}</pre>
            </div>
          )
        }
        if (s.step === 'refiner_attempt') {
          return (
            <div key={i} className="step step-refiner">
              <span className="step-label">3. Refiner</span>
              <div className="refiner-attempt">
                <span>Attempt {s.attempt}:</span>
                <pre className="step-sql">{s.sql}</pre>
                {s.success ? <span className="attempt-ok">✓ Executed successfully</span> : <span className="attempt-err">✗ {s.error || 'Error'}</span>}
              </div>
            </div>
          )
        }
        if (s.step === 'refiner') {
          const hasAttemptsAbove = steps.slice(0, i).some((x) => x.step === 'refiner_attempt')
          return (
            <div key={i} className="step step-refiner">
              <span className="step-label">3. Refiner</span>
              {!hasAttemptsAbove && s.attempts?.map((a, j) => (
                <div key={j} className="refiner-attempt">
                  <span>Attempt {a.attempt}:</span>
                  <pre className="step-sql">{a.sql}</pre>
                  {a.success ? <span className="attempt-ok">✓ Executed successfully</span> : <span className="attempt-err">✗ {a.error}</span>}
                </div>
              ))}
              {s.success ? <span className="attempt-ok">✓ Final query succeeded</span> : <span className="attempt-err">✗ Refiner did not get a successful execution</span>}
            </div>
          )
        }
        return null
      })}
    </div>
  )
}

function Turn({ turn, defaultOpen }) {
  const [open, setOpen] = useState(defaultOpen)
  const [showSteps, setShowSteps] = useState(false)
  const preview = turn.results_preview || []
  const columns = turn.results_columns?.length ? turn.results_columns : (preview[0] ? preview[0].map((_, i) => `Column ${i + 1}`) : [])
  const isFallback = turn.sql?.trim().toUpperCase() === FALLBACK_SQL.toUpperCase()
  const hasSteps = turn.steps?.length > 0

  return (
    <div className="turn">
      <button type="button" className="turn-head" onClick={() => setOpen((o) => !o)} aria-expanded={open}>
        <span className="turn-q">Q: {turn.prompt?.slice(0, 60)}{turn.prompt?.length > 60 ? '…' : ''}</span>
        <span className="turn-arrow">{open ? '▼' : '▶'}</span>
      </button>
      {open && (
        <div className="turn-body">
          <p><strong>Question</strong></p>
          <p className="turn-prompt">{turn.prompt}</p>
          {hasSteps && (
            <div className="turn-pipeline-toggle">
              <button type="button" className="btn-link" onClick={() => setShowSteps((s) => !s)}>
                {showSteps ? 'Hide pipeline steps' : 'Show pipeline steps'}
              </button>
              {showSteps && <PipelineStepsDisplay steps={turn.steps} live={false} />}
            </div>
          )}
          <p><strong>Generated SQL</strong></p>
          <pre className="sql">{turn.sql}</pre>
          {isFallback && (
            <div className="fallback-warning">
              The agent could not produce a valid query. Check that Ollama is running (<code>ollama serve</code>), the model is pulled (e.g. <code>ollama pull llama3.2</code>), and the database exists (run <code>python main.py</code> once).
            </div>
          )}
          {preview.length > 0 && !isFallback && (
            <>
              <p><strong>Results</strong> ({turn.results_count ?? preview.length} rows)</p>
              <ResultsTable columns={columns} rows={preview} />
            </>
          )}
        </div>
      )}
    </div>
  )
}

export default function App() {
  const [chats, setChats] = useState([])
  const [currentChatId, setCurrentChatId] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [prompt, setPrompt] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [liveSteps, setLiveSteps] = useState([])

  const loadChats = useCallback(async () => {
    try {
      setError(null)
      const data = await getChats()
      setChats(data.chats || [])
      setCurrentChatId(data.current_chat_id ?? (data.chats?.[0]?.id) ?? null)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadChats()
  }, [loadChats])

  const handleNewChat = async () => {
    try {
      setError(null)
      const chat = await createChat()
      setChats((prev) => [...prev, chat])
      setCurrentChatId(chat.id)
    } catch (e) {
      setError(e.message)
    }
  }

  const handleSwitchChat = async (id) => {
    try {
      setError(null)
      await setCurrentChat(id)
      setCurrentChatId(id)
    } catch (e) {
      setError(e.message)
    }
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    const q = prompt.trim()
    if (!q || submitting) return
    setSubmitting(true)
    setError(null)
    setLiveSteps([])
    try {
      const result = await askWithProgress(q, currentChatId, (steps) => {
        setLiveSteps(steps)
      })
      if (result?.error) {
        setError(result.error)
        setSubmitting(false)
        setLiveSteps([])
      } else {
        setPrompt('')
        await loadChats()
        setSubmitting(false)
        // Keep pipeline steps visible for 2.5s so the user can see what ran
        setTimeout(() => setLiveSteps([]), 2500)
      }
    } catch (e) {
      setError(e.message)
      setSubmitting(false)
      setLiveSteps([])
    }
  }

  const currentChat = chats.find((c) => c.id === currentChatId)
  const turns = currentChat?.turns ?? []

  if (loading) {
    return (
      <div className="layout">
        <aside className="sidebar"><p>Loading…</p></aside>
        <main className="main"><p>Loading…</p></main>
      </div>
    )
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <h1 className="logo">SQL Query Writer</h1>
        <p className="tagline">Ask in plain English → get SQL and results</p>
        <button type="button" className="btn btn-new" onClick={handleNewChat}>
          + New chat
        </button>
        <div className="chat-list">
          <h2>Chats</h2>
          {chats.length === 0 ? (
            <p className="muted">No chats yet. Ask a question to start.</p>
          ) : (
            chats.map((c) => (
              <button
                type="button"
                key={c.id}
                className={`chat-item ${c.id === currentChatId ? 'active' : ''}`}
                onClick={() => handleSwitchChat(c.id)}
              >
                {c.id === currentChatId && '▶ '}{c.title} ({c.turns?.length ?? 0})
              </button>
            ))
          )}
        </div>
      </aside>
      <main className="main">
        <header className="main-header">
          <h2>{currentChat?.title || 'New chat'}</h2>
        </header>
        {error && (
          <div className="error">
            {error}
            {error.includes('Cannot reach') && (
              <p className="error-hint">In a terminal, from the project root (with venv activated), run:<br />
                <code>uvicorn backend.server:app --reload --port 8000</code>
              </p>
            )}
          </div>
        )}
        {(submitting || liveSteps.length > 0) && (
          <div className="live-pipeline">
            {liveSteps.length > 0 ? (
              <PipelineStepsDisplay steps={liveSteps} live />
            ) : (
              <p className="processing-msg">Processing… (Selector → Decomposer → Refiner)</p>
            )}
          </div>
        )}
        <div className="turns">
          {turns.map((turn, i) => (
            <Turn key={i} turn={turn} defaultOpen={i === turns.length - 1} />
          ))}
        </div>
        <form className="ask-form" onSubmit={handleSubmit}>
          <input
            type="text"
            placeholder="Ask a question in natural language (e.g. What are the top 5 most expensive products?)"
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            disabled={submitting}
          />
          <button type="submit" disabled={submitting || !prompt.trim()}>
            {submitting ? 'Generating…' : 'Ask'}
          </button>
        </form>
      </main>
    </div>
  )
}
