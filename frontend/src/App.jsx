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

async function deleteChat(chatId) {
  const r = await fetch(`${API}/chats/${chatId}`, { method: 'DELETE' })
  if (!r.ok) {
    const err = await r.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to delete chat')
  }
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
function PipelineStepsDisplay({ steps, live = false, currentStage = null }) {
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
              <span className="step-label">Selector</span>
              <span className="step-detail">Tables: {s.tables?.length ? s.tables.join(', ') : '…'} {s.message || ''}</span>
            </div>
          )
        }
        if (s.step === 'planner') {
          return (
            <div key={i} className="step step-planner">
              <span className="step-label">Planner</span>
              <span className="step-detail">
                Intent: {s.plan?.intent || '—'}
              </span>
            </div>
          )
        }
        if (s.step === 'candidate') {
          return (
            <div key={i} className="step step-candidate">
              <span className="step-label">Candidate {s.candidate_index || '?'}</span>
              <pre className="step-sql">{s.sql || '—'}</pre>
            </div>
          )
        }
        if (s.step === 'critic') {
          return (
            <div key={i} className="step step-critic">
              <span className="step-label">Critic</span>
              <span className="step-detail">
                Selected #{s.selected_index || 1} (score: {Number.isFinite(s.score) ? s.score.toFixed(2) : '—'})
                {s.reason ? ` — ${s.reason}` : ''}
              </span>
              <pre className="step-sql">{s.sql || '—'}</pre>
            </div>
          )
        }
        if (s.step === 'decomposer') {
          return (
            <div key={i} className="step step-decomposer">
              <span className="step-label">Decomposer</span>
              <pre className="step-sql">{s.sql || s.message || '—'}</pre>
            </div>
          )
        }
        if (s.step === 'refiner_attempt') {
          return (
            <div key={i} className="step step-refiner">
              <span className="step-label">Refiner</span>
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
              <span className="step-label">Refiner</span>
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
        if (s.step === 'verifier') {
          return (
            <div key={i} className="step step-verifier">
              <span className="step-label">Verifier</span>
              {s.passed ? (
                <span className="attempt-ok">✓ Semantic check passed{ s.reason ? ` — ${s.reason}` : ''}</span>
              ) : (
                <span className="attempt-err">✗ Semantic check failed{ s.reason ? ` — ${s.reason}` : ''}</span>
              )}
              {!!s.suggested_sql && <pre className="step-sql">{s.suggested_sql}</pre>}
            </div>
          )
        }
        if (s.step === 'verifier_repair') {
          return (
            <div key={i} className="step step-verifier-repair">
              <span className="step-label">Verifier Repair</span>
              {s.success ? <span className="attempt-ok">✓ Repair query executed</span> : <span className="attempt-err">✗ Repair query failed</span>}
            </div>
          )
        }
        return null
      })}
      {live && currentStage && (
        <p className="pipeline-stage-footer">
          <span className="thinking">
            <span>{currentStage}</span>
            <span className="thinking-dots">
              <span />
              <span />
              <span />
            </span>
          </span>
        </p>
      )}
    </div>
  )
}

function Turn({ turn, defaultOpen, collapse }) {
  const [open, setOpen] = useState(defaultOpen)
  const [showSteps, setShowSteps] = useState(false)
  const preview = turn.results_preview || []
  const columns = turn.results_columns?.length ? turn.results_columns : (preview[0] ? preview[0].map((_, i) => `Column ${i + 1}`) : [])
  const isFallback = turn.sql?.trim().toUpperCase() === FALLBACK_SQL.toUpperCase()
  const hasSteps = turn.steps?.length > 0

  useEffect(() => {
    if (collapse) setOpen(false)
  }, [collapse])

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
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [pendingDeleteId, setPendingDeleteId] = useState(null)

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
      setSidebarOpen(false)
    } catch (e) {
      setError(e.message)
    }
  }

  const handleSwitchChat = async (id) => {
    try {
      setError(null)
      await setCurrentChat(id)
      setCurrentChatId(id)
      setSidebarOpen(false)
    } catch (e) {
      setError(e.message)
    }
  }

  const handleDeleteChat = async (id) => {
    try {
      setError(null)
      await deleteChat(id)
      setChats((prev) => prev.filter((c) => c.id !== id))
      if (currentChatId === id) {
        const remaining = chats.filter((c) => c.id !== id)
        setCurrentChatId(remaining[0]?.id ?? null)
      }
    } catch (e) {
      setError(e.message)
    }
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    const q = prompt.trim()
    if (!q || submitting) return

    // Clear the input immediately so it feels responsive
    setPrompt('')
    setSubmitting(true)
    setError(null)
    setLiveSteps([])

    // Optimistically show the user's question at the end of the current chat
    setChats((prev) => {
      if (!currentChatId) return prev
      return prev.map((c) => {
        if (c.id !== currentChatId) return c
        const turns = Array.isArray(c.turns) ? [...c.turns] : []
        turns.push({
          prompt: q,
          sql: '(pending…)',
          results_preview: [],
          results_columns: [],
          results_count: 0,
          steps: [],
        })
        const title =
          c.title && c.title !== 'New chat'
            ? c.title
            : (q.length > 60 ? `${q.slice(0, 57)}…` : q)
        return { ...c, turns, title }
      })
    })

    try {
      const result = await askWithProgress(q, currentChatId, (steps) => {
        setLiveSteps(steps)
      })
      if (result?.error) {
        setError(result.error)
        setSubmitting(false)
        setLiveSteps([])
      } else {
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

  const stageNameForStep = (step) => {
    switch (step) {
      case 'planner': return 'Planner'
      case 'selector': return 'Selector'
      case 'candidate': return 'Candidate'
      case 'critic': return 'Critic'
      case 'decomposer': return 'Decomposer'
      case 'refiner_attempt':
      case 'refiner': return 'Refiner'
      case 'verifier': return 'Verifier'
      case 'verifier_repair': return 'Verifier Repair'
      default: return 'Thinking'
    }
  }

  const currentStageLabel = (() => {
    if (!submitting && liveSteps.length === 0) return null
    if (liveSteps.length === 0) return 'Planner'
    const last = liveSteps[liveSteps.length - 1]
    return stageNameForStep(last.step)
  })()

  if (loading) {
    return (
      <div className="layout">
        <aside className="sidebar"><p>Loading…</p></aside>
        <main className="main"><p>Loading…</p></main>
      </div>
    )
  }

  const headerTitle = currentChat && currentChat.title && currentChat.title !== 'New chat'
    ? currentChat.title
    : ''

  return (
    <div className="layout">
      <aside className={`sidebar ${sidebarOpen ? 'sidebar-open' : ''}`}>
        <h1 className="logo">SQL Query Writer</h1>
        <p className="tagline">Ask in plain English → get SQL and results</p>
        <button
          type="button"
          className="sidebar-close"
          onClick={() => setSidebarOpen(false)}
          aria-label="Close chats sidebar"
        >
          ✕
        </button>
        <button type="button" className="btn btn-new" onClick={handleNewChat}>
          +
        </button>
        <div className="chat-list">
          <h2>Chats</h2>
          {chats.length === 0 ? (
            <p className="muted">No chats yet. Ask a question to start.</p>
          ) : (
            chats.map((c) => (
              <div key={c.id} className={`chat-item-row ${c.id === currentChatId ? 'active' : ''}`}>
                <button
                  type="button"
                  className="chat-item"
                  onClick={() => handleSwitchChat(c.id)}
                >
                  {c.id === currentChatId && '▶ '}{c.title} ({c.turns?.length ?? 0})
                </button>
                <button
                  type="button"
                  className="chat-delete"
                  onClick={(e) => {
                    e.stopPropagation()
                    setPendingDeleteId(c.id)
                  }}
                  aria-label="Delete chat"
                >
                  ✕
                </button>
                {pendingDeleteId === c.id && (
                  <div className="chat-delete-confirm">
                    <span>Delete?</span>
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation()
                        handleDeleteChat(c.id)
                        setPendingDeleteId(null)
                      }}
                    >
                      Yes
                    </button>
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation()
                        setPendingDeleteId(null)
                      }}
                    >
                      No
                    </button>
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      </aside>
      <main className="main">
        <header className="main-header">
          <button
            type="button"
            className="sidebar-toggle"
            onClick={() => setSidebarOpen((o) => !o)}
            aria-label="Toggle chats sidebar"
          >
            ☰
          </button>
          {/* Only show the header title when the sidebar is closed, so it doesn't clash visually */}
          {headerTitle && !sidebarOpen && <h2>{headerTitle}</h2>}
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
        <div className="turns">
          {turns.map((turn, i) => (
            <Turn
              key={i}
              turn={turn}
              defaultOpen={i === turns.length - 1}
              collapse={submitting}
            />
          ))}
          {submitting && liveSteps.length === 0 && currentStageLabel && (
            <div className="live-pipeline">
              <p className="processing-msg">
                <span className="thinking">
                  <span>{currentStageLabel}</span>
                  <span className="thinking-dots">
                    <span />
                    <span />
                    <span />
                  </span>
                </span>
              </p>
            </div>
          )}
          {liveSteps.length > 0 && currentStageLabel && (
            <div className="live-pipeline">
              <PipelineStepsDisplay steps={liveSteps} live currentStage={currentStageLabel} />
            </div>
          )}
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
