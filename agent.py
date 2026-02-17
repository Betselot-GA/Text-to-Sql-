"""
SQL Query Writer Agent

Multi-agent pipeline:  Planner → Selector → Decomposer (N candidates) → Critic → Refiner → Verifier.
Uses DuckDB for schema introspection and real query execution feedback.
All LLM calls go through Ollama.
"""

import os
import re
import json
from typing import Callable

import duckdb
from db.dataset import KaggleDataset, get_schema_info, resolve_db_path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_PROMPT_LENGTH = 4000
FALLBACK_SQL = "SELECT NULL WHERE FALSE"

# ---------------------------------------------------------------------------
# Foreign keys persistence
# ---------------------------------------------------------------------------

_FK_JSON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "foreign_keys.json")

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def _execute_sql(db_path: str, sql: str, read_only: bool = True) -> tuple[bool, str | None]:
    """Execute SQL on DuckDB.  Returns (True, None) on success, (False, error) on failure."""
    if not (sql or "").strip():
        return (False, "Empty SQL")
    con = None
    try:
        con = duckdb.connect(database=db_path, read_only=read_only)
        con.execute(sql.strip())
        return (True, None)
    except Exception as e:
        return (False, f"{type(e).__name__}: {e}")
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


def _execute_sql_with_results(
    db_path: str, sql: str, max_rows: int = 5,
) -> tuple[bool, list[str] | None, list[list] | None, int, str | None]:
    """Execute SQL and return (success, columns, sample_rows, total_count, error)."""
    if not (sql or "").strip():
        return (False, None, None, 0, "Empty SQL")
    con = None
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        result = con.execute(sql.strip())
        columns = [d[0] for d in result.description] if result.description else []
        all_rows = result.fetchall()
        total = len(all_rows)
        sample = [list(r) for r in all_rows[:max_rows]]
        return (True, columns, sample, total, None)
    except Exception as e:
        return (False, None, None, 0, f"{type(e).__name__}: {e}")
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


def _get_sample_data(db_path: str, table_name: str, n: int = 3) -> list[dict]:
    """Fetch *n* sample rows from a table as a list of dicts."""
    if not re.match(r"^[a-zA-Z_]\w*$", table_name):
        return []
    con = None
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        result = con.execute(f"SELECT * FROM {table_name} LIMIT {int(n)}")
        columns = [d[0] for d in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception:
        return []
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


def _get_tables_from_server(db_path: str) -> set[str]:
    """Get the set of table names from the DuckDB catalog."""
    con = None
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        return {r[0] for r in rows} if rows else set()
    except Exception:
        return set()
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


def _normalize_schema_columns(schema: dict) -> dict[str, set[str]]:
    """Return {table_name: {column_name, ...}} from schema dict."""
    normalized: dict[str, set[str]] = {}
    for table, columns in (schema or {}).items():
        if not isinstance(table, str) or not table:
            continue
        names: set[str] = set()
        if isinstance(columns, (list, tuple)):
            for col in columns:
                if not isinstance(col, dict):
                    continue
                name = str(col.get("name") or "").strip()
                if name:
                    names.add(name)
        if names:
            normalized[table] = names
    return normalized


def _singularize_table_name(table_name: str) -> str:
    """Best-effort singularization for table names (e.g., categories -> category)."""
    t = (table_name or "").strip().lower()
    if t.endswith("ies") and len(t) > 3:
        return t[:-3] + "y"
    if t.endswith("s") and len(t) > 1:
        return t[:-1]
    return t


def _discover_foreign_keys_from_catalog(db_path: str) -> list[tuple[str, str]]:
    """Discover FK edges from DB metadata, if constraints are available."""
    con = None
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        rows = con.execute(
            """
            SELECT
                kcu.table_name AS fk_table,
                kcu.column_name AS fk_column,
                ccu.table_name AS pk_table,
                ccu.column_name AS pk_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.referential_constraints AS rc
              ON tc.constraint_name = rc.constraint_name
             AND tc.table_schema = rc.constraint_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON rc.unique_constraint_name = ccu.constraint_name
             AND rc.unique_constraint_schema = ccu.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'main'
            ORDER BY fk_table, fk_column
            """
        ).fetchall()
        edges: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for fk_table, fk_column, pk_table, pk_column in rows or []:
            fk_from = f"{fk_table}.{fk_column}"
            fk_to = f"{pk_table}.{pk_column}"
            edge = (fk_from, fk_to)
            if edge in seen:
                continue
            seen.add(edge)
            edges.append(edge)
        return edges
    except Exception:
        return []
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


def _infer_foreign_keys_from_schema(schema: dict) -> list[tuple[str, str]]:
    """
    Infer likely FK edges from schema column names when explicit constraints are missing.

    Strategy:
    - Use non-PK *_id columns as FK candidates.
    - Prefer targets where the target's likely PK matches the same column.
    - Add common self-reference patterns like manager_id -> <table_pk>.
    """
    table_columns = _normalize_schema_columns(schema)
    if not table_columns:
        return []

    likely_pk: dict[str, str | None] = {}
    for table, cols in table_columns.items():
        pk_col = f"{_singularize_table_name(table)}_id"
        likely_pk[table] = pk_col if pk_col in cols else None

    inferred: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add_edge(fk_from: str, fk_to: str) -> None:
        edge = (fk_from, fk_to)
        if fk_from == fk_to or edge in seen:
            return
        seen.add(edge)
        inferred.append(edge)

    for source_table, source_cols in table_columns.items():
        source_pk = likely_pk.get(source_table)

        for col in sorted(source_cols):
            if not col.endswith("_id"):
                continue
            if source_pk and col == source_pk:
                continue

            candidates: list[tuple[int, str]] = []
            for target_table, target_cols in table_columns.items():
                if target_table == source_table or col not in target_cols:
                    continue
                score = 0
                target_pk = likely_pk.get(target_table)
                if target_pk and col == target_pk:
                    score += 5
                if col.startswith(f"{_singularize_table_name(target_table)}_"):
                    score += 2
                candidates.append((score, target_table))

            if not candidates:
                continue

            candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
            best_score = candidates[0][0]
            best_targets = [t for score, t in candidates if score == best_score]

            if best_score >= 5 and len(best_targets) == 1:
                add_edge(f"{source_table}.{col}", f"{best_targets[0]}.{col}")
            elif len(candidates) == 1:
                add_edge(f"{source_table}.{col}", f"{candidates[0][1]}.{col}")

        if source_pk and source_pk in source_cols:
            for col in sorted(source_cols):
                if col == source_pk or not col.endswith("_id"):
                    continue
                if col.startswith(("manager_", "parent_", "supervisor_")):
                    add_edge(f"{source_table}.{col}", f"{source_table}.{source_pk}")

    return inferred


def _resolve_foreign_keys(db_path: str, schema: dict) -> list[tuple[str, str]]:
    """
    Resolve FK edges using catalog metadata and schema inference.

    Raises RuntimeError if no foreign keys can be discovered or inferred.
    """
    table_columns = _normalize_schema_columns(schema)
    if not table_columns:
        raise RuntimeError(
            "Cannot resolve foreign keys: schema is empty. "
            "Make sure the database has been initialized."
        )

    discovered = _discover_foreign_keys_from_catalog(db_path)
    inferred = _infer_foreign_keys_from_schema(schema)

    merged: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for source in (discovered, inferred):
        for fk_from, fk_to in source:
            if "." not in fk_from or "." not in fk_to:
                continue
            from_table, from_col = fk_from.split(".", 1)
            to_table, to_col = fk_to.split(".", 1)
            if from_table not in table_columns or to_table not in table_columns:
                continue
            if from_col not in table_columns[from_table] or to_col not in table_columns[to_table]:
                continue
            edge = (fk_from, fk_to)
            if edge in seen:
                continue
            seen.add(edge)
            merged.append(edge)

    if not merged:
        raise RuntimeError(
            "No foreign keys found in the database. "
            "The dataset may not have relational constraints between tables."
        )

    return merged


# ---------------------------------------------------------------------------
# Text formatting helpers
# ---------------------------------------------------------------------------


def _format_schema(schema: dict, sample_data: dict | None = None) -> str:
    """Format schema with column types and optional sample rows."""
    if not schema:
        return ""
    parts: list[str] = []
    for table, columns in schema.items():
        if not isinstance(columns, (list, tuple)):
            continue
        col_lines = []
        for col in columns:
            if isinstance(col, dict) and "name" in col:
                col_lines.append(f"  {col['name']} ({col.get('type', '?')})")
        if not col_lines:
            continue
        section = f"Table: {table}\n  Columns:\n" + "\n".join(f"  {c}" for c in col_lines)
        if sample_data and table in sample_data and sample_data[table]:
            rows = sample_data[table][:3]
            sample_lines = []
            for row in rows:
                vals = ", ".join(f"{k}={repr(v)}" for k, v in row.items())
                sample_lines.append(f"    ({vals})")
            section += "\n  Sample rows:\n" + "\n".join(sample_lines)
        parts.append(section)
    return "\n\n".join(parts)


def _format_foreign_keys(
    foreign_keys: list[tuple[str, str]],
    relevant_tables: set[str] | None = None,
) -> str:
    """Format FK relationships, optionally filtered to relevant tables."""
    lines: list[str] = []
    for fk_from, fk_to in foreign_keys:
        t1 = fk_from.split(".")[0]
        t2 = fk_to.split(".")[0]
        if relevant_tables and (t1 not in relevant_tables or t2 not in relevant_tables):
            continue
        lines.append(f"  {fk_from} -> {fk_to}")
    if not lines:
        return ""
    return "Foreign key relationships:\n" + "\n".join(lines)


def _format_conversation(turns: list[dict] | None, max_turns: int = 4) -> str:
    """Render recent conversation history for context-aware prompting."""
    if not turns:
        return "(none)"
    cleaned: list[dict] = []
    for t in turns[-max_turns:]:
        if not isinstance(t, dict):
            continue
        q = str(t.get("prompt") or "").strip()
        s = str(t.get("sql") or "").strip()
        if q or s:
            cleaned.append({"prompt": q[:400], "sql": s[:600]})
    if not cleaned:
        return "(none)"
    lines: list[str] = []
    for i, t in enumerate(cleaned, 1):
        lines.append(f"Turn {i} Q: {t['prompt']}")
        lines.append(f"Turn {i} SQL: {t['sql']}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Response parsing helpers
# ---------------------------------------------------------------------------


def _extract_sql(text: str) -> str:
    """Extract a single SQL query from LLM output (handles CoT, markdown, etc.)."""
    if not text or not isinstance(text, str):
        return ""
    text = text.strip()
    if not text:
        return ""
    # Check for chain-of-thought markers
    for marker in ("FINAL SQL:", "Final SQL:", "SQL:"):
        if marker in text:
            idx = text.index(marker)
            text = text[idx + len(marker):].strip()
            break
    # Find first SELECT
    upper = text.upper()
    if "SELECT" in upper:
        idx = upper.index("SELECT")
        text = text[idx:]
    # Strip markdown fences
    for prefix in ("```sql", "```"):
        if text.startswith(prefix):
            text = text[len(prefix):].strip()
    if text.endswith("```"):
        text = text[:-3].strip()
    # Clean trailing semicolons / whitespace
    text = text.rstrip()
    if text.endswith(";"):
        text = text[:-1].strip()
    # Take only the first statement (in case of multiple)
    if ";" in text:
        text = text[: text.index(";")].strip()
    return text.strip()


def _parse_selector_tables(response: str, all_tables: set[str]) -> list[str]:
    """Parse Selector output into a list of valid table names."""
    if not response or not all_tables:
        return list(all_tables)
    response_upper = response.strip().upper()
    tables_upper = {t.upper(): t for t in all_tables}
    # Try comma / semicolon / newline split
    parts = [p.strip() for p in re.split(r"[,;\n]+", response) if p.strip()]
    found = [p.upper() for p in parts if p.upper() in tables_upper]
    if found:
        seen: set[str] = set()
        result: list[str] = []
        for p in found:
            if p not in seen:
                seen.add(p)
                result.append(tables_upper[p])
        return result
    # Fall back to whole-word matching
    found_names: list[str] = []
    for upper_name, original in tables_upper.items():
        if re.search(r"\b" + re.escape(upper_name) + r"\b", response_upper):
            found_names.append(original)
    return found_names if found_names else list(all_tables)


def _safe_llm_content(response) -> str:
    """Extract text from an Ollama chat response (dict or object)."""
    if not response:
        return ""
    # Dict format
    if isinstance(response, dict):
        msg = response.get("message")
        if msg and isinstance(msg, dict):
            return str(msg.get("content", "")).strip()
    # Object format (ollama >= 0.4)
    if hasattr(response, "message"):
        msg = response.message
        if hasattr(msg, "content"):
            return str(msg.content or "").strip()
    return ""


def _safe_json(text: str) -> dict:
    """Best-effort parse a JSON object from LLM output."""
    if not text:
        return {}
    candidate = text.strip()
    if "{" in candidate and "}" in candidate:
        start = candidate.find("{")
        end = candidate.rfind("}")
        if 0 <= start < end:
            candidate = candidate[start : end + 1]
    try:
        parsed = json.loads(candidate)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# LLM clients
# ---------------------------------------------------------------------------


def _get_ollama_client():
    """Create an Ollama client using OLLAMA_HOST env var."""
    import ollama

    host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    return ollama.Client(host=host)


def get_ollama_model_name() -> str:
    """Return the model name from OLLAMA_MODEL env var (default: llama3.2)."""
    return os.getenv("OLLAMA_MODEL", "llama3.2")


# ---------------------------------------------------------------------------
# QueryWriter
# ---------------------------------------------------------------------------


class QueryWriter:
    """
    SQL Query Writer Agent that converts natural language to SQL queries.

    Pipeline stages:
      1. Planner   – extract structured intent / constraints
      2. Selector  – pick relevant tables from the full schema
      3. Decomposer – generate N candidate SQL queries (chain-of-thought)
      4. Critic    – rank candidates using execution feedback
      5. Refiner   – execute SQL, fix errors via LLM, retry
      6. Verifier  – semantic check with actual result data
    """

    def __init__(self, db_path: str | None = None):
        """
        Initialize the QueryWriter.

        Args:
            db_path: Path to the DuckDB database file. Resolved from .env if omitted.
        """
        self.db_path = resolve_db_path(db_path)

        # Initialize DB (downloads dataset if needed)
        KaggleDataset(db_path=self.db_path)

        # Schema
        self.schema: dict = get_schema_info(db_path=self.db_path) or {}

        # Schema-dependent context
        self._sample_data: dict[str, list[dict]] = {}
        self.foreign_keys: list[tuple[str, str]] = []
        self._refresh_schema_context()

        # LLM (Ollama only)
        self.client = _get_ollama_client()
        self.model = get_ollama_model_name()

        # Pipeline config
        self._max_refiner_retries = 3
        self._candidate_count = max(
            1, min(3, int(os.getenv("AGENT_CANDIDATE_COUNT", "2") or "2"))
        )
        self._history_turns = max(
            0, min(8, int(os.getenv("AGENT_HISTORY_TURNS", "4") or "4"))
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def generate_query(self, prompt: str) -> str:
        """
        Competition-required interface.

        Generate a SQL query from a natural language prompt.

        Args:
            prompt: The natural language question from the user.

        Returns:
            A valid SQL query string that answers the question.
        """
        sql, _ = self.generate_query_with_steps(prompt)
        return sql

    def generate_query_with_steps(
        self,
        prompt: str,
        on_step: Callable[[dict], None] | None = None,
        conversation_turns: list[dict] | None = None,
    ) -> tuple[str, list[dict]]:
        """
        Generate SQL and return pipeline steps for debugging / UI.

        Returns:
            (final_sql, steps) where steps is a list of step dicts.
        """

        def emit(s: dict) -> None:
            if on_step:
                on_step(s)

        steps: list[dict] = []
        prompt_clean = (prompt or "").strip()

        # Guard: empty prompt
        if not prompt_clean:
            err = {"step": "error", "message": "Empty prompt"}
            steps.append(err)
            emit(err)
            return (FALLBACK_SQL, steps)
        if len(prompt_clean) > _MAX_PROMPT_LENGTH:
            prompt_clean = prompt_clean[:_MAX_PROMPT_LENGTH] + "..."

        history = self._trim_history(conversation_turns)

        # Guard: no schema
        if not self.schema:
            try:
                self.schema = get_schema_info(db_path=self.db_path) or {}
                self._refresh_schema_context()
            except Exception:
                pass
        if not self.schema:
            err = {"step": "error", "message": "No database schema loaded."}
            steps.append(err)
            emit(err)
            return (FALLBACK_SQL, steps)

        try:
            # 1) Planner
            plan = self._run_planner(prompt_clean, history)
            s = {"step": "planner", "plan": plan}
            steps.append(s)
            emit(s)

            # 2) Selector
            relevant_tables = self._run_selector(prompt_clean, plan, history)
            if not relevant_tables:
                relevant_tables = list(self.schema.keys())
            relevant_tables = self._validate_tables(relevant_tables)
            if not relevant_tables:
                relevant_tables = list(self.schema.keys())
            schema_subset = {t: self.schema[t] for t in relevant_tables if t in self.schema}
            if not schema_subset:
                s = {"step": "selector", "tables": [], "message": "No valid tables"}
                steps.append(s)
                emit(s)
                return (FALLBACK_SQL, steps)
            s = {"step": "selector", "tables": list(schema_subset.keys())}
            steps.append(s)
            emit(s)

            # 3) Decomposer: generate N candidates + pre-execute each
            candidates: list[str] = []
            candidate_exec: list[tuple] = []  # (sql, ok, cols, rows, count, err)

            for idx in range(self._candidate_count):
                sql_candidate = self._run_decomposer(
                    prompt_clean,
                    schema_subset,
                    plan,
                    history,
                    candidate_index=idx,
                    total_candidates=self._candidate_count,
                    existing_candidates=candidates,
                )
                if sql_candidate and "SELECT" in sql_candidate.upper():
                    ok, cols, rows, count, err = _execute_sql_with_results(
                        self.db_path, sql_candidate, max_rows=5,
                    )
                    candidates.append(sql_candidate)
                    candidate_exec.append((sql_candidate, ok, cols, rows, count, err))
                    cs = {
                        "step": "candidate",
                        "candidate_index": idx + 1,
                        "sql": sql_candidate,
                        "executed": ok,
                        "row_count": count if ok else 0,
                        "error": err if not ok else None,
                    }
                    steps.append(cs)
                    emit(cs)

            if not candidates:
                s = {"step": "decomposer", "sql": "(empty)", "message": "No valid candidates"}
                steps.append(s)
                emit(s)
                return (FALLBACK_SQL, steps)

            # 4) Critic: rank with execution data
            selected_sql, critic = self._run_critic(
                prompt_clean, candidates, schema_subset, plan, history, candidate_exec,
            )
            if not selected_sql:
                selected_sql = candidates[0]
            s = {
                "step": "critic",
                "selected_index": critic.get("selected_index", 1),
                "score": critic.get("score", 0.0),
                "reason": critic.get("reason", ""),
                "sql": selected_sql,
            }
            steps.append(s)
            emit(s)

            # 5) Refiner: execute + fix loop
            final_sql, executed_ok, refiner_attempts = self._run_refiner(
                selected_sql, on_attempt=emit,
            )
            s = {"step": "refiner", "attempts": refiner_attempts, "success": executed_ok}
            steps.append(s)
            emit(s)
            if not executed_ok:
                return (FALLBACK_SQL, steps)

            # 6) Verifier: semantic check with actual result data
            ok, cols, rows, count, _ = _execute_sql_with_results(
                self.db_path, final_sql, max_rows=5,
            )
            verifier = self._run_verifier(
                prompt_clean, final_sql, schema_subset, plan, history,
                result_columns=cols, result_rows=rows, result_count=count,
            )
            vs = {"step": "verifier", **verifier}
            steps.append(vs)
            emit(vs)

            if not verifier.get("passed", True):
                suggested = (verifier.get("suggested_sql") or "").strip()
                if suggested and suggested != final_sql:
                    repaired_sql, repaired_ok, repair_attempts = self._run_refiner(
                        suggested, on_attempt=emit,
                    )
                    rs = {"step": "verifier_repair", "success": repaired_ok, "attempts": repair_attempts}
                    steps.append(rs)
                    emit(rs)
                    if repaired_ok:
                        final_sql = repaired_sql

            return (final_sql.strip(), steps)

        except Exception as e:
            err = {"step": "error", "message": str(e)}
            steps.append(err)
            emit(err)
            return (FALLBACK_SQL, steps)

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def _chat(self, messages: list[dict]) -> dict:
        """Send a chat request to Ollama."""
        return self.client.chat(model=self.model, messages=messages)

    def _run_planner(self, question: str, history: list[dict]) -> dict:
        """Planner: extract structured intent / constraints."""
        try:
            schema_text = _format_schema(self.schema)
            history_text = _format_conversation(history)
            system = (
                "You are a planning agent for text-to-SQL on a DuckDB database.\n"
                "Return ONLY valid JSON with keys:\n"
                "- intent: short string describing what the user wants\n"
                "- constraints: array of filter conditions mentioned\n"
                "- entities: array of relevant tables or columns\n"
                "- metrics: array of aggregation metrics (COUNT, SUM, AVG, etc.)\n"
                "- time_scope: date/time constraints (empty string if none)\n"
                "- output_shape: expected format (e.g. 'single number', 'list', 'table')\n\n"
                f"Conversation:\n{history_text}\n\nSchema:\n{schema_text}"
            )
            resp = self._chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": question},
                ],
            )
            parsed = _safe_json(_safe_llm_content(resp))
            if parsed:
                return parsed
        except Exception as e:
            print(f"Planner error: {e}")
        return {
            "intent": "answer the question with SQL",
            "constraints": [],
            "entities": [],
            "metrics": [],
            "time_scope": "",
            "output_shape": "",
        }

    def _run_selector(self, question: str, plan: dict, history: list[dict]) -> list[str]:
        """Selector: choose relevant tables from the full schema."""
        all_tables = set(self.schema.keys())
        if not all_tables:
            return []
        try:
            schema_text = _format_schema(self.schema)
            history_text = _format_conversation(history)
            plan_text = json.dumps(plan or {})
            system = (
                "You are a table selector for a DuckDB SQL database.\n"
                "Given a user question, output ONLY a comma-separated list of table names needed.\n"
                "Rules:\n"
                "- Use exact table names from the schema.\n"
                "- Include tables needed for JOINs even if not directly queried.\n"
                "- No explanations, no markdown.\n\n"
                f"Plan: {plan_text}\n\nConversation:\n{history_text}\n\nSchema:\n{schema_text}"
            )
            resp = self._chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": question},
                ],
            )
            return _parse_selector_tables(_safe_llm_content(resp), all_tables)
        except Exception as e:
            print(f"Selector error: {e}")
            return list(all_tables)

    def _run_decomposer(
        self,
        question: str,
        schema_subset: dict,
        plan: dict,
        history: list[dict],
        candidate_index: int = 0,
        total_candidates: int = 1,
        existing_candidates: list[str] | None = None,
    ) -> str:
        """Decomposer: generate one SQL candidate with chain-of-thought reasoning."""
        if not schema_subset:
            return ""
        try:
            table_set = set(schema_subset.keys())
            schema_text = _format_schema(schema_subset, self._sample_data)
            fk_text = _format_foreign_keys(self.foreign_keys, table_set)
            history_text = _format_conversation(history)
            plan_text = json.dumps(plan or {})
            existing_text = (
                "\n".join(f"Candidate {i+1}: {sql}" for i, sql in enumerate(existing_candidates or []))
                or "(none)"
            )

            system = (
                "You are an expert SQL query writer for a DuckDB database.\n\n"
                "TASK: Translate the user's question into ONE executable SQL query.\n\n"
                "RULES:\n"
                "- Use ONLY tables and columns from the schema below.\n"
                "- Database engine is DuckDB (standard SQL).\n"
                "- Use LIMIT N (NOT TOP N) to limit rows.\n"
                "- Use explicit JOIN ... ON ... syntax. Never use implicit joins.\n"
                "- Always use table aliases in multi-table queries.\n"
                "- For 'above average' / 'more than average': use a subquery, e.g. "
                "WHERE col > (SELECT AVG(col) FROM ...).\n"
                "- For 'top N' / 'most' / 'highest': ORDER BY ... DESC LIMIT N.\n"
                "- For 'bottom N' / 'least' / 'lowest': ORDER BY ... ASC LIMIT N.\n"
                "- String comparisons are case-sensitive. Use ILIKE for case-insensitive.\n"
                "- Date functions: YEAR(date_col), MONTH(date_col), DAY(date_col).\n\n"
                "THINK STEP BY STEP:\n"
                "1. Which tables do I need?\n"
                "2. What are the correct JOIN conditions? (see foreign keys below)\n"
                "3. What WHERE filters apply?\n"
                "4. Do I need GROUP BY? (required when mixing aggregates with non-aggregated columns)\n"
                "5. Do I need HAVING for group-level filters?\n"
                "6. What columns belong in SELECT?\n"
                "7. What ORDER BY and LIMIT are needed?\n\n"
                "After reasoning, output your final query on a line starting with:\n"
                "FINAL SQL:\n\n"
                f"{fk_text}\n\n"
                f"Conversation:\n{history_text}\n\n"
                f"Plan: {plan_text}\n\n"
                f"Previous candidates (produce a different valid approach):\n{existing_text}\n\n"
                f"Generating candidate {candidate_index + 1} of {total_candidates}.\n\n"
                f"SCHEMA (with sample data):\n{schema_text}"
            )
            resp = self._chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": question},
                ],
            )
            return _extract_sql(_safe_llm_content(resp))
        except Exception as e:
            print(f"Decomposer error: {e}")
            return ""

    def _run_critic(
        self,
        question: str,
        candidates: list[str],
        schema_subset: dict,
        plan: dict,
        history: list[dict],
        candidate_exec: list[tuple] | None = None,
    ) -> tuple[str, dict]:
        """Critic: rank candidates (with execution data) and pick the best."""
        if not candidates:
            return "", {"selected_index": 1, "score": 0.0, "reason": "No candidates"}
        if len(candidates) == 1:
            return candidates[0], {"selected_index": 1, "score": 0.7, "reason": "Single candidate"}
        try:
            schema_text = _format_schema(schema_subset)
            history_text = _format_conversation(history)
            plan_text = json.dumps(plan or {})

            # Build candidate block with execution results
            cand_lines: list[str] = []
            for i, sql in enumerate(candidates):
                line = f"[{i + 1}] SQL: {sql}"
                if candidate_exec and i < len(candidate_exec):
                    _, ok, cols, rows, count, err = candidate_exec[i]
                    if ok:
                        line += f"\n    Execution: SUCCESS — {count} rows"
                        if cols:
                            line += f", columns: {cols}"
                        if rows:
                            line += f"\n    Sample: {rows[:3]}"
                    else:
                        line += f"\n    Execution: FAILED — {err}"
                cand_lines.append(line)
            cand_block = "\n\n".join(cand_lines)

            system = (
                "You are a SQL Critic for a DuckDB database.\n"
                "Select the BEST SQL candidate for the user's question.\n"
                "Return ONLY valid JSON: {\"selected_index\": <int 1-based>, \"score\": <0-1>, \"reason\": \"...\"}\n\n"
                "Ranking criteria (in order of importance):\n"
                "1. Execution success (FAILED candidates rank lower).\n"
                "2. Result plausibility (row count should make sense).\n"
                "3. Correctness (does it answer the actual question?).\n"
                "4. Schema faithfulness (correct tables / columns / joins).\n\n"
                f"Conversation:\n{history_text}\nPlan: {plan_text}\n\n"
                f"Schema:\n{schema_text}\n\nCandidates:\n{cand_block}"
            )
            resp = self._chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": question},
                ],
            )
            parsed = _safe_json(_safe_llm_content(resp))
            idx = int(parsed.get("selected_index", 1)) if parsed else 1
            idx = max(1, min(len(candidates), idx))
            return candidates[idx - 1], {
                "selected_index": idx,
                "score": float(parsed.get("score", 0.75)) if parsed else 0.75,
                "reason": str(parsed.get("reason", "Best candidate")) if parsed else "Best candidate",
            }
        except Exception as e:
            print(f"Critic error: {e}")
            # Fall back: prefer first candidate that executed successfully
            if candidate_exec:
                for i, (sql, ok, *_) in enumerate(candidate_exec):
                    if ok:
                        return sql, {
                            "selected_index": i + 1,
                            "score": 0.6,
                            "reason": "First successful candidate (critic unavailable)",
                        }
            return candidates[0], {"selected_index": 1, "score": 0.5, "reason": "Critic unavailable"}

    def _run_refiner(
        self, sql: str, on_attempt: Callable[[dict], None] | None = None,
    ) -> tuple[str, bool, list[dict]]:
        """Refiner: execute SQL, fix errors via LLM, retry up to max_retries."""

        def normalize(s: str) -> str:
            return (s or "").strip()

        def is_fallback(s: str) -> bool:
            return normalize(s).upper() == FALLBACK_SQL.upper()

        current_sql = normalize(sql)
        attempts: list[dict] = []

        if not current_sql:
            a = {"attempt": 1, "sql": FALLBACK_SQL, "success": False, "error": "Empty SQL"}
            attempts.append(a)
            if on_attempt:
                on_attempt({"step": "refiner_attempt", **a})
            return (FALLBACK_SQL, False, attempts)

        for attempt in range(self._max_refiner_retries):
            ok, error = _execute_sql(self.db_path, current_sql)

            # Never accept the fallback query as a success
            if ok and is_fallback(current_sql):
                ok = False
                error = "Fallback query rejected."

            a = {
                "attempt": attempt + 1,
                "sql": current_sql,
                "success": ok,
                "error": None if ok else error,
            }
            attempts.append(a)
            if on_attempt:
                on_attempt({"step": "refiner_attempt", **a})

            if ok:
                return (current_sql, True, attempts)
            if attempt + 1 >= self._max_refiner_retries:
                return (current_sql, False, attempts)

            # Parse error and ask LLM to fix
            if error and ":" in error:
                error_type, _, error_msg = error.partition(":")
                error_type, error_msg = error_type.strip(), error_msg.strip()
            else:
                error_type, error_msg = "Error", error or "Unknown error"

            fixed = self._refiner_fix(current_sql, error_type, error_msg)
            fixed = normalize(fixed) if fixed else ""
            if not fixed or normalize(fixed) == normalize(current_sql):
                return (current_sql, False, attempts)
            current_sql = fixed

        return (current_sql, False, attempts)

    def _refiner_fix(self, wrong_sql: str, error_type: str, error_message: str) -> str:
        """Ask LLM to fix a SQL query that produced an execution error."""
        try:
            schema_text = _format_schema(self.schema)
            fk_text = _format_foreign_keys(self.foreign_keys)
            system = (
                "You are a SQL fixer for DuckDB. The query below failed.\n\n"
                f"Error type: {error_type}\n"
                f"Error message: {error_message}\n\n"
                "Common fixes:\n"
                "- Wrong table/column name → check schema below.\n"
                "- Missing JOIN condition → check foreign keys below.\n"
                "- Use LIMIT (not TOP) in DuckDB.\n"
                "- Ambiguous column → qualify with table alias.\n"
                "- Type mismatch → use CAST(... AS type).\n\n"
                "Output ONLY the corrected SQL. No explanations, no markdown, no semicolons.\n\n"
                f"{fk_text}\n\nSchema:\n{schema_text}"
            )
            resp = self._chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": f"Wrong SQL:\n{wrong_sql}"},
                ],
            )
            return _extract_sql(_safe_llm_content(resp))
        except Exception as e:
            print(f"Refiner fix error: {e}")
            return ""

    def _run_verifier(
        self,
        question: str,
        sql: str,
        schema_subset: dict,
        plan: dict,
        history: list[dict],
        result_columns: list[str] | None = None,
        result_rows: list[list] | None = None,
        result_count: int = 0,
    ) -> dict:
        """Verifier: semantic check using actual execution results."""
        try:
            schema_text = _format_schema(schema_subset)
            fk_text = _format_foreign_keys(self.foreign_keys, set(schema_subset.keys()))
            history_text = _format_conversation(history)
            plan_text = json.dumps(plan or {})

            # Format execution results for the LLM
            result_text = "(query not executed)"
            if result_columns is not None:
                result_text = f"Columns: {result_columns}\nTotal rows: {result_count}"
                if result_rows:
                    result_text += f"\nFirst rows: {result_rows[:3]}"

            system = (
                "You are a SQL Verifier for a DuckDB database.\n"
                "Given a question, the generated SQL, and its actual execution results, "
                "decide if the SQL correctly answers the question.\n\n"
                "Return ONLY valid JSON: "
                "{\"passed\": true/false, \"reason\": \"...\", \"suggested_sql\": \"...\"}\n"
                "(suggested_sql should be empty string if passed is true)\n\n"
                "Check:\n"
                "1. Does the SQL query the right tables?\n"
                "2. Are JOIN conditions correct?\n"
                "3. Does SELECT return the right columns for the question?\n"
                "4. Are WHERE / GROUP BY / HAVING appropriate?\n"
                "5. Does the result data look plausible?\n\n"
                f"Conversation:\n{history_text}\nPlan: {plan_text}\n\n"
                f"{fk_text}\n\nSchema:\n{schema_text}"
            )
            resp = self._chat(
                messages=[
                    {"role": "system", "content": system},
                    {
                        "role": "user",
                        "content": (
                            f"Question: {question}\n"
                            f"SQL: {sql}\n\n"
                            f"Execution results:\n{result_text}"
                        ),
                    },
                ],
            )
            parsed = _safe_json(_safe_llm_content(resp))
            if not parsed:
                return {"passed": True, "reason": "Verifier unavailable", "suggested_sql": ""}
            return {
                "passed": bool(parsed.get("passed", True)),
                "reason": str(parsed.get("reason", "")),
                "suggested_sql": _extract_sql(str(parsed.get("suggested_sql", "") or "")),
            }
        except Exception as e:
            print(f"Verifier error: {e}")
            return {"passed": True, "reason": "Verifier error", "suggested_sql": ""}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _refresh_schema_context(self) -> None:
        """Recompute schema-derived context (sample rows + FK graph).

        Foreign keys are loaded from foreign_keys.json if it exists,
        otherwise resolved from the database and saved to that file.
        """
        self._sample_data = {}
        for table in self.schema:
            self._sample_data[table] = _get_sample_data(self.db_path, table, n=3)

        # Try loading cached foreign keys first
        if os.path.isfile(_FK_JSON_PATH):
            try:
                with open(_FK_JSON_PATH, "r") as f:
                    raw = json.load(f)
                self.foreign_keys = [(fk["from"], fk["to"]) for fk in raw]
                return
            except Exception:
                pass

        # Resolve and persist
        self.foreign_keys = _resolve_foreign_keys(self.db_path, self.schema)
        fk_data = [{"from": fk_from, "to": fk_to} for fk_from, fk_to in self.foreign_keys]
        with open(_FK_JSON_PATH, "w") as f:
            json.dump(fk_data, f, indent=2)

    def _validate_tables(self, table_names: list[str]) -> list[str]:
        """Return only tables that actually exist in the DuckDB catalog."""
        if not table_names:
            return []
        server_tables = _get_tables_from_server(self.db_path)
        if not server_tables:
            return table_names
        return [t for t in table_names if t in server_tables]

    def _trim_history(self, turns: list[dict] | None) -> list[dict]:
        """Normalize and trim conversation history."""
        if not turns:
            return []
        out: list[dict] = []
        for t in turns[-self._history_turns :]:
            if not isinstance(t, dict):
                continue
            q = str(t.get("prompt") or "").strip()
            s = str(t.get("sql") or "").strip()
            if q or s:
                out.append({"prompt": q[:400], "sql": s[:600]})
        return out
