"""
SQL Query Writer Agent

Multi-agent pipeline: Selector → Decomposer → Refiner.
Uses the database server (DuckDB or optional Microsoft SQL Server) for schema and execution.
- When SQL_SERVER_CONNECTION_STRING is set: schema and all SQL commands come from SQL Server.
- Otherwise: DuckDB (bike_store.db) is used for schema and execution.
"""

import os
import re
from typing import Callable

import duckdb
from db.bike_store import get_schema_info

try:
    import pyodbc
except ImportError:
    pyodbc = None  # type: ignore[assignment]

# Max prompt length to avoid token overflow (characters)
_MAX_PROMPT_LENGTH = 4000

# Fallback when the pipeline cannot produce a real query (do not treat as success in Refiner)
FALLBACK_SQL = "SELECT NULL WHERE FALSE"


# ---------------------------------------------------------------------------
# Microsoft SQL Server: schema and execution (when configured)
# ---------------------------------------------------------------------------

def _get_sql_server_schema(conn_string: str) -> dict:
    """
    Get full schema (tables + columns) from SQL Server. Used for text-to-SQL so the
    agent has all table/column "commands" from the server.
    Returns same shape as get_schema_info: { table_name: [ {name, type}, ... ] }.
    """
    if not pyodbc or not (conn_string or "").strip():
        return {}
    conn_string = conn_string.strip()
    con = None
    try:
        con = pyodbc.connect(conn_string)
        cur = con.cursor()
        cur.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        tables = cur.fetchall()
        schema = {}
        for schema_name, table_name in tables:
            key = table_name if (schema_name or "").strip().lower() == "dbo" else f"{schema_name}.{table_name}"
            cur.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, (schema_name, table_name))
            cols = cur.fetchall()
            schema[key] = [{"name": c[0], "type": c[1] or "?"} for c in cols]
        return schema
    except Exception:
        return {}
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _execute_sql_server(conn_string: str, sql: str) -> tuple[bool, str | None]:
    """
    Execute SQL on Microsoft SQL Server. Returns (True, None) on success,
    (False, error_message) on failure.
    """
    if not pyodbc or not (conn_string or "").strip():
        return (False, "SQL Server not configured or pyodbc not installed")
    if not (sql or "").strip():
        return (False, "Empty SQL")
    sql = sql.strip()
    con = None
    try:
        con = pyodbc.connect(conn_string)
        cur = con.cursor()
        cur.execute(sql)
        return (True, None)
    except Exception as e:
        return (False, f"{type(e).__name__}: {str(e)}")
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _get_tables_from_sql_server(conn_string: str) -> set[str]:
    """Get list of table names from SQL Server (schema.table or table)."""
    if not pyodbc or not (conn_string or "").strip():
        return set()
    con = None
    try:
        con = pyodbc.connect(conn_string)
        cur = con.cursor()
        cur.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        rows = cur.fetchall()
        return {(r[1] if (r[0] or "").strip().lower() == "dbo" else f"{r[0]}.{r[1]}") for r in rows}
    except Exception:
        return set()
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# DuckDB: single place for all database execution and validation
# ---------------------------------------------------------------------------

def _execute_sql(db_path: str, sql: str, read_only: bool = True) -> tuple[bool, str | None]:
    """
    Execute SQL against the database server. Centralizes connection handling and
    error reporting so the pipeline always uses real execution feedback.

    Args:
        db_path: Path to the DuckDB database file.
        sql: SQL query to execute.
        read_only: Use read-only connection (default True).

    Returns:
        (True, None) if execution succeeded.
        (False, error_message) if execution failed; error_message is the server's error.
    """
    if not sql or not (sql or "").strip():
        return (False, "Empty SQL")
    sql = sql.strip()
    con = None
    try:
        con = duckdb.connect(database=db_path, read_only=read_only)
        con.execute(sql)
        return (True, None)
    except Exception as e:
        return (False, f"{type(e).__name__}: {str(e)}")
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _table_exists(db_path: str, table_name: str) -> bool:
    """
    Check whether a table exists and is readable on the database server.
    Uses a minimal query to avoid schema/cache drift.
    """
    if not table_name or not re.match(r"^[a-zA-Z0-9_]+$", table_name):
        return False
    ok, _ = _execute_sql(db_path, f"SELECT 1 FROM {table_name} LIMIT 1", read_only=True)
    return ok


def _get_tables_from_server(db_path: str) -> set[str]:
    """
    Get the list of tables that actually exist on the database server.
    More robust than relying only on cached schema.
    """
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
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def get_ollama_client():
    """
    Get Ollama client configured for either Carleton server or local instance.

    Set OLLAMA_HOST environment variable to use Carleton's LLM server.
    Defaults to local Ollama instance.
    """
    import ollama
    host = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
    return ollama.Client(host=host)


def get_model_name():
    """
    Get the model name from environment or use default.

    Set OLLAMA_MODEL environment variable to specify which model to use.
    """
    return os.getenv('OLLAMA_MODEL', 'llama3.2')


def _format_schema_dict(schema: dict) -> str:
    """Format a schema dict (table name -> columns) as a string for prompts."""
    if not schema:
        return ""
    parts = []
    for table_name, columns in (schema or {}).items():
        if not isinstance(columns, (list, tuple)):
            continue
        col_strs = []
        for col in columns:
            if isinstance(col, dict) and "name" in col:
                name = col.get("name", "?")
                dtype = col.get("type", "?")
                col_strs.append(f"{name} ({dtype})")
        if col_strs:
            parts.append(f"Table {table_name}: {', '.join(col_strs)}")
    return "\n".join(parts)


def _extract_sql_from_response(text: str) -> str:
    """Extract a single SQL query from LLM response (strip markdown, leading text, semicolons)."""
    if not text or not isinstance(text, str):
        return ""
    text = text.strip()
    if not text:
        return ""
    # Find first SELECT (case-insensitive) to drop any leading explanation
    if "SELECT" in text.upper():
        idx = text.upper().index("SELECT")
        text = text[idx:]
    for prefix in ("```sql", "```"):
        if text.startswith(prefix):
            text = text[len(prefix):].strip()
    if text.endswith("```"):
        text = text[:-3].strip()
    text = text.rstrip()
    if text.endswith(";"):
        text = text[:-1].strip()
    return text.strip()


def _parse_selector_tables(response: str, all_tables: set) -> list:
    """
    Parse Selector agent output into a list of table names.
    Tries: comma/semicolon/newline-separated list, then word-boundary match for table names.
    Falls back to all_tables if unparseable.
    """
    if not response or not all_tables:
        return list(all_tables)
    response_upper = response.strip().upper()
    tables_upper = {t.upper(): t for t in all_tables}

    # Try comma/semicolon/newline-separated list
    parts = [p.strip() for p in re.split(r"[,;\n]+", response) if p.strip()]
    found = [p.upper() for p in parts if p.upper() in tables_upper]
    if found:
        # Preserve order, deduplicate by first occurrence
        seen = set()
        result = []
        for p in found:
            if p not in seen:
                seen.add(p)
                result.append(tables_upper[p])
        return result

    # Try finding table names as whole words in the response
    found = []
    for upper_name, original_name in tables_upper.items():
        if re.search(r"\b" + re.escape(upper_name) + r"\b", response_upper):
            found.append(original_name)
    if found:
        return found

    return list(all_tables)


def _safe_llm_content(response: dict) -> str:
    """Extract content from Ollama chat response; return empty string if missing or invalid."""
    if not response:
        return ""
    msg = response.get("message")
    if not msg:
        return ""
    content = msg.get("content")
    if content is None:
        return ""
    return str(content).strip()


class QueryWriter:
    """
    SQL Query Writer Agent using a Selector → Decomposer → Refiner pipeline.

    - Selector: identifies relevant database tables for the user question.
    - Decomposer: produces initial SQL (with subqueries if needed).
    - Refiner: executes SQL, catches errors, and fixes the query.
    """

    def __init__(self, db_path: str = 'bike_store.db'):
        """
        Initialize the QueryWriter.

        Args:
            db_path (str): Path to the DuckDB database file (used when SQL Server is not configured).
        """
        self.db_path = str(db_path).strip() if db_path else "bike_store.db"
        self._sql_server_conn = (os.getenv("SQL_SERVER_CONNECTION_STRING") or "").strip()
        self._use_sql_server = bool(pyodbc and self._sql_server_conn)

        if self._use_sql_server:
            self.schema = _get_sql_server_schema(self._sql_server_conn) or {}
        else:
            try:
                self.schema = get_schema_info(db_path=self.db_path) or {}
            except Exception:
                self.schema = {}
        self.client = get_ollama_client()
        self.model = get_model_name()
        self._max_refiner_retries = 3

    def generate_query(self, prompt: str) -> str:
        """
        Generate a SQL query from a natural language prompt using the
        Selector → Decomposer → Refiner pipeline.

        Args:
            prompt (str): The natural language question from the user.

        Returns:
            str: A valid SQL query that answers the question.
        """
        prompt = (prompt or "").strip()
        if not prompt:
            return FALLBACK_SQL
        if len(prompt) > _MAX_PROMPT_LENGTH:
            prompt = prompt[:_MAX_PROMPT_LENGTH] + "..."

        # Refresh schema from server if we had none at init
        if not self.schema:
            if self._use_sql_server:
                self.schema = _get_sql_server_schema(self._sql_server_conn) or {}
            else:
                try:
                    self.schema = get_schema_info(db_path=self.db_path) or {}
                except Exception:
                    pass
        if not self.schema:
            return FALLBACK_SQL

        try:
            # 1. Selector: which tables are relevant?
            relevant_tables = self._run_selector(prompt)
            if not relevant_tables:
                relevant_tables = list(self.schema.keys())
            # Validate against the database server: only use tables that exist
            relevant_tables = self._validate_tables_against_server(relevant_tables)
            if not relevant_tables:
                relevant_tables = list(self.schema.keys())
            schema_subset = {t: self.schema[t] for t in relevant_tables if t in self.schema}
            if not schema_subset:
                return FALLBACK_SQL

            # 2. Decomposer: build initial SQL (possibly with subqueries)
            initial_sql = self._run_decomposer(prompt, schema_subset)
            if not initial_sql or "SELECT" not in initial_sql.upper():
                return FALLBACK_SQL

            # 3. Refiner: execute and fix until valid or max retries
            final_sql, executed_ok, _ = self._run_refiner(initial_sql)
            if not executed_ok:
                return FALLBACK_SQL
            return final_sql.strip()

        except Exception as e:
            print(f"Error in pipeline: {e}")
            return FALLBACK_SQL

    def generate_query_with_steps(
        self, prompt: str, on_step: Callable[[dict], None] | None = None
    ) -> tuple[str, list[dict]]:
        """
        Same as generate_query but also returns pipeline steps for the UI.
        Returns (final_sql, steps) where steps is a list of {"step", ...} dicts.
        If on_step is provided, it is called with each step dict as it completes (for live UI).
        """
        def emit(s: dict) -> None:
            if on_step:
                on_step(s)

        steps: list[dict] = []
        prompt_clean = (prompt or "").strip()
        if not prompt_clean:
            err = {"step": "error", "message": "Empty prompt"}
            steps.append(err)
            emit(err)
            return (FALLBACK_SQL, steps)
        if len(prompt_clean) > _MAX_PROMPT_LENGTH:
            prompt_clean = prompt_clean[:_MAX_PROMPT_LENGTH] + "..."

        if not self.schema:
            if self._use_sql_server:
                self.schema = _get_sql_server_schema(self._sql_server_conn) or {}
            else:
                try:
                    self.schema = get_schema_info(db_path=self.db_path) or {}
                except Exception:
                    pass
        if not self.schema:
            err = {"step": "error", "message": "No database schema loaded. Ensure the database exists."}
            steps.append(err)
            emit(err)
            return (FALLBACK_SQL, steps)

        try:
            # 1. Selector
            relevant_tables = self._run_selector(prompt_clean)
            if not relevant_tables:
                relevant_tables = list(self.schema.keys())
            relevant_tables = self._validate_tables_against_server(relevant_tables)
            if not relevant_tables:
                relevant_tables = list(self.schema.keys())
            schema_subset = {t: self.schema[t] for t in relevant_tables if t in self.schema}
            if not schema_subset:
                s = {"step": "selector", "tables": [], "message": "No valid tables after validation"}
                steps.append(s)
                emit(s)
                return (FALLBACK_SQL, steps)
            s = {"step": "selector", "tables": list(schema_subset.keys())}
            steps.append(s)
            emit(s)

            # 2. Decomposer
            initial_sql = self._run_decomposer(prompt_clean, schema_subset)
            if not initial_sql or "SELECT" not in initial_sql.upper():
                s = {"step": "decomposer", "sql": initial_sql or "(empty)", "message": "Decomposer returned no valid SQL"}
                steps.append(s)
                emit(s)
                return (FALLBACK_SQL, steps)
            s = {"step": "decomposer", "sql": initial_sql}
            steps.append(s)
            emit(s)

            # 3. Refiner (with per-attempt callbacks for live UI)
            final_sql, executed_ok, refiner_attempts = self._run_refiner(initial_sql, on_attempt=emit)
            s = {"step": "refiner", "attempts": refiner_attempts, "success": executed_ok}
            steps.append(s)
            emit(s)
            if not executed_ok:
                return (FALLBACK_SQL, steps)
            return (final_sql.strip(), steps)
        except Exception as e:
            err = {"step": "error", "message": str(e)}
            steps.append(err)
            emit(err)
            return (FALLBACK_SQL, steps)

    def _run_selector(self, question: str) -> list:
        """
        Selector Agent: given user question and full schema, return list of relevant table names.
        On failure returns all tables so the pipeline can continue.
        """
        all_tables = set(self.schema.keys())
        if not all_tables:
            return []

        try:
            schema_text = _format_schema_dict(self.schema)
            system_prompt = f"""You are a database schema Selector. Given a user question and the full database schema, you must output ONLY the names of tables that are RELEVANT to answering the question. Exclude tables that are not needed.

Output rules:
- Output ONLY a comma-separated list of table names (e.g. products,orders,order_items).
- Use exact table names from the schema below. No explanations, no markdown, no other text.

Schema:
{schema_text}
"""
            response = self.client.chat(
                model=self.model,
                messages=[
                    {'role': 'system', 'content': system_prompt},
                    {'role': 'user', 'content': question}
                ]
            )
            raw = _safe_llm_content(response)
            return _parse_selector_tables(raw, all_tables)
        except Exception as e:
            print(f"Selector error: {e}")
            return list(all_tables)

    def _validate_tables_against_server(self, table_names: list[str]) -> list[str]:
        """
        Return only table names that exist on the database server (DuckDB or SQL Server).
        Uses the server's catalog so we never pass invalid tables to Decomposer.
        """
        if not table_names:
            return []
        if self._use_sql_server:
            server_tables = _get_tables_from_sql_server(self._sql_server_conn)
        else:
            server_tables = _get_tables_from_server(self.db_path)
        if not server_tables:
            return table_names  # Server unreachable; keep list as-is
        return [t for t in table_names if t in server_tables]

    def _run_decomposer(self, question: str, schema_subset: dict) -> str:
        """
        Decomposer Agent: given user question and relevant schema, produce a single SQL query.
        May use subqueries for complex questions (e.g. "over the average").
        Returns empty string on failure.
        """
        if not schema_subset:
            return ""

        try:
            schema_text = _format_schema_dict(schema_subset)
            if self._use_sql_server:
                engine_rules = """- Database engine: Microsoft SQL Server (T-SQL).
- For "top N", "highest", "most" use ORDER BY ... DESC and TOP N (e.g. SELECT TOP 5 ...).
- For "lowest", "least" use ORDER BY ... ASC and TOP N.
- Do NOT use LIMIT; use TOP N in the SELECT list.
- Use explicit JOINs; avoid SELECT *."""
            else:
                engine_rules = """- Database engine: DuckDB (use standard SQL; no SQLite-specific syntax).
- For "top N", "highest", "most" use ORDER BY ... DESC LIMIT N.
- For "lowest", "least" use ORDER BY ... ASC LIMIT N.
- Use explicit JOINs; avoid SELECT *."""
            system_prompt = f"""You are a SQL Decomposer. Translate the user's question into ONE executable SQL query.

Rules:
- Use ONLY the tables and columns in the schema below.
{engine_rules}
- If the question involves "above average" or similar, use a subquery for the average.
- Output ONLY the SQL query: no explanations, no markdown, no code blocks, no semicolon.
- If the question cannot be answered with the schema, output exactly: SELECT NULL WHERE FALSE

Schema (only these tables are relevant):
{schema_text}
"""
            response = self.client.chat(
                model=self.model,
                messages=[
                    {'role': 'system', 'content': system_prompt},
                    {'role': 'user', 'content': question}
                ]
            )
            raw = _safe_llm_content(response)
            return _extract_sql_from_response(raw)
        except Exception as e:
            print(f"Decomposer error: {e}")
            return ""

    def _run_refiner(
        self, sql: str, on_attempt: Callable[[dict], None] | None = None
    ) -> tuple[str, bool, list[dict]]:
        """
        Refiner Agent: execute SQL on the database server. On error, use the server's
        error message to ask the LLM to fix the query, then retry on the server.
        Returns (final_sql, executed_ok, attempts). If on_attempt is provided, it is
        called with {"step": "refiner_attempt", "attempt": n, "sql", "success", "error"} after each attempt.
        """
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
            if self._use_sql_server:
                ok, server_error = _execute_sql_server(self._sql_server_conn, current_sql)
            else:
                ok, server_error = _execute_sql(self.db_path, current_sql, read_only=True)

            if ok and is_fallback(current_sql):
                ok = False
                server_error = "Refusing to accept fallback as success; query does not answer the question."

            a = {
                "attempt": attempt + 1,
                "sql": current_sql,
                "success": ok,
                "error": None if ok else server_error,
            }
            attempts.append(a)
            if on_attempt:
                on_attempt({"step": "refiner_attempt", **a})
            if ok:
                return (current_sql, True, attempts)
            if attempt + 1 >= self._max_refiner_retries:
                return (current_sql, False, attempts)
            # Parse server error for Refiner
            if server_error and ":" in server_error:
                error_type, _, error_msg = server_error.partition(":")
                error_type, error_msg = error_type.strip(), error_msg.strip()
            else:
                error_type, error_msg = "Error", server_error or "Unknown error"
            fixed = self._refiner_fix(current_sql, error_type, error_msg)
            fixed = normalize(fixed) if fixed else ""
            if not fixed or normalize(fixed) == normalize(current_sql):
                return (current_sql, False, attempts)
            current_sql = fixed
        return (current_sql, False, attempts)

    def _refiner_fix(self, wrong_sql: str, error_type: str, error_message: str) -> str:
        """
        Refiner fix step: send wrong SQL and error to LLM, return corrected SQL.
        Returns empty string on failure or invalid response.
        """
        try:
            schema_text = _format_schema_dict(self.schema)
            db_label = "Microsoft SQL Server (T-SQL)" if self._use_sql_server else "DuckDB"
            system_prompt = f"""You are a SQL Refiner. The following SQL query was executed against a {db_label} database and failed.

Error type: {error_type}
Error message: {error_message}

Database schema:
{schema_text}

Your task: output a CORRECTED SQL query that fixes the error. Common fixes:
- Typo in table or column names (e.g. "from" -> correct table name, wrong spelling).
- Invalid identifier quoting: use double quotes or square brackets for identifiers where needed; single quotes only for string literals.
- Syntax errors: missing JOIN conditions, extra/missing parentheses, wrong keyword order.
- For SQL Server: use TOP N not LIMIT N. Type mismatches: use CAST(... AS type) where needed.

Output ONLY the corrected SQL query. No explanations, no markdown, no semicolon.
"""
            user_content = f"Wrong SQL:\n{wrong_sql}"
            response = self.client.chat(
                model=self.model,
                messages=[
                    {'role': 'system', 'content': system_prompt},
                    {'role': 'user', 'content': user_content}
                ]
            )
            raw = _safe_llm_content(response)
            return _extract_sql_from_response(raw)
        except Exception as e:
            print(f"Refiner fix error: {e}")
            return ""
