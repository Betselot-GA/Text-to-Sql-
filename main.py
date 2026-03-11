"""
SQL Query Writer Agent - Main Entry Point

This file provides an interactive interface for testing your QueryWriter agent.
Your agent implementation should be in agent.py.

Usage:
    python main.py

LLM Configuration:
    Set these environment variables to configure Ollama:
    - OLLAMA_HOST: Ollama server URL (default: http://localhost:11434)
    - OLLAMA_MODEL: Model to use (default: llama3.2)
"""

import os
import sys
import threading

import duckdb
from db.dataset import resolve_db_path
from agent import QueryWriter


def execute_query(sql: str, db_path: str | None = None):
    """
    Execute a SQL query against the DuckDB database.

    Args:
        sql (str): The SQL query to execute.
        db_path (str): Path to the DuckDB database.

    Returns:
        list: Query results as a list of tuples.
    """
    resolved = resolve_db_path(db_path)
    con = duckdb.connect(database=resolved, read_only=True)
    try:
        result = con.execute(sql).fetchall()
        return result
    finally:
        con.close()


class _StageSpinner:
    """
    Simple CLI spinner that shows an animated three-dot suffix next to the
    current pipeline stage name (e.g. 'Selector', 'Decomposer', 'Refiner').
    """

    def __init__(self, label: str):
        self.label = label
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return

        def run():
            dots = ["", ".", "..", "..."]
            i = 0
            while not self._stop.is_set():
                msg = f"\r{self.label}{dots[i % len(dots)]}   "
                sys.stdout.write(msg)
                sys.stdout.flush()
                i += 1
                # Check stop about 3 times a second
                if self._stop.wait(0.3):
                    break
            # Clear the line when done
            sys.stdout.write("\r" + " " * 80 + "\r")
            sys.stdout.flush()

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        self._thread = None


def main():
    """
    Main function to run the SQL Query Writer Agent interactively.
    """
    db_path = resolve_db_path()

    # Initialize the QueryWriter agent (also initializes the DB)
    print("Initializing QueryWriter agent...")
    agent = QueryWriter(db_path=db_path)

    # Show configuration
    print("\n" + "=" * 60)
    print("SQL Query Writer Agent - Interactive Mode")
    print("=" * 60)
    print(f"\nOllama Host: {os.getenv('OLLAMA_HOST', 'http://localhost:11434')}")
    print(f"Model: {os.getenv('OLLAMA_MODEL', 'llama3.2')}")
    print(f"Database Path: {db_path}")
    print("\nDatabase loaded with the following tables:")
    for table_name in agent.schema.keys():
        print(f"  - {table_name}")
    print("\nType 'quit' or 'exit' to stop the agent.")
    print("=" * 60 + "\n")

    # Main interaction loop
    while True:
        try:
            # Get user input
            user_query = input("\nEnter your question: ").strip()

            if user_query.lower() in ['quit', 'exit', 'q']:
                print("Goodbye!")
                break

            if not user_query:
                continue

            # Generate SQL from natural language using the QueryWriter
            print("\nGenerating SQL query...")
            # Spinner starts on the first stage name; updated as we progress.
            spinner = _StageSpinner("Selector")
            spinner.start()

            # Show pipeline progress (Selector → Decomposer → Refiner) so the user
            # can see that the agent is working and not stuck.
            def on_step(step: dict) -> None:
                nonlocal spinner
                kind = step.get("step")
                # Stop the current spinner so the previous "dots" line disappears
                spinner.stop()
                if kind == "selector":
                    tables = step.get("tables") or []
                    msg = step.get("message") or ""
                    tables_str = ", ".join(tables) if tables else "none"
                    print(f"\n  [Selector] tables: {tables_str} {msg}")
                    # Next stage: Decomposer
                    spinner = _StageSpinner("Decomposer")
                    spinner.start()
                elif kind == "decomposer":
                    msg = step.get("message") or ""
                    print("\n  [Decomposer] produced initial SQL." + (f" {msg}" if msg else ""))
                    # Next stage: Refiner
                    spinner = _StageSpinner("Refiner")
                    spinner.start()
                elif kind == "refiner_attempt":
                    attempt = step.get("attempt")
                    success = step.get("success")
                    err = step.get("error") or ""
                    if success:
                        print(f"\n  [Refiner] attempt {attempt} succeeded.")
                    else:
                        print(f"\n  [Refiner] attempt {attempt} failed: {err}")
                    # Keep spinner on Refiner between attempts
                    spinner = _StageSpinner("Refiner")
                    spinner.start()
                elif kind == "refiner":
                    if step.get("success"):
                        print("\n  [Refiner] final query succeeded.")
                    else:
                        print("\n  [Refiner] could not find a successful query.")
                    # Final stage: do not restart spinner
                elif kind == "error":
                    print(f"\n  [Pipeline error] {step.get('message')}")
                    # On error, do not restart spinner

            try:
                # Use the detailed pipeline variant so we can report steps live
                sql, _steps = agent.generate_query_with_steps(user_query, on_step=on_step)
            finally:
                spinner.stop()
            print(f"\nGenerated SQL:\n{sql}")

            # Execute the query
            print("\nExecuting query...")
            results = execute_query(sql, db_path)

            # Display results
            print(f"\nResults ({len(results)} rows):")
            for row in results[:10]:  # Show first 10 rows
                print(row)
            if len(results) > 10:
                print(f"... and {len(results) - 10} more rows")

        except NotImplementedError as e:
            print(f"\nError: {e}")
            print("Please implement the generate_query method in agent.py!")
        except Exception as e:
            print(f"\nError: {e}")


if __name__ == "__main__":
    main()
