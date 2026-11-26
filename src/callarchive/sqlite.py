"""SQLite startup operations shared across long-running components."""

import sqlite3
import time


def enable_wal(connection: sqlite3.Connection, *, wait_seconds=10):
    """Retry the journal mode change while another process opens the same database."""
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        except sqlite3.OperationalError as error:
            code = getattr(error, "sqlite_errorcode", None)
            busy = ((code & 0xff) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) if code is not None
                    else str(error) in {"database is locked", "database table is locked"})
            if not busy or time.monotonic() >= deadline:
                raise
        else:
            if mode == "wal":
                return
            if time.monotonic() >= deadline:
                raise sqlite3.OperationalError("Could not enable WAL for the database.")
        time.sleep(0.05)
