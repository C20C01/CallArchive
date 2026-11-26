"""Structured process events and an optional local operations log."""

import json
import logging
import re
import sqlite3
from contextlib import closing, contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path

from .configuration import LoggingSettings
from .sqlite import enable_wal

LOG = logging.getLogger("callarchive.runtime")
EVENT_COMPONENT = ContextVar("event_component", default=None)
LOG.addHandler(logging.NullHandler())
LOG.propagate = False
STR_FIELDS = frozenset({"recording_id", "job_id", "message_id", "status", "reason", "report_day", "scan_through", "task"})
INT_FIELDS = frozenset({"imported", "pending", "confirmed", "rejected", "unknown", "page", "total_pages", "calls_returned"})
TOKEN = re.compile(r"[A-Za-z0-9_.:-]{1,128}")


@contextmanager
def logged_as(component):
    stamp = EVENT_COMPONENT.set(component)
    try:
        yield
    finally:
        EVENT_COMPONENT.reset(stamp)


def event(name: str, *, level="INFO", **fields):
    if not TOKEN.fullmatch(name):
        raise ValueError("Invalid operational event name")
    safe = {key: value for key, value in fields.items()
            if key in STR_FIELDS and isinstance(value, str) and TOKEN.fullmatch(value)}
    safe.update({key: value for key, value in fields.items() if key in INT_FIELDS and type(value) is int})
    LOG.log(logging.getLevelName(level), name, extra={"event_fields": safe, "event_component": EVENT_COMPONENT.get()})


class RuntimeLog:
    def __init__(self, database: Path, *, retention_days=7):
        self.database = database
        self.retention_days = retention_days
        database.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as db:
            enable_wal(db)
            db.executescript("""
                CREATE TABLE IF NOT EXISTS runtime_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    component TEXT NOT NULL,
                    recording_id TEXT,
                    job_id TEXT,
                    message_id TEXT,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS runtime_recording ON runtime_events(recording_id, sequence);
                CREATE INDEX IF NOT EXISTS runtime_job ON runtime_events(job_id, sequence);
                CREATE INDEX IF NOT EXISTS runtime_message ON runtime_events(message_id, sequence);
                CREATE INDEX IF NOT EXISTS runtime_time ON runtime_events(julianday(json_extract(payload,'$.time')));
            """)
            with db:
                self.purge(db)

    def purge(self, db):
        db.execute("DELETE FROM runtime_events WHERE julianday(json_extract(payload,'$.time'))<julianday('now')-?",
                   (self.retention_days,))

    def connect(self):
        db = sqlite3.connect(self.database, timeout=1)
        db.row_factory = sqlite3.Row
        return db

    def append(self, payload, serialized):
        with closing(self.connect()) as db, db:
            db.execute(
                """INSERT INTO runtime_events (component, recording_id, job_id, message_id, payload)
                   VALUES (?,?,?,?,?)""",
                (payload["component"], payload.get("recording_id"), payload.get("job_id"),
                 payload.get("message_id"), serialized),
            )
            self.purge(db)

    def list(self, *, related_id=None, component=None, before=None, limit=50, important_only=False,
             attention_only=False):
        with closing(self.connect()) as db:
            rows = db.execute(
                """SELECT sequence, payload FROM runtime_events
                   WHERE julianday(json_extract(payload,'$.time'))>=julianday('now')-?
                     AND (? IS NULL OR component=?) AND (? IS NULL OR sequence<?)
                         AND (? IS NULL OR recording_id=? OR job_id=? OR message_id=?)
                     AND coalesce(json_extract(payload, '$.event'), '') NOT IN ('report_finished', 'alerts_finished')
                     AND (? = 0 OR (
                       json_extract(payload, '$.level') IN ('WARNING', 'ERROR', 'CRITICAL')
                       OR json_extract(payload, '$.status') IN ('needs_review', 'failed', 'unknown', 'rejected')
                       OR (json_extract(payload, '$.event') = 'outbound_import_finished'
                           AND coalesce(json_extract(payload, '$.pending'), 0) > 0)))
                     AND (? = 0 OR CASE
                       WHEN json_extract(payload, '$.level') IN ('WARNING', 'ERROR', 'CRITICAL')
                            OR nullif(json_extract(payload, '$.reason'), '') IS NOT NULL THEN 1
                       WHEN json_extract(payload, '$.status') IN
                            ('archived', 'succeeded', 'needs_input', 'needs_review', 'failed', 'unknown', 'rejected') THEN 1
                       WHEN json_extract(payload, '$.event') = 'outbound_import_finished' THEN
                            coalesce(json_extract(payload, '$.pending'), 0) > 0 OR
                            coalesce(json_extract(payload, '$.imported'), 0) > 0
                       WHEN json_extract(payload, '$.event') IN
                            ('process_started', 'transcription_accepted', 'transcription_started') THEN 0
                       WHEN json_extract(payload, '$.event') = 'archive_stage'
                            AND json_extract(payload, '$.status') IN
                            ('queued', 'transcribing', 'acknowledging', 'delivering') THEN 0
                       ELSE 1 END)
                   ORDER BY sequence DESC LIMIT ?""",
                (self.retention_days, component, component, before, before, related_id, related_id, related_id, related_id,
                 attention_only, important_only, limit + 1),
            ).fetchall()
        return {
            "items": [{"sequence": row["sequence"], **json.loads(row["payload"])} for row in rows[:limit]],
            "next_cursor": rows[limit - 1]["sequence"] if len(rows) > limit else None,
        }


class EventHandler(logging.StreamHandler):
    def __init__(self, component, style, local, stream):
        super().__init__(stream)
        self.component = component
        self.style = style
        self.local = local

    def emit(self, record):
        component = getattr(record, "event_component", None) or self.component
        payload = {
            "time": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname, "component": component, "event": record.getMessage(),
            **record.event_fields,
        }
        serialized = json.dumps(payload, ensure_ascii=False)
        output = serialized
        if self.style == "text":
            details = " ".join(f"{key}={value}" for key, value in record.event_fields.items())
            output = f'{payload["time"]} {record.levelname} {component} {payload["event"]} {details}'.rstrip()
        try:
            self.stream.write(output + self.terminator)
            self.flush()
        except (OSError, ValueError):
            pass
        if self.local:
            try:
                self.local.append(payload, serialized)
            except sqlite3.Error:
                # The console remains available if the optional operations database cannot accept writes.
                pass


def configure_logging(settings: LoggingSettings, component: str, *, stream=None) -> RuntimeLog | None:
    if not TOKEN.fullmatch(component):
        raise ValueError("Invalid component")
    for handler in LOG.handlers[:]:
        LOG.removeHandler(handler)
        handler.close()
    LOG.setLevel(settings.level)
    local = None
    failure = None
    if settings.database:
        try:
            local = RuntimeLog(settings.database, retention_days=settings.retention_days)
        except (OSError, sqlite3.Error) as error:
            failure = type(error).__name__
    LOG.addHandler(EventHandler(component, settings.format, local, stream))
    if failure:
        event("runtime_log_unavailable", level="ERROR", reason=failure)
    return local
