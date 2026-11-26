import hashlib
import json
import sqlite3
import time
from contextlib import closing, contextmanager
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from ..observability import event
from ..sqlite import enable_wal
from .call_state import CALL_STATE, DOWNLOAD_ATTEMPT_LIMIT, recording_retry


class SubmissionConflict(Exception):
    pass


class RemovalUnavailable(Exception):
    pass


class RetryUnavailable(Exception):
    pass


ARCHIVE_CACHE_SECONDS = 24 * 3600
ARCHIVE_HISTORY_SECONDS = 7 * 86400
OUTBOUND_ARCHIVE_CLOSURES = """
    SELECT o.owner, o.call_id, max(e.occurred_at) AS closed_at
    FROM outbound_calls o JOIN recordings r ON r.call_id=o.call_id
    LEFT JOIN recording_events e ON e.recording_id=r.id AND e.status='archived'
    WHERE o.complete=1 AND NOT EXISTS (SELECT 1 FROM outbound_recordings p
        WHERE p.call_id=o.call_id AND p.recording_id IS NULL)
    GROUP BY o.call_id
    HAVING min(r.status='archived' AND e.occurred_at IS NOT NULL)=1
"""
ARCHIVE_CACHE_ROWS = f"""
    SELECT r.id AS recording_id, coalesce(r.call_id,r.id) AS call_key,
           CASE WHEN o.call_id IS NOT NULL AND removed.call_key IS NULL
                THEN closed.closed_at ELSE max(e.occurred_at) END AS archived_at,
           removed.call_key IS NULL AS visible
    FROM recordings r JOIN recording_events e ON e.recording_id=r.id AND e.status='archived'
    LEFT JOIN outbound_calls o ON o.call_id=r.call_id
    LEFT JOIN console_call_removals removed ON removed.call_key=r.call_id
    LEFT JOIN ({OUTBOUND_ARCHIVE_CLOSURES}) closed ON closed.call_id=r.call_id
    WHERE r.status='archived' AND (o.call_id IS NULL OR removed.call_key IS NOT NULL OR closed.call_id IS NOT NULL)
    GROUP BY r.id
"""
WORKFLOW_CANDIDATES = """
    FROM recordings r LEFT JOIN workflow w ON w.recording_id = r.id
    LEFT JOIN recording_recovery recovery ON recovery.recording_id = r.id
    WHERE r.status IN ('queued', 'transcribing', 'acknowledging', 'delivering')
      AND (coalesce(w.retry_at, 0) <= :now OR (r.status='transcribing' AND w.job_id=:completed_job))
      AND coalesce(w.lease_until, 0) <= :lease_at
      AND NOT EXISTS (SELECT 1 FROM console_call_removals x WHERE x.call_key=coalesce(r.call_id,r.id))
      AND NOT EXISTS (SELECT 1 FROM outbound_short_calls s WHERE s.call_id=r.call_id)
      AND NOT EXISTS (SELECT 1 FROM outbound_recordings f WHERE f.call_id=r.call_id
          AND f.recording_id IS NULL AND (f.error IS NULL OR f.error='invalid_audio'))
"""
WORKFLOW_ORDER = " ORDER BY coalesce(w.retry_at, 0), r.rowid"


class ArchiveStore:
    def __init__(self, database: Path, *, write_guard=None):
        self.database = database
        self.write_guard = write_guard
        database.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as db:
            enable_wal(db)
            db.executescript("""
                CREATE TABLE IF NOT EXISTS calls (
                    id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    external_id TEXT,
                    UNIQUE (owner, external_id)
                );
                CREATE TABLE IF NOT EXISTS recordings (
                    id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    call_id TEXT REFERENCES calls(id),
                    metadata TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    audio BLOB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    UNIQUE (owner, request_id)
                );
                CREATE TABLE IF NOT EXISTS workflow (
                    recording_id TEXT PRIMARY KEY REFERENCES recordings(id),
                    retry_at REAL NOT NULL DEFAULT 0,
                    lease_until REAL NOT NULL DEFAULT 0,
                    lease_token TEXT,
                    job_id TEXT,
                    transcript TEXT,
                    receipts TEXT,
                    error TEXT,
                    transcription_failures INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS recording_events (
                    sequence INTEGER PRIMARY KEY,
                    recording_id TEXT NOT NULL REFERENCES recordings(id),
                    occurred_at REAL NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    actor TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS recording_event_order
                    ON recording_events (recording_id, sequence);
                CREATE INDEX IF NOT EXISTS recording_archived_time
                    ON recording_events (occurred_at, recording_id) WHERE status='archived';
                CREATE TABLE IF NOT EXISTS recording_recovery (
                    recording_id TEXT PRIMARY KEY REFERENCES recordings(id),
                    resume_status TEXT,
                    retry_id TEXT,
                    generation INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS recording_removals (
                    recording_id TEXT PRIMARY KEY REFERENCES recordings(id),
                    previous_status TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS recording_media (
                    recording_id TEXT PRIMARY KEY REFERENCES recordings(id),
                    duration_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS remote_attachments (
                    recording_id TEXT NOT NULL REFERENCES recordings(id),
                    target TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    request_id TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'pending',
                    receipt TEXT,
                    filename_stem TEXT,
                    retry_at REAL DEFAULT 0,
                    UNIQUE (recording_id, target, kind)
                );
                CREATE TABLE IF NOT EXISTS outbound_windows (
                    owner TEXT NOT NULL,
                    source TEXT NOT NULL,
                    starts TEXT NOT NULL,
                    ends TEXT NOT NULL,
                    cursor TEXT,
                    done INTEGER NOT NULL,
                    UNIQUE (owner, source, starts, ends)
                );
                CREATE TABLE IF NOT EXISTS outbound_calls (
                    owner TEXT NOT NULL,
                    source TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    call_id TEXT NOT NULL UNIQUE REFERENCES calls(id),
                    metadata TEXT NOT NULL,
                    complete INTEGER NOT NULL,
                    recordings_checked_at INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (owner, source, external_id)
                );
                CREATE TABLE IF NOT EXISTS outbound_recordings (
                    call_id TEXT NOT NULL REFERENCES outbound_calls(call_id),
                    external_id TEXT NOT NULL,
                    reference TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    recording_id TEXT REFERENCES recordings(id),
                    error TEXT,
                    download_attempts INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (call_id, external_id)
                );
                CREATE TABLE IF NOT EXISTS outbound_call_aliases (
                    owner TEXT NOT NULL,
                    source TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    call_id TEXT NOT NULL REFERENCES outbound_calls(call_id),
                    PRIMARY KEY(owner, source, identifier)
                );
                CREATE TABLE IF NOT EXISTS outbound_cleanup_cutoff (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    cutoff_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS outbound_import_exclusions (
                    owner TEXT NOT NULL,
                    source TEXT NOT NULL,
                    kind TEXT NOT NULL CHECK(kind IN ('call','recording')),
                    identifier TEXT NOT NULL,
                    PRIMARY KEY(owner, source, kind, identifier)
                );
                CREATE TABLE IF NOT EXISTS console_call_removals (
                    call_key TEXT PRIMARY KEY,
                    actor TEXT NOT NULL,
                    occurred_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS retired_transcription_groups (
                    recording_id TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS outbound_console_retries (
                    call_id TEXT PRIMARY KEY REFERENCES outbound_calls(call_id),
                    recordings_checked INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS outbound_short_calls (
                    call_id TEXT PRIMARY KEY REFERENCES outbound_calls(call_id)
                );
                CREATE INDEX IF NOT EXISTS recordings_call ON recordings(call_id);
            """)
            with db:
                db.execute("BEGIN IMMEDIATE")
                # The daily report now counts from the archive records themselves.
                db.execute("DROP TABLE IF EXISTS archive_completions")
                cutoff_columns = {row["name"] for row in db.execute("PRAGMA table_info(outbound_cleanup_cutoff)")}
                if "archived_at" in cutoff_columns:
                    db.execute("ALTER TABLE outbound_cleanup_cutoff RENAME COLUMN archived_at TO cutoff_at")
                for table, column in (
                    ("workflow", "transcription_failures INTEGER NOT NULL DEFAULT 0"),
                    ("workflow", "archive_checked_at REAL"),
                    ("workflow", "archive_lookup_grouped INTEGER NOT NULL DEFAULT 1"),
                    ("outbound_import_exclusions", "call_started_at REAL"),
                    ("outbound_recordings", "download_attempts INTEGER NOT NULL DEFAULT 0"),
                    ("outbound_calls", "recordings_checked_at INTEGER NOT NULL DEFAULT 0"),
                    ("outbound_console_retries", "recordings_checked INTEGER NOT NULL DEFAULT 0"),
                    ("console_call_removals", "owner TEXT"),
                    ("console_call_removals", "external_id TEXT"),
                    ("remote_attachments", "filename_stem TEXT"),
                    ("remote_attachments", "retry_at REAL DEFAULT 0"),
                    ("remote_attachments", "failures INTEGER NOT NULL DEFAULT 0"),
                    ("remote_attachments", "routing_key TEXT"),
                ):
                    fields = {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}
                    if column.split()[0] not in fields:
                        db.execute(f"ALTER TABLE {table} ADD COLUMN {column}")
                        if table == "outbound_import_exclusions":
                            self.backfill_exclusion_call_times(db)

    @staticmethod
    def call_start_timestamp(metadata):
        value = json.loads(metadata).get("started_at")
        if not isinstance(value, str):
            return None
        try:
            started = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return started.timestamp() if started.tzinfo is not None else None

    @classmethod
    def backfill_exclusion_call_times(cls, db):
        # Recover time only for an exact match to a single surviving call.
        # Some exclusions no longer have a call or recording row to look up.
        rows = db.execute("""
            WITH links AS (
                SELECT owner,source,'call' AS kind,external_id AS identifier,call_id FROM outbound_calls
                UNION SELECT owner,source,'call',identifier,call_id FROM outbound_call_aliases
                UNION SELECT c.owner,c.source,'recording',r.external_id,c.call_id
                      FROM outbound_recordings r JOIN outbound_calls c USING (call_id)
            )
            SELECT x.owner,x.source,x.kind,x.identifier,c.metadata
            FROM outbound_import_exclusions x JOIN links l USING (owner,source,kind,identifier)
            JOIN outbound_calls c USING (call_id)
            WHERE x.call_started_at IS NULL
            GROUP BY x.owner,x.source,x.kind,x.identifier HAVING count(*)=1
        """).fetchall()
        db.executemany("""
            UPDATE outbound_import_exclusions SET call_started_at=?
            WHERE owner=? AND source=? AND kind=? AND identifier=?
        """, [(cls.call_start_timestamp(row["metadata"]), row["owner"], row["source"],
               row["kind"], row["identifier"]) for row in rows])

    def connect(self):
        db = sqlite3.connect(self.database, timeout=10)
        db.row_factory = sqlite3.Row
        db.create_function("recording_retry", 6, recording_retry, deterministic=True)
        db.execute("PRAGMA foreign_keys = ON")
        return db

    @contextmanager
    def transaction(self):
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if self.write_guard is not None:
                self.write_guard(db)
            yield db
            if self.write_guard is not None:
                self.write_guard(db)

    def submit(self, owner, request_id, metadata, audio, media_type, *, linked_call=None, duration_ms=None):
        serialized = json.dumps(metadata, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        digest = hashlib.sha256(audio).hexdigest()
        with self.transaction() as db:
            existing = db.execute(
                """SELECT id, call_id, status, digest, metadata
                   FROM recordings WHERE owner = ? AND request_id = ?""", (owner, request_id),
            ).fetchone()
            if existing:
                if (existing["digest"] != digest or existing["metadata"] != serialized
                        or (linked_call is not None and existing["call_id"] != linked_call)):
                    raise SubmissionConflict
                return self.summary(existing)

            call_id = linked_call
            if linked_call is not None:
                if db.execute("""SELECT 1 FROM calls c WHERE c.id=? AND c.owner=?
                    AND c.id NOT IN (SELECT call_key FROM console_call_removals)
                    AND NOT EXISTS (SELECT 1 FROM outbound_calls o JOIN outbound_import_exclusions x
                        ON x.owner=o.owner AND x.source=o.source AND x.kind='call' AND x.identifier=o.external_id
                        WHERE o.call_id=c.id)""", (linked_call, owner)).fetchone() is None:
                    raise SubmissionConflict
            elif metadata["call"] is not None:
                external_id = metadata["call"].get("external_call_id")
                if external_id is not None and db.execute("""
                    SELECT 1 FROM console_call_removals x LEFT JOIN calls c ON c.id=x.call_key
                    WHERE coalesce(x.owner,c.owner)=? AND coalesce(x.external_id,c.external_id)=?
                """, (owner, external_id)).fetchone() is not None:
                    raise SubmissionConflict
                if external_id is None:
                    call_id = uuid4().hex
                else:
                    db.execute(
                        """INSERT INTO calls (id, owner, external_id) VALUES (?, ?, ?)
                           ON CONFLICT (owner, external_id) DO NOTHING""",
                        (uuid4().hex, owner, external_id),
                    )
                    call_id = db.execute(
                        "SELECT id FROM calls WHERE owner = ? AND external_id = ?",
                        (owner, external_id),
                    ).fetchone()["id"]
                if external_id is None:
                    db.execute("INSERT INTO calls VALUES (?, ?, ?)", (call_id, owner, None))

            recording_id = uuid4().hex
            db.execute(
                """INSERT INTO recordings
                   (id, owner, request_id, call_id, metadata, digest, media_type, audio)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (recording_id, owner, request_id, call_id, serialized, digest, media_type, audio),
            )
            if duration_ms is not None:
                db.execute("INSERT INTO recording_media VALUES (?, ?)", (recording_id, duration_ms))
            self.record_event(db, recording_id, "queued", actor=owner)
            return self.summary({"id": recording_id, "call_id": call_id, "status": "queued"})

    @staticmethod
    def summary(row):
        return {"recording_id": row["id"], "call_id": row["call_id"], "status": row["status"]}

    @staticmethod
    def available_actions(row):
        status = row["status"]
        now = time.time()
        return {
            "can_remove": status not in {"archived", "removed"} and (row["lease_until"] or 0) <= now,
            "can_restore": status == "removed",
            "can_retry": recording_retry(status, row["error"], row["resume_status"], row["retry_at"],
                                         row["lease_until"], now) is not None,
        }

    def get(self, owner, recording_id):
        with closing(self.connect()) as db:
            row = db.execute(
                """SELECT r.id, r.call_id, r.status, r.metadata, r.media_type, r.digest,
                          w.job_id, w.transcript, w.receipts, w.error, w.retry_at, w.lease_until,
                          recovery.resume_status, media.duration_ms
                   FROM recordings r LEFT JOIN workflow w ON w.recording_id = r.id
                   LEFT JOIN recording_recovery recovery ON recovery.recording_id = r.id
                   LEFT JOIN recording_media media ON media.recording_id=r.id
                   WHERE (? IS NULL OR r.owner = ?) AND r.id = ?""", (owner, owner, recording_id),
            ).fetchone()
            if row is None:
                return None
            expired = row["status"] == "archived" and self.archive_cache_expired(db, recording_id)
            return {**self.summary(row), **json.loads(row["metadata"]),
                    "media_type": row["media_type"], "sha256": row["digest"],
                    "transcription_job_id": row["job_id"], "recording_duration_ms": row["duration_ms"],
                    "transcript": json.loads(row["transcript"]) if row["transcript"] and not expired else None,
                    "receipts": json.loads(row["receipts"]) if row["receipts"] else {},
                    "error": row["error"],
                    **self.available_actions(row)}

    def get_audio(self, owner, recording_id):
        with closing(self.connect()) as db:
            return db.execute(f"""SELECT audio, media_type FROM recordings
                WHERE (? IS NULL OR owner=?) AND id=? AND length(audio)>0 AND
                      (status<>'archived' OR NOT EXISTS (SELECT 1 FROM ({ARCHIVE_CACHE_ROWS}) cache
                        WHERE cache.recording_id=recordings.id AND cache.archived_at<=?))""",
                (owner, owner, recording_id, time.time() - ARCHIVE_CACHE_SECONDS)).fetchone()

    @staticmethod
    def archive_cache_expired(db, recording_id):
        return db.execute(f"""SELECT 1 FROM ({ARCHIVE_CACHE_ROWS}) cache
            WHERE recording_id=? AND archived_at<=?""",
            (recording_id, time.time() - ARCHIVE_CACHE_SECONDS)).fetchone() is not None

    def purge_archive_cache(self, now=None, *, history_seconds=ARCHIVE_HISTORY_SECONDS):
        now = time.time() if now is None else now
        cutoff = now - ARCHIVE_CACHE_SECONDS
        expired = f"SELECT recording_id FROM ({ARCHIVE_CACHE_ROWS}) cache WHERE archived_at<=?"
        with self.transaction() as db:
            db.execute("UPDATE workflow SET transcript=NULL WHERE transcript IS NOT NULL AND recording_id IN ("
                       + expired + ")", (cutoff,))
            cleared = db.execute("UPDATE recordings SET audio=X'' WHERE length(audio)>0 AND id IN ("
                                 + expired + ")", (cutoff,)).rowcount
            calls = db.execute(f"""WITH closures AS ({OUTBOUND_ARCHIVE_CLOSURES})
                SELECT c.call_id, c.closed_at FROM closures c WHERE c.closed_at<=?
                ORDER BY c.closed_at LIMIT 100""", (cutoff,)).fetchall()
            for call in calls:
                self.retire_call(db, call["call_id"], include_archived=True)
            removed = db.execute("""SELECT x.call_key FROM console_call_removals x
                WHERE EXISTS (SELECT 1 FROM recordings r
                              WHERE (r.call_id=x.call_key OR (r.call_id IS NULL AND r.id=x.call_key))
                                AND r.status<>'archived')
                   OR EXISTS (SELECT 1 FROM outbound_calls o WHERE o.call_id=x.call_key
                              AND NOT EXISTS (SELECT 1 FROM outbound_import_exclusions s
                                  WHERE s.owner=o.owner AND s.source=o.source AND s.kind='call'
                                    AND s.identifier=o.external_id))
                LIMIT 100""").fetchall()
            for call in removed:
                self.retire_call(db, call["call_key"])
            history_cutoff = now - history_seconds
            # Replays and exclusion expiry both use when the call started.
            latest = db.execute("SELECT max(call_started_at) FROM outbound_import_exclusions WHERE call_started_at<=?",
                                (history_cutoff,)).fetchone()[0]
            if latest is not None:
                # Retention may later increase. Never reopen a range whose IDs were deleted.
                db.execute("""INSERT INTO outbound_cleanup_cutoff VALUES (1,?)
                              ON CONFLICT(singleton) DO UPDATE SET cutoff_at=max(cutoff_at,excluded.cutoff_at)""",
                           (latest,))
                db.execute("DELETE FROM outbound_import_exclusions WHERE call_started_at<=?", (history_cutoff,))
            return cleared

    @staticmethod
    def outbound_scan_cutoff(db, now, history_seconds):
        row = db.execute("SELECT cutoff_at FROM outbound_cleanup_cutoff WHERE singleton=1").fetchone()
        cutoff = now - history_seconds
        return max(cutoff, row["cutoff_at"]) if row is not None else cutoff

    def retire_call(self, db, call_key, *, include_archived=False, now=None):
        now = time.time() if now is None else now
        rows = db.execute("""SELECT r.id, r.status, coalesce(w.lease_until,0) AS lease_until
            FROM recordings r LEFT JOIN workflow w ON w.recording_id=r.id
            WHERE r.call_id=? OR (r.call_id IS NULL AND r.id=?)""", (call_key, call_key)).fetchall()
        removable = [row for row in rows if include_archived or row["status"] != "archived"]
        if any(row["lease_until"] > now for row in removable):
            raise RemovalUnavailable

        source = db.execute("SELECT owner,source,external_id,metadata FROM outbound_calls WHERE call_id=?",
                            (call_key,)).fetchone()
        if source is not None:
            scope = source["owner"], source["source"]
            started_at = self.call_start_timestamp(source["metadata"])
            db.execute("""INSERT INTO outbound_import_exclusions (owner,source,kind,identifier,call_started_at)
                SELECT ?,?,'call',?,?
                UNION SELECT owner,source,'call',identifier,? FROM outbound_call_aliases WHERE call_id=?
                UNION SELECT ?,?,'recording',external_id,? FROM outbound_recordings WHERE call_id=?
                ON CONFLICT(owner,source,kind,identifier) DO UPDATE SET call_started_at=excluded.call_started_at
                WHERE outbound_import_exclusions.call_started_at IS NULL""",
                       (*scope, source["external_id"], started_at, started_at, call_key, *scope, started_at, call_key))
            db.execute("DELETE FROM outbound_console_retries WHERE call_id=?", (call_key,))
            db.execute("UPDATE outbound_calls SET complete=1 WHERE call_id=?", (call_key,))

        ids = json.dumps([row["id"] for row in removable])
        members = "SELECT value FROM json_each(?)"
        # A transcription submit may have reached the service even if no job ID was saved.
        db.execute(f"""INSERT OR IGNORE INTO retired_transcription_groups (recording_id)
            SELECT r.id FROM recordings r JOIN workflow w ON w.recording_id=r.id
            WHERE r.id IN ({members}) AND r.status<>'archived'
        """, (ids,))
        db.execute("DELETE FROM outbound_recordings WHERE call_id=? AND (recording_id IS NULL OR recording_id IN ("
                   + members + "))", (call_key, ids))
        for table in ("remote_attachments", "recording_events", "recording_recovery",
                      "recording_removals", "recording_media", "workflow"):
            db.execute(f"DELETE FROM {table} WHERE recording_id IN ({members})", (ids,))
        db.execute(f"DELETE FROM recordings WHERE id IN ({members})", (ids,))
        db.execute("DELETE FROM outbound_short_calls WHERE call_id=?", (call_key,))
        if len(removable) == len(rows):
            # Preserve the API identity on older removal records before dropping the call row.
            db.execute("""UPDATE console_call_removals SET
                owner=(SELECT owner FROM calls WHERE id=?),
                external_id=(SELECT external_id FROM calls WHERE id=?)
                WHERE call_key=? AND owner IS NULL
            """, (call_key, call_key, call_key))
            db.execute("DELETE FROM outbound_call_aliases WHERE call_id=?", (call_key,))
            db.execute("DELETE FROM outbound_calls WHERE call_id=?", (call_key,))
            db.execute("DELETE FROM calls WHERE id=?", (call_key,))


    def due_transcription_jobs(self, *, now, excluding, limit):
        with closing(self.connect()) as db:
            rows = db.execute(
                "SELECT w.job_id " + WORKFLOW_CANDIDATES
                + " AND r.status='transcribing' AND w.job_id<>:excluding" + WORKFLOW_ORDER + " LIMIT :limit",
                {"now": now, "excluding": excluding, "limit": limit, "completed_job": None, "lease_at": now},
            )
            return list(dict.fromkeys(row["job_id"] for row in rows))

    def due_recordings(self, *, now, limit, exclude=()):
        with closing(self.connect()) as db:
            return [row["id"] for row in db.execute(
                "SELECT r.id " + WORKFLOW_CANDIDATES + " AND r.id NOT IN (SELECT value FROM json_each(:exclude))"
                + WORKFLOW_ORDER + " LIMIT :limit",
                {"now": now, "limit": limit, "completed_job": None, "lease_at": now,
                 "exclude": json.dumps(list(exclude))},
            )]

    def has_outbound_retry(self, call_key, owner, source):
        with closing(self.connect()) as db:
            return db.execute("""SELECT 1 FROM outbound_console_retries JOIN outbound_calls USING(call_id)
                                 WHERE call_id=? AND owner=? AND source=?""", (call_key, owner, source)).fetchone() is not None

    def call_recordings(self, call_key):
        with closing(self.connect()) as db:
            return [row["id"] for row in db.execute(
                "SELECT id FROM recordings WHERE call_id=? OR id=? ORDER BY rowid", (call_key, call_key),
            )]

    def recording_for_job(self, job_id):
        with closing(self.connect()) as db:
            row = db.execute("""SELECT w.recording_id FROM workflow w JOIN recordings r ON r.id=w.recording_id
                                WHERE w.job_id=? AND r.status IN ('transcribing','acknowledging','delivering')""",
                             (job_id,)).fetchone()
            return row["recording_id"] if row else None

    def claim(self, *, now, lease=120, recording_id=None, completed_job=None, lease_at=None):
        candidates = WORKFLOW_CANDIDATES
        lease_at = now if lease_at is None else lease_at
        parameters = {"now": now, "completed_job": completed_job, "lease_at": lease_at}
        if recording_id is not None:
            candidates += " AND r.id=:recording_id"
            parameters["recording_id"] = recording_id
        if completed_job is not None:
            candidates += " AND w.job_id=:completed_job"
        with closing(self.connect()) as probe:
            if probe.execute("SELECT 1 " + candidates + " LIMIT 1", parameters).fetchone() is None:
                return None
        with self.transaction() as db:
            row = db.execute(
                """SELECT r.*, w.job_id, w.transcript, w.error, coalesce(recovery.generation, 0) AS retry_generation,
                          coalesce(w.transcription_failures, 0) AS transcription_failures,
                          recovery.resume_status IS NULL AND recovery.retry_id IS NOT NULL AS manual_lookup """
                + candidates + WORKFLOW_ORDER + " LIMIT 1",
                parameters,
            ).fetchone()
            if row is None:
                return None
            token = uuid4().hex
            db.execute(
                """INSERT INTO workflow (recording_id, lease_until, lease_token) VALUES (?, ?, ?)
                   ON CONFLICT (recording_id) DO UPDATE
                   SET lease_until = excluded.lease_until, lease_token = excluded.lease_token""",
                (row["id"], lease_at + lease, token),
            )
            return {**dict(row), "lease_token": token}

    def advance(self, item, status, *, retry_at=0, job_id=None, transcript=None, receipts=None, error=None,
                restart=False, occurred_at=None, archive_checked_at=None, archive_lookup_grouped=None):
        with self.transaction() as db:
            updated = db.execute(
                """UPDATE workflow SET retry_at = ?, lease_until = 0, lease_token = NULL,
                   archive_checked_at = coalesce(?, archive_checked_at),
                   archive_lookup_grouped = coalesce(?, archive_lookup_grouped),
                   job_id = coalesce(?, job_id), transcript = coalesce(?, transcript),
                   receipts = coalesce(?, receipts), error = ?,
                   transcription_failures = CASE WHEN ? THEN transcription_failures+1
                                                 WHEN ? THEN 0 ELSE transcription_failures END
                   WHERE recording_id = ? AND lease_token = ?""",
                (retry_at, archive_checked_at, archive_lookup_grouped, job_id, transcript, receipts, error,
                 error == "transcription_failed" and item["status"] != "delivering",
                 error is None and status in {"acknowledging", "delivering"}, item["id"], item["lease_token"]),
            ).rowcount
            if updated:
                db.execute("UPDATE recordings SET status = ? WHERE id = ?", (status, item["id"]))
                if restart:
                    db.execute("UPDATE workflow SET job_id=NULL WHERE recording_id=?", (item["id"],))
                    db.execute("""INSERT INTO recording_recovery (recording_id, generation) VALUES (?, 1)
                        ON CONFLICT (recording_id) DO UPDATE SET generation=generation+1, resume_status=NULL""",
                        (item["id"],))
                if status in {"needs_review", "needs_input"}:
                    db.execute(
                        """INSERT INTO recording_recovery (recording_id, resume_status) VALUES (?, ?)
                           ON CONFLICT (recording_id) DO UPDATE SET resume_status=excluded.resume_status""",
                        (item["id"], item.get("resume_stage", item["status"])),
                    )
                if item["status"] != status or item["error"] != error:
                    self.record_event(db, item["id"], status, actor="archive-worker", error=error, now=occurred_at)
        if updated and (item["status"] != status or item["error"] != error):
            event("archive_stage", recording_id=item["id"], job_id=job_id or item["job_id"],
                  status=status, reason=error, level="WARNING" if error else "INFO")
        return bool(updated)


    @staticmethod
    def record_event(db, recording_id, status, *, actor, error=None, now=None):
        now = time.time() if now is None else now
        db.execute("""INSERT INTO recording_events (recording_id, occurred_at, status, error, actor)
                      VALUES (?, ?, ?, ?, ?)""", (recording_id, now, status, error, actor))

    def list_recordings(self, owner, *, cursor, limit, view="all", day=None, mobile=None,
                        assignee=None, status=None, include_actions=False):
        with closing(self.connect()) as db:
            rows = db.execute(
                """SELECT r.rowid AS position, r.id, r.owner, r.status, r.call_id, r.metadata, w.error,
                          media.duration_ms, w.lease_until, w.retry_at, recovery.resume_status
                   FROM recordings r LEFT JOIN workflow w ON w.recording_id=r.id
                   LEFT JOIN recording_media media ON media.recording_id=r.id
                   LEFT JOIN recording_recovery recovery ON recovery.recording_id=r.id
                   WHERE (:owner IS NULL OR r.owner=:owner) AND (:cursor IS NULL OR r.rowid < :cursor)
                     AND (:view='all' OR (:view='archived' AND r.status='archived')
                                      OR (:view='pending' AND r.status NOT IN ('archived', 'removed')))
                     AND (:status IS NULL OR r.status=:status)
                     AND (:day IS NULL OR substr(json_extract(r.metadata, '$.call.started_at'), 1, 10)=:day)
                     AND (:mobile IS NULL OR json_extract(r.metadata, '$.call.callee')=:mobile)
                     AND (:assignee IS NULL OR r.owner=:assignee OR json_extract(r.metadata, '$.call.caller')=:assignee)
                   ORDER BY r.rowid DESC LIMIT :limit""",
                {"owner": owner, "cursor": cursor, "view": view, "status": status, "day": day,
                 "mobile": mobile, "assignee": assignee, "limit": limit + 1},
            ).fetchall()
        return {
            "items": [{**self.summary(row), "owner": row["owner"],
                       "call": json.loads(row["metadata"]).get("call"), "error": row["error"],
                       "recording_duration_ms": row["duration_ms"],
                       **(self.available_actions(row) if include_actions else {})}
                      for row in rows[:limit]],
            "next_cursor": rows[limit - 1]["position"] if len(rows) > limit else None,
        }

    def events(self, owner, recording_id, *, after, limit):
        with closing(self.connect()) as db:
            if db.execute("""SELECT 1 FROM recordings
                             WHERE id=? AND (? IS NULL OR owner=?)""",
                          (recording_id, owner, owner)).fetchone() is None:
                return None
            rows = db.execute("""SELECT sequence, occurred_at, status, actor, error
                                 FROM recording_events WHERE recording_id=? AND sequence>?
                                 ORDER BY sequence LIMIT ?""",
                              (recording_id, after, limit + 1)).fetchall()
        return {"items": [dict(row) for row in rows[:limit]],
                "next_cursor": rows[limit - 1]["sequence"] if len(rows) > limit else None}


    def request_retry(self, owner, recording_id, *, actor):
        with self.transaction() as db:
            return self._request_recording_retry(db, owner, recording_id, actor=actor, now=time.time())

    def request_call_retry(self, call_key, *, actor):
        with self.transaction() as db:
            params = {"id": call_key, "now": time.time()}
            call = db.execute(CALL_STATE + """
                SELECT waiting_recording, pending, outbound, busy FROM summaries WHERE call_key=:id
            """, params).fetchone()
            if call is None:
                return None

            scheduled = queued = False
            if call["outbound"] and (call["waiting_recording"] or call["pending"]):
                source_retry = db.execute(
                    """SELECT q.recordings_checked, EXISTS (
                           SELECT 1 FROM outbound_recordings r
                           WHERE r.call_id=q.call_id AND r.recording_id IS NULL
                             AND (r.error='invalid_audio' OR r.download_attempts>=?)
                       ) AS blocked
                       FROM outbound_console_retries q WHERE call_id=?""", (DOWNLOAD_ATTEMPT_LIMIT, call_key),
                ).fetchone()
                queued = source_retry is not None and not source_retry["blocked"] and (
                    (call["waiting_recording"] and not source_retry["recordings_checked"]) or call["pending"]
                )
                if not queued:
                    db.execute("""INSERT INTO outbound_console_retries (call_id) VALUES (?)
                                  ON CONFLICT(call_id) DO UPDATE SET recordings_checked=0""", (call_key,))
                    db.execute("""UPDATE outbound_recordings SET error=NULL, download_attempts=0
                                  WHERE call_id=? AND recording_id IS NULL""", (call_key,))
                    scheduled = True

            recordings = db.execute(CALL_STATE + """
                SELECT f.id, r.retry_id IS NOT NULL AND (
                        (f.status='needs_input' AND w.retry_at<0)
                        OR (f.status IN ('queued','delivering') AND w.error IS NULL AND w.retry_at=0)
                    ) AS already_queued
                FROM recording_facts f JOIN workflow w ON w.recording_id=f.id
                LEFT JOIN recording_recovery r ON r.recording_id=f.id
                WHERE f.call_key=:id AND (f.retryable OR (
                    r.retry_id IS NOT NULL AND coalesce(w.lease_until,0)<=:now AND
                    f.status IN ('queued','delivering') AND w.error IS NULL AND w.retry_at=0
                )) ORDER BY f.position
            """, params).fetchall()
            for row in recordings:
                if row["already_queued"]:
                    queued = True
                else:
                    self._request_recording_retry(db, None, row["id"], actor=actor, now=params["now"])
                    scheduled = True
            if scheduled:
                return "scheduled"
            if queued:
                return "queued"
            return "processing" if call["busy"] else None

    def _request_recording_retry(self, db, owner, recording_id, *, actor, now):
        item = db.execute(
            """SELECT r.status, w.error, w.retry_at, w.lease_until, w.archive_checked_at,
                      w.archive_lookup_grouped, recovery.resume_status, recovery.retry_id
               FROM recordings r LEFT JOIN workflow w ON w.recording_id=r.id
               LEFT JOIN recording_recovery recovery ON recovery.recording_id=r.id
               WHERE r.id=? AND (? IS NULL OR r.owner=?)""", (recording_id, owner, owner),
        ).fetchone()
        if item is None:
            return None
        mode = recording_retry(item["status"], item["error"], item["resume_status"],
                               item["retry_at"], item["lease_until"], now)
        if mode == "expedite_transcription":
            retry_id = uuid4().hex
            db.execute("UPDATE workflow SET retry_at=0 WHERE recording_id=?", (recording_id,))
            db.execute("""INSERT INTO recording_recovery (recording_id,retry_id) VALUES (?,?)
                          ON CONFLICT(recording_id) DO UPDATE SET retry_id=excluded.retry_id""",
                       (recording_id, retry_id))
            self.record_event(db, recording_id, "queued", actor=actor)
            return {"retry_id": retry_id, "status": item["status"]}
        if item["status"] in {"queued", "transcribing", "acknowledging", "delivering"} and item["retry_id"]:
            return {"retry_id": item["retry_id"], "status": item["status"]}
        if mode is None:
            raise RetryUnavailable

        if item["status"] == "needs_input" and item["error"] == "archive_target_missing":
            if item["retry_at"] < 0 and item["retry_id"]:
                return {"retry_id": item["retry_id"], "status": item["status"]}
            if item["archive_lookup_grouped"] and (item["retry_at"] > 0 or item["archive_checked_at"] is not None):
                retry_id = uuid4().hex
                db.execute("UPDATE workflow SET retry_at=-1 WHERE recording_id=?", (recording_id,))
                db.execute("UPDATE recording_recovery SET retry_id=? WHERE recording_id=?",
                           (retry_id, recording_id))
                self.record_event(db, recording_id, item["status"], actor=actor)
                return {"retry_id": retry_id, "status": item["status"]}

        restart = item["error"] == "transcription_failed"
        status = "queued" if restart else item["resume_status"]
        retry_id = uuid4().hex
        db.execute("""UPDATE recording_recovery SET retry_id=?, resume_status=NULL, generation=generation+?
                      WHERE recording_id=?""", (retry_id, int(restart), recording_id))
        if restart:
            db.execute("UPDATE workflow SET job_id=NULL, transcript=NULL, transcription_failures=0 WHERE recording_id=?",
                       (recording_id,))
        if item["error"] in {"archive_attachment_unknown", "archive_attachment_rejected"}:
            db.execute("""UPDATE remote_attachments SET status='pending',receipt=NULL,retry_at=0,failures=0
                          WHERE recording_id=? AND status='rejected'""", (recording_id,))
            db.execute("UPDATE remote_attachments SET retry_at=0 WHERE recording_id=? AND status='unknown'",
                       (recording_id,))
        db.execute("UPDATE workflow SET retry_at=0, error=NULL WHERE recording_id=?", (recording_id,))
        db.execute("UPDATE recordings SET status=? WHERE id=?", (status, recording_id))
        self.record_event(db, recording_id, status, actor=actor)
        return {"retry_id": retry_id, "status": status}


    def set_removed(self, owner, recording_id, *, actor, removed, now=None):
        now = time.time() if now is None else now
        with self.transaction() as db:
            return self._set_removed(db, owner, recording_id, actor=actor, removed=removed, now=now)

    def remove_many(self, owner, recording_ids, *, actor):
        now = time.time()
        with self.transaction() as db:
            for recording_id in recording_ids:
                if self._set_removed(db, owner, recording_id, actor=actor, removed=True, now=now) is None:
                    raise RemovalUnavailable

    def _set_removed(self, db, owner, recording_id, *, actor, removed, now):
        item = db.execute(
            """SELECT r.status, w.lease_until, removal.previous_status
               FROM recordings r LEFT JOIN workflow w ON w.recording_id=r.id
               LEFT JOIN recording_removals removal ON removal.recording_id=r.id
               WHERE r.id=? AND (? IS NULL OR r.owner=?)""", (recording_id, owner, owner),
        ).fetchone()
        if item is None:
            return None
        status = item["status"]
        if removed and status != "removed":
            if status == "archived" or (item["lease_until"] or 0) > now:
                raise RemovalUnavailable
            db.execute("INSERT INTO recording_removals VALUES (?, ?)", (recording_id, status))
            db.execute("UPDATE workflow SET lease_token=NULL, lease_until=0 WHERE recording_id=?",
                       (recording_id,))
            status = "removed"
        elif not removed and status == "removed":
            status = item["previous_status"]
            if status is None:
                raise RemovalUnavailable
            db.execute("DELETE FROM recording_removals WHERE recording_id=?", (recording_id,))
        if status != item["status"]:
            db.execute("UPDATE recordings SET status=? WHERE id=?", (status, recording_id))
            self.record_event(db, recording_id, status, actor=actor)
        return {"recording_id": recording_id, "status": status}
