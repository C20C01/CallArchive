import hashlib
import json
from contextlib import closing
from uuid import uuid4

from .store import ArchiveStore, SubmissionConflict


class BrowserJobs:
    """Associate independent transcription jobs with the submitting operator."""

    def __init__(self, store: ArchiveStore):
        self.store = store
        with closing(store.connect()) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS browser_transcriptions (
                    id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    options TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    upstream_id TEXT,
                    UNIQUE (owner, request_id)
                );
            """)

    def reserve(self, owner, request_id, audio, options, filename):
        payload = json.dumps(options, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha256(audio).hexdigest()
        with self.store.transaction() as db:
            row = db.execute("SELECT * FROM browser_transcriptions WHERE owner=? AND request_id=?",
                             (owner, request_id)).fetchone()
            if row is not None:
                if row["digest"] != digest or row["options"] != payload:
                    raise SubmissionConflict
                return dict(row)
            job_id = uuid4().hex
            db.execute("INSERT INTO browser_transcriptions VALUES (?, ?, ?, ?, ?, ?, NULL)",
                       (job_id, owner, request_id, digest, payload, filename))
            return {"id": job_id, "upstream_id": None}

    def attach(self, job_id, upstream_id):
        with self.store.transaction() as db:
            db.execute("""UPDATE browser_transcriptions SET upstream_id=?
                          WHERE id=? AND upstream_id IS NULL""", (upstream_id, job_id))

    def get(self, owner, job_id):
        with closing(self.store.connect()) as db:
            return db.execute("SELECT * FROM browser_transcriptions WHERE id=? AND (? IS NULL OR owner=?)",
                              (job_id, owner, owner)).fetchone()

    def remove_missing(self, entries):
        with self.store.transaction() as db:
            db.executemany("DELETE FROM browser_transcriptions WHERE id=? AND upstream_id=?", entries)

    def list(self, owner, *, cursor=None, limit=35, confirmed_only=False):
        with closing(self.store.connect()) as db:
            rows = db.execute("""SELECT rowid AS position, id, filename, upstream_id FROM browser_transcriptions
                                 WHERE (? IS NULL OR owner=?) AND (? IS NULL OR rowid<?)
                                   AND (NOT ? OR upstream_id IS NOT NULL)
                                 ORDER BY rowid DESC LIMIT ?""",
                              (owner, owner, cursor, cursor, confirmed_only, limit + 1)).fetchall()
        return {"items": rows[:limit], "next_cursor": rows[limit - 1]["position"] if len(rows) > limit else None}
