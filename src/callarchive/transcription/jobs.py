import hashlib
import json
import sqlite3
import time
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from ..sqlite import enable_wal


DEFAULT_RETENTION = 86400
ARCHIVE_RETIREMENT_FENCE_SECONDS = 86400
DEFAULT_MAX_UNCLAIMED = 7 * 86400
EXPIRED_JOBS = """
    (status='failed' AND archive_group IS NULL AND finished<=:cutoff)
    OR (status='succeeded' AND ((requires_ack=0 AND finished<=:cutoff) OR acknowledged<=:cutoff
        OR (requires_ack=1 AND acknowledged IS NULL AND finished<=:unclaimed_cutoff)))
"""


@dataclass(frozen=True)
class Claim:
    job_id: str
    token: str
    audio: bytes
    options: dict
    archive_group: str | None = None


class RequestConflict(ValueError):
    pass


class ArchiveGroupRetired(ValueError):
    pass


class JobStore:
    def __init__(self, database):
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            enable_wal(db)
            db.execute("BEGIN IMMEDIATE")
            db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, owner TEXT NOT NULL, request_id TEXT NOT NULL,
                digest TEXT NOT NULL, options TEXT NOT NULL, audio BLOB, media_type TEXT NOT NULL,
                engine TEXT NOT NULL, requires_ack INTEGER NOT NULL, status TEXT NOT NULL,
                created REAL NOT NULL, claim_token TEXT, lease_until REAL, finished REAL,
                acknowledged REAL, result TEXT, error TEXT, UNIQUE(owner, request_id)
            )""")
            columns = {row["name"] for row in db.execute("PRAGMA table_info(jobs)")}
            if "filename" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN filename TEXT")
            if "progress" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN progress TEXT")
            if "archive_group" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN archive_group TEXT")
            db.execute("CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(engine, status, created)")
            db.execute("""CREATE INDEX IF NOT EXISTS jobs_archive_group ON jobs(owner, archive_group)
                          WHERE archive_group IS NOT NULL""")
            db.execute("""CREATE TABLE IF NOT EXISTS retired_archive_groups (
                owner TEXT NOT NULL, group_id TEXT NOT NULL, expires_at REAL NOT NULL,
                PRIMARY KEY (owner, group_id)
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS worker_heartbeats (
                engine TEXT PRIMARY KEY, seen REAL NOT NULL
            )""")

    @contextmanager
    def connection(self):
        with closing(sqlite3.connect(self.database, timeout=5)) as db, db:
            db.row_factory = sqlite3.Row
            yield db

    def submit(self, owner, requires_ack, request_id, options, audio, media_type, engine, *,
               filename=None, archive_group=None, retention=DEFAULT_RETENTION,
               max_unclaimed=DEFAULT_MAX_UNCLAIMED):
        digest = hashlib.sha256(audio).hexdigest()
        normalized = json.dumps(options, ensure_ascii=False, sort_keys=True)
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            now = time.time()
            if archive_group is not None and db.execute(
                "SELECT 1 FROM retired_archive_groups WHERE owner=? AND group_id=? AND expires_at>?",
                (owner, archive_group, now),
            ).fetchone() is not None:
                raise ArchiveGroupRetired
            db.execute(f"DELETE FROM jobs WHERE owner=:owner AND request_id=:request_id AND ({EXPIRED_JOBS})",
                       {"owner": owner, "request_id": request_id,
                        "cutoff": now - retention, "unclaimed_cutoff": now - max_unclaimed})
            existing = db.execute(
                "SELECT id, digest, options FROM jobs WHERE owner=? AND request_id=?",
                (owner, request_id),
            ).fetchone()
            if existing:
                if (existing["digest"], existing["options"]) != (digest, normalized):
                    raise RequestConflict("请求标识已用于其他输入")
                return existing["id"]
            job_id = uuid4().hex
            db.execute("""INSERT INTO jobs
                (id,owner,request_id,digest,options,audio,media_type,engine,requires_ack,status,created,filename,
                 archive_group)
                VALUES (?,?,?,?,?,?,?,?,?,'queued',?,?,?)""",
                (job_id, owner, request_id, digest, normalized, audio, media_type,
                 engine, requires_ack, now, filename, archive_group),
            )
            return job_id

    def get(self, owner, job_id):
        with self.connection() as db:
            return db.execute("""SELECT id,status,engine,error,finished,acknowledged,
                requires_ack,result, CASE WHEN lease_until>? THEN progress END AS progress
                FROM jobs WHERE owner=? AND id=?""", (time.time(), owner, job_id)).fetchone()

    def statuses(self, owner, job_ids):
        with self.connection() as db:
            return db.execute("""SELECT id, status, created, finished, acknowledged, archive_group, audio IS NOT NULL AS has_audio,
                result IS NOT NULL AS has_result, requires_ack,
                CASE WHEN lease_until>? THEN progress END AS progress
                FROM jobs WHERE owner=? AND id IN (SELECT value FROM json_each(?))""",
                (time.time(), owner, json.dumps(job_ids))).fetchall()

    def external_jobs(self, gateway, owners, *, job_id=None, include_system=False, system_only=False):
        with self.connection() as db:
            return db.execute("""SELECT id, owner, filename, status, created, finished, acknowledged, archive_group,
                requires_ack, audio IS NOT NULL AS has_audio, result IS NOT NULL AS has_result,
                CASE WHEN :job_id IS NOT NULL THEN result END AS result,
                CASE WHEN lease_until>:now THEN progress END AS progress
                FROM jobs WHERE owner<>:gateway
                AND ((requires_ack=1 AND :include_system)
                     OR (requires_ack=0 AND NOT :system_only AND
                         (:owners IS NULL OR owner IN (SELECT value FROM json_each(:owners)))))
                AND (:job_id IS NULL OR id=:job_id)
                ORDER BY created DESC, id DESC LIMIT 35""",
                {"gateway": gateway, "owners": None if owners is None else json.dumps(owners),
                 "job_id": job_id, "include_system": include_system or system_only,
                 "system_only": system_only, "now": time.time()}).fetchall()

    def get_audio(self, owner, job_id):
        with self.connection() as db:
            return db.execute("SELECT audio,media_type FROM jobs WHERE owner=? AND id=?",
                              (owner, job_id)).fetchone()

    def queue_status(self, engine, now=None):
        now = time.time() if now is None else now
        with self.connection() as db:
            row = db.execute("""SELECT
                EXISTS(SELECT 1 FROM worker_heartbeats WHERE engine=:engine AND seen>:recent) AS seen,
                COUNT(CASE WHEN status='queued' OR lease_until<=:now THEN 1 END) AS waiting,
                COUNT(CASE WHEN status='running' AND lease_until>:now THEN 1 END) AS running
                FROM jobs WHERE engine=:engine AND status IN ('queued','running')""",
                             {"engine": engine, "recent": now - 15, "now": now}).fetchone()
        return {"online": bool(row["seen"] or row["running"]),
                "busy": bool(row["running"]), "waiting": row["waiting"]}

    def claim(self, engine, lease_seconds=30, now=None):
        now = time.time() if now is None else now
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""INSERT INTO worker_heartbeats (engine, seen) VALUES (?,?)
                ON CONFLICT(engine) DO UPDATE SET seen=excluded.seen
                WHERE excluded.seen-worker_heartbeats.seen>=5""", (engine, now))
            row = db.execute("""SELECT id,audio,options,archive_group FROM jobs
                WHERE engine=? AND (status='queued' OR (status='running' AND lease_until<=?))
                ORDER BY created,id LIMIT 1""", (engine, now)).fetchone()
            if row is None:
                return None
            token = uuid4().hex
            db.execute("UPDATE jobs SET status='running',claim_token=?,lease_until=?,progress=NULL WHERE id=?",
                       (token, now + lease_seconds, row["id"]))
            return Claim(row["id"], token, row["audio"], json.loads(row["options"]), row["archive_group"])

    def renew(self, claim, lease_seconds):
        now = time.time()
        with self.connection() as db:
            return bool(db.execute("""UPDATE jobs SET lease_until=?
                WHERE id=? AND claim_token=? AND status='running' AND lease_until>?""",
                (now + lease_seconds, claim.job_id, claim.token, now)).rowcount)

    def update_progress(self, claim, progress):
        # A concurrent database write may delay a heartbeat or result. Progress is optional.
        try:
            with self.connection() as db:
                db.execute("PRAGMA busy_timeout=100")
                db.execute("""UPDATE jobs SET progress=?
                    WHERE id=? AND claim_token=? AND status='running' AND lease_until>?""",
                           (json.dumps(progress), claim.job_id, claim.token, time.time()))
        except sqlite3.OperationalError:
            return

    def finish(self, claim, result=None, *, error=None, now=None):
        now = time.time() if now is None else now
        status = "failed" if error else "succeeded"
        result_json = json.dumps(result, ensure_ascii=False) if result is not None else None
        with self.connection() as db:
            finished = db.execute("""UPDATE jobs
                SET status=?,result=?,error=?,finished=?,claim_token=NULL,lease_until=NULL,progress=NULL
                WHERE id=? AND claim_token=? AND status='running' AND lease_until>?""",
                (status, result_json, error, now, claim.job_id, claim.token, now)).rowcount
            if finished:
                # Keep a single failed audio copy per archive recording. The backend retains the original.
                db.execute("""UPDATE jobs SET audio=NULL
                    WHERE status='failed' AND id<>? AND audio IS NOT NULL
                      AND (owner, archive_group)=(SELECT owner, archive_group FROM jobs WHERE id=?)""",
                    (claim.job_id, claim.job_id))
            return bool(finished)

    def acknowledge(self, owner, job_id):
        with self.connection() as db:
            acknowledged = db.execute("""UPDATE jobs SET acknowledged=coalesce(acknowledged,?)
                WHERE owner=? AND id=? AND status='succeeded' AND requires_ack=1 AND result IS NOT NULL""",
                (time.time(), owner, job_id)).rowcount
            if acknowledged:
                db.execute("""DELETE FROM jobs WHERE owner=? AND status='failed'
                    AND archive_group=(SELECT archive_group FROM jobs WHERE id=?)
                    AND finished<=(SELECT finished FROM jobs WHERE id=?)""", (owner, job_id, job_id))
            return bool(acknowledged)

    def retire_archive_groups(self, owner, group_ids):
        deadlines = time.time() + ARCHIVE_RETIREMENT_FENCE_SECONDS
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            # Fence late POSTs from the retired recording's last workflow attempt.
            db.executemany("""INSERT INTO retired_archive_groups (owner, group_id, expires_at) VALUES (?, ?, ?)
                ON CONFLICT(owner, group_id) DO UPDATE SET expires_at=excluded.expires_at""",
                           ((owner, group_id, deadlines) for group_id in group_ids))
            db.execute("""DELETE FROM jobs WHERE owner=? AND requires_ack=1
                AND archive_group IN (SELECT value FROM json_each(?))""", (owner, json.dumps(group_ids)))

    def discard_completed_external_audio(self):
        with self.connection() as db:
            return db.execute("""UPDATE jobs SET audio=NULL
                                 WHERE requires_ack=0 AND status='succeeded' AND audio IS NOT NULL""").rowcount

    def purge(self, retention, max_unclaimed, now=None):
        now = time.time() if now is None else now
        with self.connection() as db:
            db.execute("DELETE FROM retired_archive_groups WHERE expires_at<=?", (now,))
            db.execute(f"DELETE FROM jobs WHERE {EXPIRED_JOBS}",
                       {"cutoff": now - retention, "unclaimed_cutoff": now - max_unclaimed})
