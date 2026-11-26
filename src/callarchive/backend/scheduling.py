import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from typing import NamedTuple

from ..observability import event


def report_day(now, zone, daily_at):
    local = now.astimezone(zone)
    return local.date() if local.time() >= daily_at else None


def recurring_slots(now, zone, daily_at):
    outbound = now.astimezone(timezone(timedelta(hours=8)))
    outbound_minute = outbound.minute // 5 * 5 if 8 <= outbound.hour < 22 else 0
    local = now.astimezone(zone).replace(second=0, microsecond=0)
    alert = local - timedelta(minutes=(local.minute - 1) % 5)
    slots = {
        "outbound": outbound.replace(minute=outbound_minute, second=0, microsecond=0),
        "alerts": alert,
    }
    daily = datetime.combine(local.date(), daily_at, zone)
    if now >= daily:
        slots["daily"] = daily
    retry = local - timedelta(minutes=local.minute % 5)
    if retry > daily:
        slots["daily_retry"] = retry
    return slots


class ScheduledClaim(NamedTuple):
    task: str
    scope: str
    slot: int
    token: str


class ScheduleLeaseLost(RuntimeError):
    pass


class RecurringSchedule:
    def __init__(self, store):
        self.store = store
        with store.transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS recurring_tasks (
                task TEXT NOT NULL, scope TEXT NOT NULL, slot INTEGER NOT NULL,
                token TEXT, lease_until REAL NOT NULL, status TEXT NOT NULL,
                started_at REAL NOT NULL, finished_at REAL, succeeded_at REAL, error TEXT,
                PRIMARY KEY (task, scope)
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS scheduler_signals (
                name TEXT PRIMARY KEY, observed_at REAL NOT NULL, next_at REAL
            )""")

    def heartbeat(self, at):
        with self.store.transaction() as db:
            db.execute("""
                INSERT INTO scheduler_signals (name,observed_at) VALUES ('heartbeat',?)
                ON CONFLICT(name) DO UPDATE SET observed_at=excluded.observed_at
            """, (at,))

    def record_maintenance(self, at, next_at):
        with self.store.transaction() as db:
            db.execute("""
                INSERT INTO scheduler_signals VALUES ('archive_cleanup',?,?)
                ON CONFLICT(name) DO UPDATE SET observed_at=excluded.observed_at,next_at=excluded.next_at
            """, (at, next_at))

    def claim(self, task, scope, slot, *, now, lease_seconds):
        token = uuid4().hex
        with self.store.transaction() as db:
            changed = db.execute(
                """INSERT INTO recurring_tasks
                   (task,scope,slot,token,lease_until,status,started_at)
                   VALUES (?,?,?,?,?,'running',?)
                   ON CONFLICT(task,scope) DO UPDATE SET
                      slot=excluded.slot, token=excluded.token, lease_until=excluded.lease_until,
                      status='running', started_at=excluded.started_at, finished_at=NULL, error=NULL
                   WHERE recurring_tasks.lease_until<=excluded.started_at
                     AND (recurring_tasks.slot<excluded.slot OR
                          (recurring_tasks.slot=excluded.slot AND recurring_tasks.status='running'))""",
                (task, scope, slot, token, now + lease_seconds, now),
            ).rowcount
        return ScheduledClaim(task, scope, slot, token) if changed else None

    def active_leases(self, *, after):
        with closing(self.store.connect()) as db:
            rows = db.execute(
                """SELECT task, scope, lease_until FROM recurring_tasks
                   WHERE status='running' AND lease_until>?""", (after,)
            ).fetchall()
        return {(row["task"], row["scope"]): row["lease_until"] for row in rows}

    def expire_daily(self, scope, slot, *, now):
        with self.store.transaction() as db:
            return bool(db.execute(
                """UPDATE recurring_tasks SET status='missed', slot=?, token=NULL, lease_until=0,
                          finished_at=?, error='missed_window'
                   WHERE task='daily' AND scope=? AND lease_until<=?
                     AND (slot<? OR (slot=? AND status='running'))""",
                (slot, now, scope, now, slot, slot),
            ).rowcount)

    @staticmethod
    def require_lease(db, claim, *, now):
        if db.execute(
            "SELECT 1 FROM recurring_tasks WHERE task=? AND scope=? AND slot=? AND token=? AND lease_until>?",
            (*claim, now),
        ).fetchone() is None:
            raise ScheduleLeaseLost

    def renew(self, claim, *, now, lease_seconds):
        with self.store.transaction() as db:
            return bool(db.execute(
                """UPDATE recurring_tasks SET lease_until=?
                   WHERE task=? AND scope=? AND slot=? AND token=? AND lease_until>?""",
                (now + lease_seconds, *claim, now),
            ).rowcount)

    def finish(self, claim, *, now, error=None):
        with self.store.transaction() as db:
            return bool(db.execute(
                """UPDATE recurring_tasks
                   SET token=NULL, lease_until=0, finished_at=?, status=?, error=?,
                       succeeded_at=coalesce(?,succeeded_at)
                   WHERE task=? AND scope=? AND slot=? AND token=? AND lease_until>?""",
                (now, "failed" if error else "succeeded", error, None if error else now, *claim, now),
            ).rowcount)


class ScanWindow(NamedTuple):
    start: datetime
    end: datetime


class OutboundSchedule:
    def __init__(self, store, owner, source, *, interval, lookback, overlap=0):
        self.store = store
        self.key = owner, source
        self.interval = interval
        self.lookback = lookback
        self.overlap = overlap
        with store.transaction() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS outbound_schedules (
                    owner TEXT NOT NULL,
                    source TEXT NOT NULL,
                    watermark INTEGER,
                    opened_at INTEGER,
                    opened_until INTEGER,
                    UNIQUE (owner, source)
                )
            """)

    def window(self, now, *, overlap=None, oldest=None):
        overlap = self.overlap if overlap is None else overlap
        end = int(now.timestamp()) // self.interval * self.interval
        with self.store.transaction() as db:
            db.execute("INSERT OR IGNORE INTO outbound_schedules (owner, source) VALUES (?, ?)", self.key)
            row = db.execute("""SELECT watermark, opened_at, opened_until
                                FROM outbound_schedules WHERE owner=? AND source=?""", self.key).fetchone()
            limit = int(oldest.timestamp()) if oldest is not None else None
            if row["opened_until"] is not None and (limit is None or row["opened_until"] > limit):
                start, end = row["opened_at"], row["opened_until"]
                if limit is not None and start < limit:
                    # The old cursor applies to a different window; restart inside the cutoff.
                    start = limit
                    db.execute("UPDATE outbound_schedules SET opened_at=? WHERE owner=? AND source=?",
                               (start, *self.key))
            else:
                watermark = row["watermark"]
                if watermark is not None and end <= watermark:
                    return None
                start = end - max(self.lookback, overlap) if watermark is None else min(watermark, end - overlap)
                if limit is not None:
                    start = max(start, limit)
                if start >= end:
                    return None
                db.execute("""UPDATE outbound_schedules SET opened_at=?, opened_until=?
                              WHERE owner=? AND source=?""", (start, end, *self.key))
        return ScanWindow(datetime.fromtimestamp(start, timezone.utc), datetime.fromtimestamp(end, timezone.utc))

    def finish(self, window):
        start = int(window.start.timestamp())
        end = int(window.end.timestamp())
        with self.store.transaction() as db:
            db.execute("""UPDATE outbound_schedules SET watermark=?, opened_at=NULL, opened_until=NULL
                          WHERE owner=? AND source=? AND opened_at=? AND opened_until=?""",
                       (end, *self.key, start, end))


class OutboundHealth:
    def __init__(self, store, owner, source, *, max_lag, started_at=None):
        self.store = store
        self.key = owner, source
        self.max_lag = max_lag
        self.started_at = time.time() if started_at is None else started_at
        self.stalled = False

    def check(self, *, now=None):
        now = time.time() if now is None else now
        with closing(self.store.connect()) as db:
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='outbound_schedules'").fetchone()
            row = db.execute("SELECT watermark FROM outbound_schedules WHERE owner=? AND source=?",
                             self.key).fetchone() if exists else None
        watermark = row["watermark"] if row else None
        stalled = now - (self.started_at if watermark is None else watermark) > self.max_lag
        if stalled == self.stalled:
            return
        scan_through = (datetime.fromtimestamp(watermark, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        if watermark is not None else None)
        if stalled:
            event("outbound_poll_stalled", level="WARNING", scan_through=scan_through)
        else:
            event("outbound_poll_recovered", scan_through=scan_through)
        self.stalled = stalled
