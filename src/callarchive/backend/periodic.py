
"""Run the configured outbound and notification schedules inside the backend worker."""

import asyncio
import json
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from functools import partial
from zoneinfo import ZoneInfo

from ..observability import event, logged_as
from .alert_changes import AlertChanges
from .import_outbound import run_import
from .report import run_report
from .scheduling import RecurringSchedule, recurring_slots


LEASE_SECONDS = 300
ALERT_CHANGE_CHECK_SECONDS = 5
HEARTBEAT_SECONDS = 60


def task_scopes(settings):
    outbound = settings.outbound
    source = outbound.active_source or outbound.source
    if outbound.owner and source:
        yield "outbound", json.dumps([outbound.owner, source])
    notices = settings.notifications
    if notices.owner:
        scope = json.dumps([notices.owner, notices.channel, notices.channel_id, notices.destination])
        for task in ("daily", "daily_retry", "alerts"):
            yield task, scope


def next_due(task, now, zone, daily_at):
    local = now.astimezone(zone).replace(second=0, microsecond=0)
    if task == "outbound":
        local = now.astimezone(timezone(timedelta(hours=8))).replace(second=0, microsecond=0)
        interval = 5 if 8 <= local.hour < 22 else 60
        return local + timedelta(minutes=interval - local.minute % interval)
    if task == "alerts":
        return local + timedelta(minutes=5 - (local.minute - 1) % 5)

    daily = datetime.combine(local.date(), daily_at, zone)
    if task == "daily":
        return daily if now < daily else daily + timedelta(days=1)
    retry = local + timedelta(minutes=5 - local.minute % 5)
    if retry.date() != local.date():
        daily += timedelta(days=1)
    first_retry = daily.replace(second=0, microsecond=0) + timedelta(minutes=5 - daily.minute % 5)
    if first_retry.date() != daily.date():
        return None
    return max(retry, first_retry)


def schedule_status(store, settings, *, now=None):
    now = datetime.now(timezone.utc) if now is None else now
    zone = ZoneInfo(settings.notifications.timezone)
    daily_at = settings.notifications.daily_at
    slots = recurring_slots(now, zone, daily_at)
    with closing(store.connect()) as db:
        table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recurring_tasks'").fetchone()
        if table:
            rows = {(row["task"], row["scope"]): row for row in db.execute("SELECT * FROM recurring_tasks")}
        else:
            rows = {}
    for task, scope in task_scopes(settings):
        row = rows.get((task, scope))
        due = slots.get(task)
        next_at = next_due(task, now, zone, daily_at)
        if task == "alerts" and (row is None or row["status"] != "failed"):
            next_at = None
        state = "not_run" if row is None else row["status"]
        if row is not None and state == "running" and row["lease_until"] <= now.timestamp():
            state = "recovery"
        if next_at is not None and state != "running" and due and (row is None or due.timestamp() > row["slot"]):
            next_at = due
        yield dict(task=task, state=state, next_at=next_at, last_success=row["succeeded_at"] if row else None,
                   finished_at=row["finished_at"] if row else None,
                   error=row["error"] if row else None)


class BackendScheduler:
    def __init__(self, settings, store):
        self.schedule = RecurringSchedule(store)
        self.zone = ZoneInfo(settings.notifications.timezone)
        self.daily_at = settings.notifications.daily_at
        modes = {"daily": "scheduled", "daily_retry": "retry", "alerts": "alerts"}
        self.tasks = {
            task: (scope, partial(self.import_outbound, settings, store.database) if task == "outbound"
                   else partial(self.send_report, settings, store.database, modes[task]))
            for task, scope in task_scopes(settings)
        }
        self.running = {}
        self.alert_changes = AlertChanges(store) if "alerts" in self.tasks else None
        self.observed_alert_revision = None
        self.alert_retry_pending = False
        self.heartbeat_due = None

    @staticmethod
    def import_outbound(settings, database, slot, write_guard):
        _, pending = run_import(settings, database=database, scheduled_at=slot, write_guard=write_guard)
        return "download_pending" if pending else None

    @staticmethod
    def send_report(settings, database, mode, slot, write_guard):
        result = run_report(settings, mode, database=database, now=slot, write_guard=write_guard)
        failures = set(result) - {"confirmed"}
        return min(failures) if failures else None

    async def tick(self, now):
        slots = recurring_slots(now, self.zone, self.daily_at)
        for task, (scope, work) in self.tasks.items():
            current = self.running.get(task)
            if current is not None:
                if not current.done():
                    continue
                current.result()
                self.running.pop(task)
            if task == "daily":
                self.observe_daily_gap(scope, now)
            if task not in slots:
                continue
            slot = slots[task]
            revision = None
            if task == "alerts":
                revision, due, failed = self.alert_changes.inspect(scope, int(slot.timestamp()))
                self.alert_retry_pending = failed
                self.observed_alert_revision = revision
                if not due:
                    continue
                slot = now
            claim = self.schedule.claim(task, scope, int(slot.timestamp()), now=now.timestamp(),
                                        lease_seconds=LEASE_SECONDS)
            if claim is not None:
                self.running[task] = asyncio.create_task(self.execute(claim, work, slot, revision=revision))

    def observe_daily_gap(self, scope, now):
        local_day = now.astimezone(self.zone).date()
        previous = datetime.combine(local_day - timedelta(days=1), self.daily_at, self.zone)
        if self.schedule.expire_daily(scope, int(previous.timestamp()), now=now.timestamp()):
            event("scheduled_daily_missed", level="WARNING", report_day=previous.date().isoformat())

    def next_wakeup(self, now):
        deadlines = []
        waiting = [(task, scope) for task, (scope, _) in self.tasks.items() if task not in self.running]
        leases = self.schedule.active_leases(after=now.timestamp()) if waiting else {}
        for task, (scope, _) in self.tasks.items():
            due = next_due(task, now, self.zone, self.daily_at)
            if due is not None and (task != "alerts" or self.alert_retry_pending):
                deadlines.append(due.timestamp())
            lease = leases.get((task, scope)) if task not in self.running else None
            if lease is not None:
                deadlines.append(lease)
        return min(deadlines, default=None)

    async def wait_until_due(self, deadline):
        watch_alerts = self.alert_changes is not None and "alerts" not in self.running
        while True:
            delay = None if deadline is None else max(0, deadline - time.time())
            timeout = delay
            if watch_alerts:
                timeout = ALERT_CHANGE_CHECK_SECONDS if delay is None else min(delay, ALERT_CHANGE_CHECK_SECONDS)
            if self.heartbeat_due is not None:
                pulse = max(0, self.heartbeat_due - time.time())
                timeout = pulse if timeout is None else min(timeout, pulse)
            if self.running:
                done, _ = await asyncio.wait(self.running.values(), timeout=timeout,
                                             return_when=asyncio.FIRST_COMPLETED)
                if done:
                    return
            elif timeout is not None:
                await asyncio.sleep(timeout)
            else:
                await asyncio.Future()
            if not watch_alerts and self.heartbeat_due is None:
                return
            now = time.time()
            if self.heartbeat_due is not None and now >= self.heartbeat_due:
                self.record_heartbeat(now)
            if deadline is not None and now >= deadline:
                return
            if watch_alerts and self.alert_changes.revision() != self.observed_alert_revision:
                return

    def record_heartbeat(self, now):
        self.schedule.heartbeat(now)
        self.heartbeat_due = now + HEARTBEAT_SECONDS

    async def run(self):
        self.record_heartbeat(time.time())
        while True:
            now = datetime.now(timezone.utc)
            await self.tick(now)
            await self.wait_until_due(self.next_wakeup(now))

    async def execute(self, claim, work, slot, *, revision=None):
        def write_guard(db):
            self.schedule.require_lease(db, claim, now=time.time())

        component = "outbound-import" if claim.task == "outbound" else "notification"
        with logged_as(component):
            execution = asyncio.create_task(asyncio.to_thread(work, slot, write_guard))
        owned = True
        error = None
        try:
            while not execution.done():
                await asyncio.wait((execution,), timeout=30)
                if execution.done():
                    break
                owned = self.schedule.renew(claim, now=time.time(), lease_seconds=LEASE_SECONDS) and owned
            try:
                error = execution.result()
            finally:
                if revision is not None and owned:
                    self.alert_changes.checkpoint(claim.scope, revision, write_guard)
        except Exception as failure:
            error = type(failure).__name__
        if owned and self.schedule.finish(claim, now=time.time(), error=error) and error:
            with logged_as(component):
                event("scheduled_task_failed", level="WARNING", task=claim.task, reason=error)
