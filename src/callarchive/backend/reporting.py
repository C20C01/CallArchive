import hashlib
import json
import re
import time
from collections import Counter
from contextlib import closing
from dataclasses import asdict
from datetime import date, datetime, timedelta
from urllib.parse import urlencode, urlsplit, urlunsplit
from uuid import uuid4

from ..notifications import Delivery, Message, NotificationChannel, Receipt
from ..observability import event
from .call_state import CALL_STATE
from .followup_names import caller_key, normalize_followup_names
from .store import ARCHIVE_CACHE_ROWS, ARCHIVE_CACHE_SECONDS, OUTBOUND_ARCHIVE_CLOSURES, ArchiveStore


TRANSCRIPTION_SYSTEM_REASONS = frozenset({"transcription_failed", "transcription_unavailable"})


def admin_reason_family(reasons):
    if TRANSCRIPTION_SYSTEM_REASONS.intersection(reasons):
        return "transcription_system"
    return reasons[0]

class ReportDelivery:
    def __init__(self, store: ArchiveStore, *, page_size=20, workbench_url=None, followup_names=None, zone=None):
        if page_size < 1:
            raise ValueError("Page size must be positive")
        self.store = store
        self.page_size = page_size
        self.workbench_url = workbench_url
        self.followup_names = normalize_followup_names(followup_names)
        self.zone = zone
        with closing(store.connect()) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS report_messages (
                    id TEXT PRIMARY KEY,
                    report_key TEXT NOT NULL,
                    page INTEGER NOT NULL,
                    body TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempt TEXT,
                    expires REAL NOT NULL DEFAULT 0,
                    receipt TEXT,
                    created_at REAL,
                    UNIQUE (report_key, page)
                );
                CREATE TABLE IF NOT EXISTS report_wait_members (
                    recording_id TEXT PRIMARY KEY REFERENCES recordings(id) ON DELETE CASCADE,
                    episode TEXT NOT NULL,
                    observed_through INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS report_wait_cycles (
                    owner TEXT NOT NULL,
                    instance TEXT NOT NULL,
                    callee TEXT NOT NULL,
                    episode TEXT NOT NULL,
                    observed_through INTEGER NOT NULL,
                    PRIMARY KEY (owner, instance, callee)
                );
            """)
        with store.transaction() as db:
            if "created_at" not in {row["name"] for row in db.execute("PRAGMA table_info(report_messages)")}:
                db.execute("ALTER TABLE report_messages ADD COLUMN created_at REAL")
                db.execute("UPDATE report_messages SET created_at=?", (time.time(),))

    def redact_snapshots(self, *, now=None):
        now = time.time() if now is None else now
        with self.store.transaction() as db:
            # Waiting episodes already have their own opaque identity. Old keys also stored the number.
            old_keys = db.execute(
                """SELECT id, report_key FROM report_messages WHERE report_key LIKE '["alert-call",%'
                   AND json_array_length(report_key)=8 AND json_extract(report_key, '$[4]')='waiting'"""
            ).fetchall()
            for row in old_keys:
                key = json.loads(row["report_key"])
                normalized = json.dumps([*key[:5], key[7]], ensure_ascii=False)
                db.execute("UPDATE report_messages SET report_key=? WHERE id=?", (normalized, row["id"]))

            active_calls = {}

            def has_retained_call(issue):
                call_id = issue.get("call_id") or issue.get("recording_id")
                if not call_id:
                    return True
                if call_id not in active_calls:
                    active_calls[call_id] = db.execute(
                        f"""SELECT NOT EXISTS (SELECT 1 FROM console_call_removals WHERE call_key=:call)
                                AND (EXISTS (SELECT 1 FROM recordings r
                                             WHERE (r.call_id=:call OR r.id=:call) AND r.status<>'removed'
                                               AND (r.status<>'archived' OR NOT EXISTS (
                                                   SELECT 1 FROM ({ARCHIVE_CACHE_ROWS}) cache
                                                   WHERE cache.recording_id=r.id AND cache.archived_at<=:cutoff)))
                                  OR EXISTS (SELECT 1 FROM outbound_recordings
                                             WHERE call_id=:call AND recording_id IS NULL))""",
                        {"call": call_id, "cutoff": now - ARCHIVE_CACHE_SECONDS},
                    ).fetchone()[0]
                return active_calls[call_id]

            for row in db.execute(
                """SELECT id, report_key, body, status, created_at FROM report_messages
                   WHERE expires<=? AND json_type(body, '$.id') IS NOT NULL""", (now,),
            ):
                message = json.loads(row["body"])
                content = message.get("content", {})
                alert = content.get("issue")
                aged = row["created_at"] is not None and row["created_at"] <= now - ARCHIVE_CACHE_SECONDS
                if (aged and row["status"] in {"confirmed", "unknown"}) or (alert and not has_retained_call(alert)):
                    # The legacy-alert migration needs these classifications to preserve deduplication.
                    message = {}
                    if alert and row["report_key"].startswith('["alert",'):
                        message = {"content": {"issue": {key: alert.get(key) for key in ("category", "reason")}}}
                elif "issues" in content:
                    issues = [issue for issue in content["issues"] if has_retained_call(issue)]
                    if len(issues) == len(content["issues"]):
                        continue
                    content["issues"] = issues
                    assignees = {issue.get("assignee") for issue in issues if issue["category"] == "follow_up"}
                    for key in ("continued_followups", "followups_with_more"):
                        if key in content:
                            content[key] = [name for name in content[key] if name in assignees]
                else:
                    continue
                db.execute("UPDATE report_messages SET body=? WHERE id=?",
                           (json.dumps(message, ensure_ascii=False), row["id"]))

    def pending_url(self, **filters):
        if not self.workbench_url:
            return None
        origin = urlsplit(self.workbench_url)
        host = origin.netloc.rsplit("@", 1)[-1]
        return urlunsplit((origin.scheme, host, "/calls/pending", urlencode(filters), ""))

    def assignee(self, issue):
        caller = issue.get("caller")
        return self.followup_names.get(caller_key(caller), caller)

    def deliver(self, day: date, owner: str, channel_id: str, destination: str,
                channel: NotificationChannel, *, archive_window=None, replay_rejected=False,
                fresh_only=False) -> dict[str, int]:
        self.redact_snapshots()
        key = json.dumps([day.isoformat(), owner, channel_id, destination], ensure_ascii=False)
        if replay_rejected:
            with closing(self.store.connect()) as db:
                pages = db.execute("SELECT id FROM report_messages WHERE report_key=? ORDER BY page",
                                   (key,)).fetchall()
            return self.dispatch(pages, channel, replay_rejected=True)
        fits = getattr(channel, "fits", None)
        pages = self.snapshot(key, day, owner, destination, archive_window, fits=fits)
        return self.dispatch(pages, channel, fresh_only=fresh_only)


    def retry_report(self, day, owner, channel_id, destination, channel):
        """Replay the stored pages for today's explicit transient rejections."""
        self.redact_snapshots()
        key = json.dumps([day.isoformat(), owner, channel_id, destination], ensure_ascii=False)
        retryable = getattr(channel, "retryable_rejections", ())
        if not retryable:
            return {}
        with closing(self.store.connect()) as db:
            pages = [row for row in db.execute(
                """SELECT id, receipt FROM report_messages
                   WHERE report_key=? AND status='rejected' ORDER BY page""", (key,),
            ) if row["receipt"] in retryable]
        return self.dispatch(pages, channel, retry_only=True)

    def deliver_alerts(self, owner: str, channel_id: str, destination: str,
                       channel: NotificationChannel, *, replay_rejected=False) -> dict[str, int]:
        self.redact_snapshots()
        with self.store.transaction() as db:
            _, issues = collect_issues(db, owner, zone=self.zone)
            cohorts = {}
            for issue in issues:
                call_id = issue["call_id"]
                if issue["category"] == "follow_up" and (issue.get("callee") or "").strip() and (
                        "archive_target_missing" in issue["reasons"]):
                    identity = ("waiting", issue.get("archive_instance") or "", issue["callee"].strip())
                elif issue["category"] == "admin":
                    identity = ("admin", call_id, admin_reason_family(issue["reasons"]))
                else:
                    identity = (issue["category"], call_id)
                cohorts.setdefault(identity, issue)
            episodes = self.waiting_episode_keys(db, owner, cohorts)
            legacy = self.legacy_alerts(db, owner, channel_id, destination) if cohorts else {}
            pages = []
            for identity, issue in cohorts.items():
                episode, started = episodes.get(identity, (None, 0))
                key_parts = ("waiting", episode) if identity[0] == "waiting" else (*identity, episode)
                key = json.dumps(["alert-call", owner, channel_id, destination, *key_parts], ensure_ascii=False)
                select = """SELECT id, status, json_type(body, '$.id') AS has_body
                            FROM report_messages WHERE report_key=?"""
                page = db.execute(select, (key,)).fetchone()
                if page is None:
                    previous = next((row for row in legacy.get(identity, ())
                                     if (row["revision"] or 0) >= started and
                                     (identity[0] == "admin" or row["reason"] in issue["reasons"])), None)
                    if previous is not None:
                        db.execute("UPDATE report_messages SET report_key=? WHERE id=?", (key, previous["id"]))
                        page = db.execute(select, (key,)).fetchone()
                if page is not None and (page["has_body"] or page["status"] not in {"pending", "rejected"}):
                    pages.append(page)
                    continue
                if page is None and replay_rejected:
                    continue
                assignee = self.assignee(issue)
                if issue["category"] == "follow_up":
                    title = f"{assignee} · 待跟进" if assignee else "待跟进"
                else:
                    title = "归档异常 · 管理员事项"
                mobile = str(issue.get("callee") or "").strip()
                filters = {"mobile": mobile} if re.fullmatch(r"\+?[0-9]{1,20}", mobile) else {}
                if assignee and len(assignee) <= 100:
                    filters["owner"] = assignee
                link = self.pending_url(**filters)
                message_id = page["id"] if page else hashlib.sha256(key.encode()).hexdigest()
                body = json.dumps({
                    "id": message_id, "destination": destination, "title": title,
                    "content": {"kind": "alert", "issue": issue, "details_url": link},
                }, ensure_ascii=False)
                if page is None:
                    db.execute("""INSERT INTO report_messages (id, report_key, page, body, created_at)
                                  VALUES (?, ?, 1, ?, ?)""", (message_id, key, body, time.time()))
                else:
                    db.execute("UPDATE report_messages SET body=?, created_at=? WHERE id=?",
                               (body, time.time(), message_id))
                pages.append(db.execute(select, (key,)).fetchone())
        return self.dispatch(pages, channel, replay_rejected=replay_rejected)


    @staticmethod
    def legacy_alerts(db, owner, channel_id, destination):
        alerts = {}
        for row in db.execute(
            """SELECT m.id, coalesce(r.call_id,r.id) AS call_id,
                      coalesce(json_extract(r.metadata, '$.archive_instance'),'') AS instance,
                      trim(coalesce(json_extract(o.metadata, '$.callee'),json_extract(r.metadata, '$.call.callee'))) AS callee,
                      json_extract(m.report_key,'$[5]') AS revision,
                      json_extract(m.body,'$.content.issue.category') AS category,
                      json_extract(m.body,'$.content.issue.reason') AS reason
               FROM report_messages m JOIN recordings r ON r.id=json_extract(m.report_key,'$[4]')
               LEFT JOIN outbound_calls o ON o.call_id=r.call_id
               WHERE m.report_key LIKE '["alert",%' AND r.owner=?
                 AND json_extract(m.report_key,'$[2]')=? AND json_extract(m.report_key,'$[3]')=?
                 AND m.status<>'pending'
               ORDER BY CASE m.status WHEN 'confirmed' THEN 0 WHEN 'unknown' THEN 1
                                      WHEN 'sending' THEN 1 ELSE 2 END, m.rowid""",
            (owner, channel_id, destination),
        ):
            identity = (row["category"], row["call_id"])
            if row["category"] == "follow_up" and row["reason"] == "archive_target_missing" and row["callee"]:
                identity = ("waiting", row["instance"], row["callee"])
            elif row["category"] == "admin":
                identity = ("admin", row["call_id"], admin_reason_family([row["reason"]]))
            alerts.setdefault(identity, []).append(row)
        return alerts

    @staticmethod
    def waiting_episode_keys(db, owner, cohorts):
        existing = {(row["instance"], row["callee"]): row for row in db.execute(
            "SELECT * FROM report_wait_cycles WHERE owner=?", (owner,),
        )}
        active = {identity for identity in cohorts if identity[0] == "waiting"}
        previous_members = {row["recording_id"]: row for row in db.execute(
            """SELECT m.* FROM report_wait_members m JOIN recordings r ON r.id=m.recording_id
               WHERE r.owner=?""", (owner,),
        )}
        if not active and not existing and not previous_members:
            return {}
        inactive_members = set(previous_members)
        watermark = db.execute("SELECT coalesce(max(sequence),0) FROM recording_events").fetchone()[0]
        episodes = {}
        for identity in sorted(active):
            _, instance, callee = identity
            members = {}
            started = 0
            for row in db.execute(
                """SELECT e.sequence, e.recording_id, e.status, e.error,
                          trim(json_extract(r.metadata, '$.call.callee')) AS original_callee
                   FROM recording_events e JOIN recordings r ON r.id=e.recording_id
                   LEFT JOIN outbound_calls o ON o.call_id=r.call_id
                   WHERE r.owner=? AND coalesce(json_extract(r.metadata, '$.archive_instance'),'')=?
                     AND trim(coalesce(json_extract(o.metadata, '$.callee'),
                                       json_extract(r.metadata, '$.call.callee')))=?
                     AND (e.status<>'needs_input' OR e.error IS NOT NULL)
                   ORDER BY e.sequence""", (owner, instance, callee),
            ):
                if row["status"] == "needs_input" and row["error"] == "archive_target_missing":
                    if not members:
                        started = row["sequence"]
                    members.setdefault(row["recording_id"], (row["sequence"], row["original_callee"]))
                else:
                    members.pop(row["recording_id"], None)
            previous = existing.get((instance, callee))
            continuous = []
            if previous is not None and started and started <= previous["observed_through"]:
                continuous.append((started, previous["episode"]))
            for recording_id, (sequence, original_callee) in members.items():
                known = previous_members.get(recording_id)
                if known is None:
                    # Upgrade accounts that only have the prior number-keyed record.
                    known = existing.get((instance, original_callee))
                if known is not None and sequence <= known["observed_through"]:
                    continuous.append((sequence, known["episode"]))
            episode = min(continuous)[1] if continuous else uuid4().hex
            inactive_members.difference_update(members)
            db.executemany(
                """INSERT INTO report_wait_members VALUES (?,?,?)
                   ON CONFLICT(recording_id) DO UPDATE SET episode=excluded.episode,
                       observed_through=excluded.observed_through""",
                [(recording_id, episode, watermark) for recording_id in members],
            )
            episodes[identity] = episode, started
            db.execute(
                """INSERT INTO report_wait_cycles (owner, instance, callee, episode, observed_through)
                   VALUES (?, ?, ?, ?, ?) ON CONFLICT (owner,instance,callee) DO UPDATE SET
                   episode=excluded.episode, observed_through=excluded.observed_through""",
                (owner, instance, callee, episode, watermark),
            )
        db.executemany("DELETE FROM report_wait_cycles WHERE owner=? AND instance=? AND callee=?",
                       [(owner, instance, callee) for instance, callee in existing
                        if ("waiting", instance, callee) not in active])
        db.executemany("DELETE FROM report_wait_members WHERE recording_id=?",
                       [(recording_id,) for recording_id in inactive_members])
        return episodes

    def dispatch(self, pages, channel, *, retry_only=False, replay_rejected=False, fresh_only=False):
        for page in pages:
            claim = self.claim(page, channel, retry_only=retry_only, replay_rejected=replay_rejected,
                               fresh_only=fresh_only)
            if claim is None:
                continue
            attempt, body = claim
            event_name = "report_retry_delivery" if retry_only or replay_rejected else "report_delivery"
            details = {}
            try:
                message = Message(**json.loads(body))
                if message.content.get("issue") is not None:
                    message.content.setdefault("kind", "alert")
                    event_name = "alert_delivery"
                    details["recording_id"] = message.content.get("issue", {}).get("recording_id")
                else:
                    details.update(report_day=message.content.get("day"), page=message.content.get("page"),
                                   total_pages=message.content.get("total_pages"))
                receipt = channel.send(message)
                if receipt.reason:
                    details["reason"] = receipt.reason
            except Exception as error:
                details["reason"] = type(error).__name__
                receipt = Receipt(Delivery.UNKNOWN)
            with self.store.transaction() as db:
                db.execute("""UPDATE report_messages SET status=?, receipt=?, attempt=NULL, expires=0
                              WHERE id=? AND attempt=?""",
                           (receipt.status.value, receipt.reference, page["id"], attempt))
            event(event_name, message_id=page["id"], status=receipt.status.value,
                  level="INFO" if receipt.status == Delivery.CONFIRMED else "WARNING", **details)
        with closing(self.store.connect()) as db:
            return dict(Counter(db.execute("SELECT status FROM report_messages WHERE id=?", (page["id"],))
                                .fetchone()["status"] for page in pages))

    def claim(self, page, channel, *, retry_only=False, replay_rejected=False, fresh_only=False):
        now = time.time()
        retryable = getattr(channel, "retryable_rejections", ())
        with self.store.transaction() as db:
            current = db.execute("""SELECT status, expires, receipt, body, json_type(body, '$.id') AS has_body
                                    FROM report_messages WHERE id=?""", (page["id"],)).fetchone()
            if current["status"] == "confirmed" or current["expires"] > now:
                return None
            if fresh_only and current["status"] != "pending":
                if current["status"] == "sending":
                    db.execute("UPDATE report_messages SET status='unknown', attempt=NULL, expires=0 WHERE id=?",
                               (page["id"],))
                return None
            if (retry_only or replay_rejected) and current["status"] != "rejected":
                return None
            if (current["status"] == "rejected" and not replay_rejected
                    and current["receipt"] not in retryable):
                return None
            if current["status"] in {"sending", "unknown"} and not channel.supports_idempotency:
                db.execute("UPDATE report_messages SET status='unknown', attempt=NULL, expires=0 WHERE id=?",
                           (page["id"],))
                return None
            if not current["has_body"]:
                return None
            attempt = uuid4().hex
            db.execute("UPDATE report_messages SET status='sending', attempt=?, expires=? WHERE id=?",
                       (attempt, now + 120, page["id"]))
            return attempt, current["body"]

    def snapshot(self, key, day, owner, destination, archive_window, *, fits=None):
        select = "SELECT id FROM report_messages WHERE report_key=? ORDER BY page"
        with self.store.transaction() as db:
            existing = db.execute(select, (key,)).fetchall()
            if existing:
                return existing
            waiting, issues = collect_issues(db, owner, zone=self.zone)
            archived = None
            start = day - timedelta(days=1)
            if archive_window is not None:
                # The closure query has one row per completed call, even for multiple segments.
                archived = db.execute(f"""WITH closures AS ({OUTBOUND_ARCHIVE_CLOSURES})
                    SELECT count(*) FROM closures c JOIN outbound_calls o USING (call_id)
                    WHERE c.owner=? AND c.closed_at>? AND c.closed_at<=?
                      AND NOT EXISTS (SELECT 1 FROM console_call_removals r WHERE r.call_key=c.call_id)
                      AND NOT EXISTS (SELECT 1 FROM outbound_short_calls s WHERE s.call_id=c.call_id)
                      AND NOT EXISTS (SELECT 1 FROM outbound_import_exclusions x WHERE x.owner=o.owner
                                      AND x.source=o.source AND x.kind='call' AND x.identifier=o.external_id)
                    """, (owner, *archive_window)).fetchone()[0]
            for issue in issues:
                if issue["category"] == "follow_up":
                    assignee = self.assignee(issue)
                    issue["assignee"] = assignee
                    issue["owner_url"] = (self.pending_url(owner=assignee) if assignee
                                          else self.pending_url(status="waiting_applicant"))
            issues.sort(key=lambda issue: (issue["category"] != "follow_up", issue.get("assignee") or "", issue["reason"]))
            followup_count = sum(issue["category"] == "follow_up" for issue in issues)
            groups = [issues[index:index + self.page_size] for index in range(0, len(issues), self.page_size)] or [[]]
            def messages():
                last_page = {(issue["category"], issue.get("assignee")): page
                             for page, group in enumerate(groups, start=1) for issue in group}
                seen_followups = set()
                seen_admin = False
                for page, group in enumerate(groups, start=1):
                    assignees = {issue.get("assignee") for issue in group if issue["category"] == "follow_up"}
                    has_admin = any(issue["category"] == "admin" for issue in group)
                    message_id = hashlib.sha256(json.dumps([key, page]).encode()).hexdigest()
                    content = {
                        "day": day.isoformat(), "page": page, "total_pages": len(groups),
                        "waiting_recordings": waiting if page == len(groups) else None,
                        "archived_calls": archived if page == 1 else None,
                        "followup_count": followup_count if page == 1 else None,
                        "admin_count": len(issues) - followup_count if page == 1 else None,
                        "total_issues": len(issues), "issues": group,
                        "continued_followups": sorted(assignees & seen_followups, key=lambda name: name or ""),
                        "admin_continued": has_admin and seen_admin,
                        "followups_with_more": sorted(
                            (name for name in assignees if last_page["follow_up", name] > page),
                            key=lambda name: name or "",
                        ),
                        "admin_has_more": has_admin and last_page["admin", None] > page,
                        "workbench_url": self.pending_url(),
                        "admin_url": self.pending_url(status="attention"),
                    }
                    seen_followups.update(assignees)
                    seen_admin |= has_admin
                    yield Message(message_id, destination, f"归档日报 · {start:%m/%d}—{day:%m/%d}", content)

            while True:
                pages = list(messages())
                oversized = next((index for index, message in enumerate(pages) if fits and not fits(message)), None)
                if oversized is None:
                    break
                group = groups[oversized]
                if len(group) < 2:
                    raise ValueError("One report entry exceeds the notification card budget")
                midpoint = len(group) // 2
                groups[oversized:oversized + 1] = [group[:midpoint], group[midpoint:]]
            for page, message in enumerate(pages, start=1):
                body = json.dumps(asdict(message), ensure_ascii=False)
                db.execute("""INSERT INTO report_messages (id, report_key, page, body, created_at)
                              VALUES (?, ?, ?, ?, ?)""", (message.id, key, page, body, time.time()))
            return db.execute(select, (key,)).fetchall()


def local_started_at(value, zone):
    if not value or zone is None:
        return value
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp.astimezone(zone).isoformat() if stamp.tzinfo else value
    except ValueError:
        return value


def collect_issues(db, owner, *, zone=None):
    params = {"owner": owner, "now": time.time()}
    waiting = db.execute(CALL_STATE + """
        SELECT count(*) FROM summaries WHERE owner=:owner AND waiting_recording
    """, params).fetchone()[0]

    rows = db.execute(CALL_STATE + """
        SELECT f.id AS recording_id, f.call_key, f.metadata, f.category, f.reason,
               s.call, s.status AS call_status, s.downloaded_duration, 0 AS source, f.position
        FROM recording_facts f JOIN summaries s USING (call_key)
        WHERE f.owner=:owner AND f.category IS NOT NULL
          AND (f.category<>'follow_up' OR s.status<>'recording_invalid')
        UNION ALL
        SELECT NULL, s.call_key, f.metadata, 'admin', f.reason,
               s.call, s.status, s.downloaded_duration, 1, f.position
        FROM source_facts f JOIN summaries s USING (call_key)
        WHERE s.owner=:owner AND f.reason IS NOT NULL
        ORDER BY source, position
    """, params)

    grouped = {}
    for row in rows:
        key = row["call_key"], row["category"]
        if key not in grouped:
            metadata = json.loads(row["metadata"])
            fallback = metadata if row["source"] else (metadata.get("call") or {})
            call = {**fallback, **json.loads(row["call"])}
            grouped[key] = {
                "recording_id": row["recording_id"], "call_id": row["call_key"],
                "archive_instance": metadata.get("archive_instance") if not row["source"] else None,
                "caller": call.get("caller"), "callee": call.get("callee"),
                "started_at": local_started_at(call.get("started_at"), zone),
                "category": row["category"], "reasons": set(),
                "recording_duration_ms": (None if row["call_status"] == "recording_invalid"
                                          else row["downloaded_duration"]),
            }
        grouped[key]["reasons"].add(row["reason"])

    for issue in grouped.values():
        issue["reasons"] = sorted(issue["reasons"])
        issue["reason"] = issue["reasons"][0]
    return waiting, list(grouped.values())
