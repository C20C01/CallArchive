import hashlib
import json
from contextlib import closing
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from ..archive.provider import (
    Attachment, ExternalArchive, LookupUnavailable, MatchStatus, VerifyStatus, WriteResult, WriteStatus,
)
from .store import ArchiveStore

EXTENSIONS = {"audio/wav": "wav", "audio/mpeg": "mp3", "audio/flac": "flac"}
LOOKUP_ZONE = timezone(timedelta(hours=8))
RECENT_ARCHIVE_WAIT_SECONDS = 3 * 86400
RECENT_ARCHIVE_LOOKUP_INTERVAL = 3 * 3600
OLDER_ARCHIVE_LOOKUP_INTERVAL = 12 * 3600
ATTACHMENT_RETRY_SECONDS = 60
ATTACHMENT_ATTEMPT_LIMIT = 4
CALL_FIELDS = "coalesce(o.metadata,json_extract(r.metadata, '$.call'))"
WAITING_LOOKUPS = f"""
    SELECT r.id, r.owner, r.call_id, r.status, r.metadata, w.job_id, w.error,
           CASE WHEN w.transcript IS NULL THEN 'queued' ELSE 'delivering' END AS resume_stage,
           w.retry_at, w.lease_until, w.archive_checked_at,
           {CALL_FIELDS} AS call_fields, trim(json_extract({CALL_FIELDS}, '$.callee')) AS callee,
           (SELECT min(occurred_at) FROM recording_events e WHERE e.recording_id=r.id
               AND e.status='needs_input' AND e.sequence>coalesce((
                   SELECT max(sequence) FROM recording_events
                   WHERE recording_id=r.id AND status<>'needs_input'), 0)) AS waiting_since
    FROM recordings r JOIN workflow w ON w.recording_id=r.id
    LEFT JOIN outbound_calls o ON o.call_id=r.call_id
    WHERE r.status='needs_input' AND w.error='archive_target_missing'
          AND json_extract(r.metadata, '$.archive_instance')=:instance
          AND NOT EXISTS (SELECT 1 FROM console_call_removals x WHERE x.call_key=coalesce(r.call_id,r.id))
          AND NOT EXISTS (SELECT 1 FROM outbound_short_calls s WHERE s.call_id=r.call_id)
          AND NOT EXISTS (SELECT 1 FROM outbound_recordings f WHERE f.call_id=r.call_id
              AND f.recording_id IS NULL AND (f.error IS NULL OR f.error='invalid_audio'))
"""


DUE_LOOKUPS = f"""
    SELECT *, CASE WHEN :grouped AND callee<>'' THEN 'callee:' || callee ELSE id END AS lookup_group,
              CASE WHEN retry_at<0 OR archive_checked_at IS NULL THEN retry_at
                   WHEN :now-coalesce(waiting_since,:now)<{RECENT_ARCHIVE_WAIT_SECONDS}
                       THEN archive_checked_at+{RECENT_ARCHIVE_LOOKUP_INTERVAL}
                   ELSE archive_checked_at+{OLDER_ARCHIVE_LOOKUP_INTERVAL} END AS ready_at
    FROM ({WAITING_LOOKUPS})
"""


def attachment_request_id(instance, recording_id, target, kind):
    key = json.dumps([instance, recording_id, target, kind]).encode()
    return hashlib.sha256(key).hexdigest()


class RemoteDelivery:
    def __init__(self, store: ArchiveStore, provider: ExternalArchive):
        self.store = store
        self.provider = provider
        self.lookup_by_callee = getattr(provider, "lookup_by_callee", False)

    def waiting_for_callee(self, owner, callee):
        with closing(self.store.connect()) as db:
            return db.execute(WAITING_LOOKUPS + f"""
                AND r.owner=:owner AND trim(json_extract({CALL_FIELDS}, '$.callee'))=:callee
                ORDER BY w.archive_checked_at DESC, w.retry_at LIMIT 1
            """, {"instance": self.provider.id, "owner": owner, "callee": callee}).fetchone()

    def claim_waiting(self, now, call_key=None):
        parameters = {"instance": self.provider.id, "now": now, "call_key": call_key,
                      "grouped": self.lookup_by_callee}
        with closing(self.store.connect()) as db:
            available = db.execute(
                "SELECT 1 FROM (" + DUE_LOOKUPS + """)
                GROUP BY owner, lookup_group HAVING min(ready_at)<=:now AND max(lease_until)<=:now
                    AND (:call_key IS NULL OR max(call_id=:call_key OR id=:call_key)) LIMIT 1""",
                parameters,
            ).fetchone()
        if available is None:
            return []
        with self.store.transaction() as db:
            groups = {}
            for row in db.execute(DUE_LOOKUPS + " ORDER BY ready_at", parameters):
                groups.setdefault((row["owner"], row["lookup_group"]), []).append(dict(row))
            for rows in groups.values():
                if call_key is not None and not any(call_key in (row["call_id"], row["id"]) for row in rows):
                    continue
                if rows[0]["ready_at"] > now or any(row["lease_until"] > now for row in rows):
                    continue
                token = uuid4().hex
                db.executemany("UPDATE workflow SET lease_until=?, lease_token=? WHERE recording_id=?",
                               [(now + 120, token, row["id"]) for row in rows])
                return [{**row, "lease_token": token} for row in rows]
        return []

    async def recheck_waiting(self, now, call_key=None):
        rows = self.claim_waiting(now, call_key)
        if not rows:
            return False
        unchecked = []
        for item in rows:
            if self.attachments(item):
                self.store.advance(item, item["resume_stage"], occurred_at=now)
            else:
                unchecked.append(item)
        if not unchecked:
            return True
        fields = json.loads(unchecked[0]["call_fields"] or "{}")
        try:
            matches = await self.provider.find_targets(fields)
        except LookupUnavailable as error:
            for item in unchecked:
                if error.retryable:
                    self.store.advance(item, "needs_input", archive_checked_at=now, error="archive_target_missing")
                else:
                    self.store.advance(item, "needs_review", error="archive_lookup_unavailable")
            return True
        for item in unchecked:
            if self.apply_lookup(item, matches, fields, now=now):
                self.store.advance(item, item["resume_stage"], occurred_at=now)
        return True

    def apply_lookup(self, item, matches, fields, *, now):
        if matches.status == MatchStatus.NEEDS_INPUT:
            self.store.advance(item, "needs_input", archive_checked_at=now,
                               archive_lookup_grouped=self.lookup_by_callee and bool(fields.get("callee")),
                               error="archive_target_missing", occurred_at=now)
            return False
        if matches.status == MatchStatus.CONFLICT:
            self.store.advance(item, "needs_review", error="archive_target_conflict")
            return False
        if (matches.status != MatchStatus.MATCHED or not matches.targets
                or any(not isinstance(target, str) or not target or len(target) > 256 for target in matches.targets)):
            self.store.advance(item, "needs_review", error="archive_response_invalid")
            return False
        if self.save_plan(item, matches.targets, self.routing_key(fields)):
            return True
        self.store.advance(item, item["status"], occurred_at=now)
        return False

    @staticmethod
    def current_fields(db, item):
        if item["call_id"]:
            row = db.execute("SELECT metadata FROM outbound_calls WHERE call_id=?", (item["call_id"],)).fetchone()
            if row is not None:
                return json.loads(row["metadata"])
        return json.loads(item["metadata"]).get("call") or {}

    def routing_key(self, fields):
        value = str(fields.get("callee") or "").strip() if self.lookup_by_callee else fields
        return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()

    async def prepare(self, item, *, now):
        metadata = json.loads(item["metadata"])
        if metadata.get("archive_instance") != self.provider.id:
            self.store.advance(item, "needs_review", error="archive_instance_unavailable")
            return False
        with closing(self.store.connect()) as db:
            fields = self.current_fields(db, item)
        key = self.routing_key(fields)
        attachments = self.attachments(item)
        legacy_started = False
        if attachments:
            if all(row["routing_key"] == key for row in attachments):
                return True
            started = any(row["status"] != "pending" or row["failures"] for row in attachments)
            # Older plans did not record the fields used for lookup. Recheck the target set
            # before continuing a partial upload, and keep its existing requests and receipts.
            legacy_started = started and all(row["routing_key"] is None for row in attachments)
            if started and not legacy_started:
                self.store.advance(item, "needs_review", error="archive_routing_changed", occurred_at=now)
                return False
            if not started:
                with self.store.transaction() as db:
                    if not self.owns_lease(db, item):
                        return False
                    db.execute("DELETE FROM remote_attachments WHERE recording_id=?", (item["id"],))
        callee = str(fields.get("callee") or "").strip() if self.lookup_by_callee else ""
        if callee and not item["manual_lookup"] and not legacy_started:
            existing = self.waiting_for_callee(item["owner"], callee)
            if existing:
                self.store.advance(item, "needs_input", retry_at=max(0, existing["retry_at"]),
                                   archive_checked_at=existing["archive_checked_at"], archive_lookup_grouped=True,
                                   error="archive_target_missing", occurred_at=now)
                return False
        try:
            matches = await self.provider.find_targets(fields)
        except LookupUnavailable as error:
            self.store.advance(item, item["status"] if error.retryable else "needs_review",
                               retry_at=now + 10 if error.retryable else 0, error="archive_lookup_unavailable")
            return False
        if legacy_started and (matches.status != MatchStatus.MATCHED
                               or set(matches.targets) != {row["target"] for row in attachments}):
            self.store.advance(item, "needs_review", error="archive_routing_changed", occurred_at=now)
            return False
        return self.apply_lookup(item, matches, fields, now=now)

    async def step(self, item, *, now):
        if not await self.prepare(item, now=now):
            return
        rows = self.attachments(item)

        pending = self.next_attachment(rows)
        if pending is not None and pending["retry_at"] <= now:
            await self.send(item, pending, now=now)
        elif rows:
            self.complete_step(item)

    def next_attachment(self, rows):
        actionable = {"pending", "writing"}
        if self.provider.supports_verification or self.provider.supports_idempotency:
            actionable.add("unknown")
        return min(
            (row for row in rows if row["status"] in actionable and row["retry_at"] is not None),
            key=lambda row: row["retry_at"],
            default=None,
        )

    def attachments(self, item):
        with closing(self.store.connect()) as db:
            return db.execute("SELECT * FROM remote_attachments WHERE recording_id=? ORDER BY rowid",
                              (item["id"],)).fetchall()

    def save_plan(self, item, targets, routing_key):
        with self.store.transaction() as db:
            if not self.owns_lease(db, item) or routing_key != self.routing_key(self.current_fields(db, item)):
                return False
            fields = json.loads(item["metadata"]).get("call") or {}
            started = fields.get("started_at")
            try:
                date = datetime.fromisoformat(started.replace("Z", "+00:00")).astimezone(LOOKUP_ZONE)
                stem = f"{date:%Y-%m-%d_%H-%M-%S}_{item['id'][:12]}"
            except (AttributeError, TypeError, ValueError):
                stem = None
            for target in dict.fromkeys(targets):
                for kind in ("audio", "transcript"):
                    request_id = attachment_request_id(self.provider.id, item["id"], target, kind)
                    db.execute("""INSERT OR IGNORE INTO remote_attachments
                                  (recording_id, target, kind, request_id, filename_stem, routing_key)
                                  VALUES (?, ?, ?, ?, ?, ?)""",
                               (item["id"], target, kind, request_id, stem, routing_key))
            db.execute("UPDATE remote_attachments SET routing_key=? WHERE recording_id=? AND routing_key IS NULL",
                       (routing_key, item["id"]))
            return True

    @staticmethod
    def owns_lease(db, item):
        return db.execute("SELECT 1 FROM workflow WHERE recording_id=? AND lease_token=?",
                          (item["id"], item["lease_token"])).fetchone() is not None

    def set_status(self, item, request_id, status, receipt=None, *, retry_at=0, failed=False, routing_key=None):
        with self.store.transaction() as db:
            if not self.owns_lease(db, item):
                return False
            if routing_key is not None and routing_key != self.routing_key(self.current_fields(db, item)):
                return False
            changed = db.execute("""UPDATE remote_attachments SET status=?, receipt=?, retry_at=?,
                                    failures=failures+? WHERE request_id=?""",
                                 (status, receipt, retry_at, int(failed), request_id)).rowcount
            return bool(changed)

    def complete_step(self, item):
        receipts = {}
        rows = self.attachments(item)
        for row in rows:
            target = receipts.setdefault(row["target"], {})
            target[row["kind"]] = {"status": row["status"], "receipt": row["receipt"]}
        states = {row["status"] for row in rows}
        pending = self.next_attachment(rows)
        retry_at, error = 0, None
        if states == {"confirmed"}:
            status = "archived"
        elif pending is not None:
            status, retry_at = "delivering", pending["retry_at"]
        else:
            status = "needs_review"
            error = "archive_attachment_unknown" if "unknown" in states else "archive_attachment_rejected"
        self.store.advance(item, status, receipts=json.dumps(receipts, ensure_ascii=False),
                           retry_at=retry_at, error=error)

    async def send(self, item, row, *, now):
        request_id, target = row["request_id"], row["target"]
        extension = EXTENSIONS[item["media_type"]]
        if row["kind"] == "audio":
            stem = row["filename_stem"] or "recording"
            attachment = Attachment("audio", f"{stem}.{extension}", item["media_type"], item["audio"])
        else:
            text = json.loads(item["transcript"])["text"].encode("utf-8")
            stem = row["filename_stem"] or "transcript"
            attachment = Attachment("transcript", f"{stem}.txt", "text/plain; charset=utf-8", text)

        if row["status"] in {"writing", "unknown"}:
            verified = None
            if self.provider.supports_verification:
                try:
                    verified = await self.provider.verify_attachment(target, attachment, request_id)
                except (OSError, TimeoutError):
                    pass
            if verified is not None and verified.status == VerifyStatus.CONFIRMED:
                self.set_status(item, request_id, "confirmed", verified.receipt)
                self.complete_step(item)
                return
            absent = verified is not None and verified.status == VerifyStatus.CONFIRMED_ABSENT
            if not absent and not self.provider.supports_idempotency:
                # Inconclusive verification needs a manual decision when replay is unsafe.
                self.set_status(item, request_id, "unknown", row["receipt"], retry_at=None)
                self.complete_step(item)
                return

        if not self.set_status(item, request_id, "writing", routing_key=row["routing_key"]):
            self.store.advance(item, item["status"], occurred_at=now)
            return
        try:
            result = await self.provider.put_attachment(target, attachment, request_id)
        except (OSError, TimeoutError):
            result = WriteResult(WriteStatus.UNKNOWN)
        if result.status == WriteStatus.RETRYABLE:
            stopped = row["failures"] + 1 >= ATTACHMENT_ATTEMPT_LIMIT
            self.set_status(item, request_id, "rejected" if stopped else "pending", result.receipt,
                            retry_at=None if stopped else now + ATTACHMENT_RETRY_SECONDS, failed=True)
        elif result.status == WriteStatus.UNKNOWN:
            self.set_status(item, request_id, "unknown", result.receipt, retry_at=now + ATTACHMENT_RETRY_SECONDS)
        else:
            self.set_status(item, request_id, result.status.value, result.receipt)
        self.complete_step(item)
