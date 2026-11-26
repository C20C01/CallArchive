import hashlib
import json
import shutil
from collections import defaultdict
from contextlib import closing
from datetime import datetime, time, timedelta, timezone
from uuid import uuid4

from fastapi import HTTPException

from ..http import inspect_recording
from ..observability import event
from ..outbound import CallRecord, CursorExpired, OutboundSource, RecordingRef
from .call_state import DOWNLOAD_ATTEMPT_LIMIT
from .store import ArchiveStore, RemovalUnavailable, SubmissionConflict


DEFAULT_MIN_FREE_MIB = 1024
FOLLOWUP_INTERVAL_SECONDS = 20 * 60
OLDER_FOLLOWUP_INTERVAL_SECONDS = 3 * 3600
FOLLOWUP_WINDOWS_PER_POLL = 12
_CURSOR_RESTARTED = object()


def outbound_request_id(call_id, external_id):
    key = json.dumps((call_id, external_id)).encode()
    return "outbound:" + hashlib.sha256(key).hexdigest()


class OutboundImport:
    def __init__(self, store: ArchiveStore, owner: str, source_id: str, source: OutboundSource,
                 *, max_bytes=25 * 1024**2, archive_instance=None, skip_call_duration_ms=None,
                 min_free_mib=DEFAULT_MIN_FREE_MIB, recording_followup_interval_seconds=FOLLOWUP_INTERVAL_SECONDS,
                 recording_followup_windows_per_poll=FOLLOWUP_WINDOWS_PER_POLL):
        self.store = store
        self.owner = owner
        self.source_id = source_id
        self.source = source
        self.max_bytes = max_bytes
        self.min_free_bytes = min_free_mib * 1024**2
        self.recording_followup_interval = recording_followup_interval_seconds
        self.recording_followup_limit = recording_followup_windows_per_poll
        self.archive_instance = archive_instance
        self.skip_call_duration_ms = skip_call_duration_ms
        self.calls_returned = 0
        self.scanned_call_ids = set()

    def scan_page(self, start, end, *, page_size=100, scan_cutoff=None, seen_cursors=None):
        if start.utcoffset() is None or end.utcoffset() is None or start >= end or page_size < 1:
            raise ValueError("Expected a valid time range and positive page size")
        window = (self.owner, self.source_id, start.astimezone(timezone.utc).isoformat(),
                  end.astimezone(timezone.utc).isoformat())
        query = """SELECT cursor, done FROM outbound_windows
                   WHERE owner = ? AND source = ? AND starts = ? AND ends = ?"""
        with closing(self.store.connect()) as db:
            previous = db.execute(query, window).fetchone()
        if previous and previous["done"]:
            return True
        cursor = previous["cursor"] if previous else None
        # Track request cursors within this reading of the window.
        if seen_cursors is not None:
            if cursor in seen_cursors:
                raise ValueError("Outbound provider repeated a page cursor")
            seen_cursors.add(cursor)
        try:
            page = self.source.list_calls(start, end, cursor, page_size)
            self.calls_returned += len(page.calls)
        except CursorExpired:
            if cursor is None:
                raise ValueError("Outbound provider rejected the initial cursor.") from None
            with self.store.transaction() as db:
                reset = db.execute("""UPDATE outbound_windows SET cursor=NULL
                                      WHERE owner=? AND source=? AND starts=? AND ends=? AND done=0 AND cursor=?""",
                                   (*window, cursor)).rowcount
            if reset and seen_cursors is not None:
                if _CURSOR_RESTARTED in seen_cursors:
                    raise ValueError("Outbound cursor expired again after restarting the window") from None
                seen_cursors.clear()
                seen_cursors.add(_CURSOR_RESTARTED)
            return False
        if page.cursor is not None and (
            page.cursor == cursor or (seen_cursors is not None and page.cursor in seen_cursors)
        ):
            raise ValueError("Outbound cursor did not advance")
        if any(not start <= call.started_at < end for call in page.calls):
            raise ValueError("Call lies outside the requested window")
        with self.store.transaction() as db:
            current = db.execute(query, window).fetchone()
            if current != previous:
                return bool(current and current["done"])
            # Cleanup can commit while the upstream page is in flight.
            # Check the current cutoff while holding the same write lock as cleanup.
            cutoff = scan_cutoff(db) if scan_cutoff is not None else None
            for call in page.calls:
                if cutoff is not None and call.started_at.timestamp() <= cutoff:
                    continue
                keys = json.dumps([call.external_call_id, *call.source_aliases])
                excluded = db.execute("""
                    SELECT 1 FROM outbound_import_exclusions
                    WHERE owner=? AND source=? AND kind='call'
                    AND identifier IN (SELECT value FROM json_each(?)) LIMIT 1
                """, (self.owner, self.source_id, keys)).fetchone()
                if excluded:
                    db.execute("""
                        UPDATE outbound_import_exclusions SET call_started_at=?
                        WHERE owner=? AND source=? AND kind='call' AND call_started_at IS NULL
                        AND identifier IN (SELECT value FROM json_each(?))
                    """, (call.started_at.timestamp(), self.owner, self.source_id, keys))
                    continue
                self.scanned_call_ids.add(self.save_call_snapshot(db, call))
            done = page.cursor is None
            db.execute(
                """INSERT INTO outbound_windows VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT (owner, source, starts, ends)
                   DO UPDATE SET cursor = excluded.cursor, done = excluded.done""",
                (*window, page.cursor, done),
            )
            return done

    def save_call_snapshot(self, db, call, current=None):
        metadata = call.model_dump(mode="json", exclude={"recordings", "recordings_complete", "source_aliases"})
        external_id = call.external_call_id
        aliases = call.source_aliases
        if current is not None:
            # Keep the lookup key stable while retaining any newly resolved source identifier.
            external_id = current["external_id"]
            aliases = [*aliases, call.external_call_id]
            metadata["external_call_id"] = external_id
            previous = json.loads(current["metadata"])
            if "_archive_instance" in previous:
                metadata["_archive_instance"] = previous["_archive_instance"]
        elif self.archive_instance:
            metadata["_archive_instance"] = self.archive_instance
        duration = metadata.get("call_duration_ms")
        short = (self.skip_call_duration_ms is not None and duration is not None
                 and duration <= self.skip_call_duration_ms)
        entry = self.save_call(db, external_id, metadata, short or call.recordings_complete, source_aliases=aliases)
        if short:
            db.execute("INSERT OR IGNORE INTO outbound_short_calls VALUES (?)", (entry["call_id"],))
            db.executemany("""INSERT OR IGNORE INTO outbound_import_exclusions
                (owner,source,kind,identifier,call_started_at) VALUES (?,?,'recording',?,?)""",
                [(self.owner, self.source_id, recording.id, call.started_at.timestamp())
                 for recording in call.recordings])
        else:
            self.save_recordings(db, entry, call.recordings)
        return entry["call_id"]

    def save_call(self, db, external_id, metadata, complete, *, source_aliases=()):
        key = self.owner, self.source_id, external_id
        call = db.execute("SELECT * FROM outbound_calls WHERE owner=? AND source=? AND external_id=?",
                          key).fetchone()
        if call is None and source_aliases:
            aliases = db.execute("""
                SELECT * FROM outbound_calls WHERE call_id IN (
                    SELECT call_id FROM outbound_call_aliases WHERE owner=? AND source=?
                    AND identifier IN (SELECT value FROM json_each(?))
                ) LIMIT 2
            """, (self.owner, self.source_id, json.dumps(source_aliases))).fetchall()
            if len(aliases) > 1:
                raise ValueError("Outbound call aliases match multiple existing calls.")
            if aliases:
                call = aliases[0]
        if call is None:
            serialized = json.dumps(metadata, ensure_ascii=False)
            call_id = uuid4().hex
            db.execute("INSERT INTO calls (id, owner) VALUES (?, ?)", (call_id, self.owner))
            db.execute("""INSERT INTO outbound_calls (owner, source, external_id, call_id, metadata, complete)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                       (*key, call_id, serialized, complete))
        else:
            previous = json.loads(call["metadata"])
            previous.pop("_archive_instance", None)
            previous.update({key: value for key, value in metadata.items() if value is not None or key not in previous})
            serialized = json.dumps(previous, ensure_ascii=False)
            db.execute("""UPDATE outbound_calls SET external_id=?,metadata=?,complete=max(complete, ?)
                          WHERE call_id=?""", (external_id, serialized, complete, call["call_id"]))
        entry = db.execute("SELECT * FROM outbound_calls WHERE owner=? AND source=? AND external_id=?",
                           key).fetchone()
        db.executemany("""INSERT OR IGNORE INTO outbound_call_aliases
            (owner,source,identifier,call_id) VALUES (?,?,?,?)""",
            [(self.owner, self.source_id, alias, entry["call_id"]) for alias in source_aliases if alias != external_id])
        return entry

    @staticmethod
    def save_recordings(db, entry, recordings):
        db.executemany(
            """INSERT INTO outbound_recordings (call_id, external_id, reference, metadata)
               SELECT ?, ?, ?, ? WHERE NOT EXISTS (
                   SELECT 1 FROM outbound_import_exclusions
                   WHERE owner=? AND source=? AND kind='recording' AND identifier=?
               ) ON CONFLICT (call_id, external_id) DO UPDATE
               SET reference = excluded.reference
               WHERE outbound_recordings.recording_id IS NULL""",
            [(entry["call_id"], rec.id, rec.reference, entry["metadata"],
              entry["owner"], entry["source"], rec.id) for rec in recordings],
        )

    def refresh_waiting(self, *, now=None, scanned_window=None, only_call=None):
        with closing(self.store.connect()) as db:
            waiting = db.execute(
                """SELECT c.*, q.call_id IS NOT NULL AND q.recordings_checked=0 AS forced
                   FROM outbound_calls c LEFT JOIN outbound_console_retries q USING (call_id)
                   WHERE c.owner=? AND c.source=? AND c.complete=0
                   AND (? IS NULL OR c.call_id=?)
                   AND c.call_id NOT IN (SELECT call_key FROM console_call_removals)
                   ORDER BY q.call_id IS NOT NULL DESC""",
                (self.owner, self.source_id, only_call, only_call),
            ).fetchall()
        now = now or datetime.now(timezone.utc)
        due = []
        for entry in waiting:
            occurred = datetime.fromisoformat(json.loads(entry["metadata"])["started_at"].replace("Z", "+00:00"))
            if occurred >= now or (not entry["forced"] and scanned_window
                                   and scanned_window.start <= occurred < scanned_window.end):
                continue
            interval = (self.recording_followup_interval if now - occurred < timedelta(days=1)
                        else OLDER_FOLLOWUP_INTERVAL_SECONDS)
            if entry["forced"] or entry["recordings_checked_at"] <= now.timestamp() - interval:
                due.append((entry, occurred))
        due.sort(key=lambda candidate: (not candidate[0]["forced"], candidate[0]["recordings_checked_at"]))
        lookup_window = getattr(self.source, "get_calls_in_window", None)
        if lookup_window is None:
            lookup_window = getattr(self.source, "get_recordings_in_window", None)
        if lookup_window is not None:
            self.refresh_waiting_windows(due, lookup_window, now)
            return
        checked_at = int(now.timestamp())
        for entry, _ in due[:self.recording_followup_limit]:
            try:
                result = self.source.get_recordings(entry["external_id"])
            except (OSError, ValueError) as error:
                event("outbound_recording_lookup_failed", level="WARNING", reason=type(error).__name__)
                continue
            self.save_waiting_lookup(entry, result, checked_at=checked_at, consume_retry=True)

    def refresh_waiting_windows(self, due, lookup_window, now):
        windows = defaultdict(list)
        now_seconds = int(now.timestamp())
        zone = getattr(self.source, "recording_window_timezone", timezone.utc)
        for entry, occurred in due:
            local_day = occurred.astimezone(zone).date()
            start = datetime.combine(local_day, time.min, tzinfo=zone)
            windows[start].append(entry)
        for start, entries in list(windows.items())[:self.recording_followup_limit]:
            end = min(start + timedelta(days=1), now)
            try:
                results = lookup_window([entry["external_id"] for entry in entries], start, end)
            except (OSError, ValueError) as error:
                event("outbound_recording_lookup_failed", level="WARNING", reason=type(error).__name__)
                results = {}
                lookup_succeeded = False
            else:
                lookup_succeeded = True
            for entry in entries:
                self.save_waiting_lookup(entry, results.get(entry["external_id"]), checked_at=now_seconds,
                                         consume_retry=lookup_succeeded)

    def save_waiting_lookup(self, entry, result, *, checked_at=None, consume_retry=False):
        with self.store.transaction() as db:
            current = db.execute("SELECT * FROM outbound_calls WHERE call_id=?",
                                 (entry["call_id"],)).fetchone()
            if current is None or current["complete"] or current["external_id"] != entry["external_id"]:
                return
            if isinstance(result, CallRecord):
                self.save_call_snapshot(db, result, current)
            elif result is not None:
                recordings, complete = result
                self.save_recordings(db, current, recordings)
                if complete:
                    db.execute("UPDATE outbound_calls SET complete=1 WHERE call_id=?", (entry["call_id"],))
            if checked_at is not None:
                db.execute("""UPDATE outbound_calls SET recordings_checked_at=max(recordings_checked_at, ?)
                              WHERE call_id=?""", (checked_at, entry["call_id"]))
                if consume_retry and entry["forced"]:
                    db.execute("UPDATE outbound_console_retries SET recordings_checked=1 WHERE call_id=?",
                               (entry["call_id"],))

    def retire_short_calls(self):
        with self.store.transaction() as db:
            if self.skip_call_duration_ms is not None:
                db.execute("""INSERT OR IGNORE INTO outbound_short_calls
                    SELECT c.call_id FROM outbound_calls c
                    WHERE c.owner=? AND c.source=? AND json_extract(c.metadata,'$.call_duration_ms')<=?
                    AND NOT EXISTS (SELECT 1 FROM outbound_import_exclusions x WHERE x.owner=c.owner
                        AND x.source=c.source AND x.kind='call' AND x.identifier=c.external_id)""",
                    (self.owner, self.source_id, self.skip_call_duration_ms))
            calls = db.execute("""SELECT s.call_id FROM outbound_short_calls s JOIN outbound_calls c USING (call_id)
                WHERE c.owner=? AND c.source=?""", (self.owner, self.source_id)).fetchall()
            for call in calls:
                try:
                    self.store.retire_call(db, call["call_id"])
                except RemovalUnavailable:
                    continue

    def recover_downloads(self, db):
        candidates = db.execute("""
            SELECT r.call_id,r.external_id,saved.id,saved.request_id
            FROM outbound_recordings r JOIN outbound_calls c USING (call_id)
            JOIN recordings saved ON saved.owner=c.owner AND saved.call_id=c.call_id
            WHERE c.owner=? AND c.source=? AND r.recording_id IS NULL
              AND saved.request_id GLOB 'outbound:*'
              AND NOT EXISTS (SELECT 1 FROM outbound_recordings linked
                              WHERE linked.call_id=r.call_id AND linked.recording_id=saved.id)
        """, (self.owner, self.source_id)).fetchall()
        recovered = 0
        for row in candidates:
            if row["request_id"] == outbound_request_id(row["call_id"], row["external_id"]):
                recovered += db.execute("""
                    UPDATE outbound_recordings SET recording_id=?,error=NULL
                    WHERE call_id=? AND external_id=? AND recording_id IS NULL
                """, (row["id"], row["call_id"], row["external_id"])).rowcount
        return recovered

    def download(self, *, only_calls=None, attempted=None, downloaded=None):
        self.retire_short_calls()
        with self.store.transaction() as db:
            imported = self.recover_downloads(db)
        included = None if only_calls is None else json.dumps(list(only_calls))
        with closing(self.store.connect()) as db:
            pending = db.execute(
                """SELECT r.*, c.metadata AS call_metadata,
                          r.call_id IN (SELECT call_id FROM outbound_console_retries) AS manual_retry
                   FROM outbound_recordings r JOIN outbound_calls c USING (call_id)
                   WHERE c.owner=? AND c.source=? AND r.recording_id IS NULL
                   AND r.error IS NOT 'invalid_audio' AND r.download_attempts < ?
                   AND (? IS NULL OR r.call_id IN (SELECT value FROM json_each(?)))
                   AND r.call_id NOT IN (SELECT call_key FROM console_call_removals)
                   AND r.call_id NOT IN (SELECT call_id FROM outbound_short_calls)
                   ORDER BY manual_retry DESC, r.rowid""",
                (self.owner, self.source_id, DOWNLOAD_ATTEMPT_LIMIT, included, included),
            ).fetchall()
        failed = 0
        reopen = getattr(self.source, "reopen_recording", self.source.open_recording)
        for entry in pending:
            if not self.disk_has_space():
                break
            key = (entry["call_id"], entry["external_id"])
            if attempted is not None:
                if key in attempted:
                    continue
                attempted.add(key)
            ref = RecordingRef(id=entry["external_id"], reference=entry["reference"])
            request_id = outbound_request_id(*key)
            try:
                open_recording = reopen if entry["manual_retry"] else self.source.open_recording
                with open_recording(ref) as stream:
                    audio = stream.read(self.max_bytes + 1)
                if len(audio) > self.max_bytes:
                    raise ValueError("Source recording exceeds upload limit")
                if not self.disk_has_space(len(audio)):
                    break
                try:
                    info = inspect_recording(audio)
                except HTTPException:
                    self.record_download_error(entry, "invalid_audio")
                    failed += 1
                    continue
                provider = json.loads(entry["metadata"]).get("_archive_instance")
                call = json.loads(entry["call_metadata"])
                call.pop("_archive_instance", None)
                metadata = {"flow": "remote" if provider else "local", "options": {}, "call": call}
                if provider:
                    metadata["archive_instance"] = provider
                result = self.store.submit(self.owner, request_id, metadata, audio, info.media_type,
                                           linked_call=key[0], duration_ms=info.duration_ms)
            except (OSError, ValueError, SubmissionConflict) as error:
                self.record_download_error(entry, type(error).__name__)
                failed += 1
                continue
            with self.store.transaction() as db:
                db.execute("UPDATE outbound_recordings SET recording_id=?, error=NULL WHERE call_id=? AND external_id=?",
                           (result["recording_id"], *key))
            imported += 1
            if downloaded is not None:
                downloaded.append(result["recording_id"])
        with self.store.transaction() as db:
            db.execute("""DELETE FROM outbound_console_retries WHERE call_id IN (
                SELECT call_id FROM outbound_calls WHERE owner=? AND source=? AND complete=1
                AND NOT EXISTS (SELECT 1 FROM outbound_recordings r WHERE r.call_id=outbound_calls.call_id
                                AND r.recording_id IS NULL)
            )""", (self.owner, self.source_id))
        return imported, failed

    def disk_has_space(self, payload_bytes=0):
        if not self.min_free_bytes:
            return True
        if shutil.disk_usage(self.store.database.parent).free >= self.min_free_bytes + payload_bytes:
            return True
        event("outbound_disk_reserve", level="WARNING")
        return False

    def record_download_error(self, entry, error):
        increment = int(error != "invalid_audio")
        with self.store.transaction() as db:
            changed = db.execute(
                """UPDATE outbound_recordings SET error=?, download_attempts=download_attempts+?
                   WHERE call_id=? AND external_id=? AND recording_id IS NULL
                   AND download_attempts=? AND error IS ?""",
                (error, increment, entry["call_id"], entry["external_id"],
                 entry["download_attempts"], entry["error"]),
            ).rowcount
        if changed and increment and entry["download_attempts"] + increment == DOWNLOAD_ATTEMPT_LIMIT:
            event("outbound_recording_download_exhausted", level="WARNING", reason=error)
