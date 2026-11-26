import json
import time
from contextlib import closing

from .call_state import CALL_STATE
from .followup_names import caller_key, normalize_followup_names
from .recording_names import file_name, timestamp
from .runtime_log_view import REASONS, STATUSES


PENDING_LABELS = {
    "waiting_recording": "等待录音", "waiting_download": "等待下载", "recording_invalid": "录音异常",
    "waiting_applicant": "待补录档案", "processing": "处理中", "attention": "处理异常",
}
class ConsoleCalls:
    def __init__(self, store, names):
        self.store = store
        self.names = normalize_followup_names(names)

    def person(self, number):
        return self.names.get(caller_key(number), number) if number else "—"

    def pending(self, *, day=None, mobile=None, owner=None, salesperson=None, status=None,
                offset=0, limit=30, call_key=None):
        with closing(self.store.connect()) as db:
            db.create_function("followup_name", 1, self.person)
            rows = db.execute(CALL_STATE + """
                SELECT * FROM summaries
                WHERE (:id IS NULL OR call_key=:id) AND (:mobile IS NULL OR json_extract(call,'$.callee')=:mobile)
                  AND (:day IS NULL OR date(json_extract(call,'$.started_at'),'+8 hours')=:day)
                  AND (:status IS NULL OR status=:status OR (:status='attention' AND status='recording_invalid'))
                  AND (:owner IS NULL OR json_extract(call,'$.caller')=:owner
                       OR followup_name(json_extract(call,'$.caller'))=:owner)
                  AND (:salesperson IS NULL OR instr(followup_name(json_extract(call,'$.caller')),:salesperson)>0
                       OR instr(json_extract(call,'$.caller'),:salesperson)>0)
                ORDER BY julianday(json_extract(call,'$.started_at')) DESC,call_key DESC LIMIT :limit OFFSET :offset""",
                dict(now=time.time(), id=call_key, day=day, mobile=mobile,
                     owner=self.names.get(caller_key(owner), owner), salesperson=salesperson,
                     status=status, limit=limit + 1, offset=offset)).fetchall()
            calls = [self.pending_item(row) for row in rows[:limit]]
        return {"calls": calls, "statuses": PENDING_LABELS,
                "next_offset": offset + limit if len(rows) > limit else None}

    def counts(self):
        with closing(self.store.connect()) as db:
            counts = db.execute(CALL_STATE + "SELECT status,count(*) AS total FROM summaries GROUP BY status",
                                {"now": time.time()}).fetchall()
        return {row["status"]: row["total"] for row in counts}

    def pending_item(self, row):
        call = json.loads(row["call"])
        caller = call.get("caller")
        return {
            "talk_id": row["call_key"], "caller": caller, "salesperson": self.person(caller),
            "has_salesperson_name": caller_key(caller) in self.names, "mobile": call.get("callee"),
            "call_time": timestamp(call.get("started_at")), "closed_at": None,
            "status": row["status"], "status_label": PENDING_LABELS[row["status"]],
            "source": "外呼导入" if row["outbound"] else "已登记话单", "recording_count": row["loaded"] + row["pending"],
            "downloaded_count": row["loaded"], "archived_count": row["archived_count"],
            "recording_seconds": row["duration"] / 1000 if row["duration"] is not None else None,
            "can_retry": bool(row["retryable"] or row["pending"] or row["waiting_recording"]),
            "can_remove": not row["busy"],
        }

    @staticmethod
    def segments(db, call_key, *, pending_only=False):
        return db.execute("""
            SELECT r.id, r.status, r.media_type, length(r.audio)>0 AS has_audio,
                   w.transcript IS NOT NULL AS has_text, w.error, w.retry_at, w.lease_until,
                   recovery.resume_status, m.duration_ms,
                   1 + (SELECT count(*) FROM recordings previous
                        WHERE previous.call_id=r.call_id AND previous.rowid<r.rowid) AS segment_number
            FROM recordings r LEFT JOIN workflow w ON w.recording_id=r.id
            LEFT JOIN recording_recovery recovery ON recovery.recording_id=r.id
            LEFT JOIN recording_media m ON m.recording_id=r.id
            WHERE coalesce(r.call_id,r.id)=? AND r.status<>'removed'
              AND (NOT ? OR r.status<>'archived') ORDER BY r.rowid
        """, (call_key, pending_only)).fetchall()

    def pending_detail(self, call_key):
        results = self.pending(call_key=call_key, limit=1)["calls"]
        if not results:
            return None
        call = results[0]
        with closing(self.store.connect()) as db:
            rows = self.segments(db, call_key)
            staged = db.execute("""SELECT external_id,error,download_attempts FROM outbound_recordings
                WHERE call_id=? AND recording_id IS NULL ORDER BY rowid""", (call_key,)).fetchall()
            attachments = db.execute("""SELECT a.recording_id,a.target,a.kind,a.status
                FROM remote_attachments a JOIN recordings r ON r.id=a.recording_id
                WHERE coalesce(r.call_id,r.id)=? ORDER BY a.rowid
            """, (call_key,)).fetchall()
        states = {"pending": "待归档", "writing": "归档中", "confirmed": "已归档",
                  "unknown": "待核对", "rejected": "归档失败"}
        targets = {}
        for attachment in attachments:
            recording_targets = targets.setdefault(attachment["recording_id"], {})
            facts = recording_targets.setdefault(attachment["target"], {
                "apply_id": attachment["target"], "audio": "—", "text": "—",
            })
            field = "audio" if attachment["kind"] == "audio" else "text"
            facts[field] = states[attachment["status"]]
        names = [file_name(row["media_type"], row["segment_number"], call["call_time"], call["mobile"]) for row in rows]
        call["recordings"] = [{
            "file_id": row["id"], "name": name,
            "download_name": name if row["has_audio"] and row["status"] != "archived" else None,
            "download": "已下载", "matching": "待补录应聘档案" if row["status"] == "needs_input" else "—",
            "transcription": "已转写" if row["has_text"] else ("转写中" if row["status"] == "transcribing" else "待转写"),
            "worker_label": STATUSES.get(row["status"], row["status"]),
            "audio": "已归档" if row["status"] == "archived" else "—",
            "text": "已归档" if row["status"] == "archived" else "—",
            "failure": REASONS.get(row["error"], row["error"]),
            "targets": list(targets.get(row["id"], {}).values()),
            "retry_after": timestamp(row["retry_at"]) if row["retry_at"] and row["retry_at"] > time.time() else None,
        } for row, name in zip(rows, names)]
        call["recordings"].extend({
            "file_id": row["external_id"], "name": "待获取录音", "download": "获取失败" if row["error"] else "待下载",
            "failure": "录音无法解码" if row["error"] == "invalid_audio" else None,
            "download_attempts": row["download_attempts"], "targets": [],
        } for row in staged)
        return call

    def retry(self, call_key, actor):
        return self.store.request_call_retry(call_key, actor=actor)

    def remove(self, call_key, actor):
        now = time.time()
        with self.store.transaction() as db:
            if db.execute("SELECT 1 FROM console_call_removals WHERE call_key=?", (call_key,)).fetchone():
                return True
            row = db.execute(CALL_STATE + "SELECT 1 FROM summaries WHERE call_key=:id",
                             {"now": now, "id": call_key}).fetchone()
            if row is None:
                return False
            db.execute("""INSERT INTO console_call_removals (call_key,actor,occurred_at,owner,external_id)
                VALUES (?,?,?,(SELECT owner FROM calls WHERE id=?),(SELECT external_id FROM calls WHERE id=?))
            """, (call_key, actor, now, call_key, call_key))
            self.store.retire_call(db, call_key, now=now)
        return True
