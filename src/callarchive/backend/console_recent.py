import json
import time
from contextlib import closing

from .recording_names import file_name, timestamp
from .store import ARCHIVE_CACHE_ROWS, ARCHIVE_CACHE_SECONDS


RECENT = f"""
WITH cache AS (
    {ARCHIVE_CACHE_ROWS}
), recent AS (
    SELECT cache.call_key, r.id, r.metadata, cache.archived_at
    FROM cache JOIN recordings r ON r.id=cache.recording_id
    WHERE cache.visible AND cache.archived_at>:cutoff
), aggregated AS (
    SELECT call_key, min(metadata) AS metadata, max(archived_at) AS archived_at, count(*) AS recording_count
    FROM recent GROUP BY call_key
)
"""


class RecentArchives:
    def __init__(self, store):
        self.store = store

    def list(self, *, day, mobile, offset, limit=30, now=None):
        cutoff = (time.time() if now is None else now) - ARCHIVE_CACHE_SECONDS
        with closing(self.store.connect()) as db:
            rows = db.execute(RECENT + """
                SELECT a.*, coalesce(o.metadata,json_extract(a.metadata,'$.call'),'{}') AS call
                FROM aggregated a LEFT JOIN outbound_calls o ON o.call_id=a.call_key
                WHERE (:mobile IS NULL OR json_extract(call,'$.callee')=:mobile)
                  AND (:day IS NULL OR date(json_extract(call,'$.started_at'),'+8 hours')=:day)
                ORDER BY archived_at DESC, call_key DESC LIMIT :limit OFFSET :offset
            """, dict(cutoff=cutoff, day=day, mobile=mobile, offset=offset, limit=limit + 1)).fetchall()
        calls = []
        for row in rows[:limit]:
            call = json.loads(row["call"])
            calls.append({"talk_id": row["call_key"], "mobile": call.get("callee"),
                          "call_time": timestamp(call.get("started_at")), "archived_at": timestamp(row["archived_at"]),
                          "recording_count": row["recording_count"]})
        return {"calls": calls, "next_offset": offset + limit if len(rows) > limit else None}

    def files(self, call_key, *, now=None):
        cutoff = (time.time() if now is None else now) - ARCHIVE_CACHE_SECONDS
        with closing(self.store.connect()) as db:
            rows = db.execute(RECENT + """
                SELECT r.id, r.media_type, length(r.audio)>0 AS has_audio, w.transcript IS NOT NULL AS has_text,
                       m.duration_ms, recent.archived_at,
                       1 + (SELECT count(*) FROM recordings previous
                            WHERE previous.call_id=r.call_id AND previous.rowid<r.rowid) AS segment_number,
                       coalesce(o.metadata,json_extract(r.metadata,'$.call'),'{}') AS call
                FROM recent JOIN recordings r ON r.id=recent.id
                LEFT JOIN outbound_calls o ON o.call_id=r.call_id
                LEFT JOIN workflow w ON w.recording_id=r.id
                LEFT JOIN recording_media m ON m.recording_id=r.id
                WHERE recent.call_key=:call_key ORDER BY recent.archived_at,r.rowid
            """, {"cutoff": cutoff, "call_key": call_key}).fetchall()
        files = []
        for index, row in enumerate(rows, 1):
            call = json.loads(row["call"])
            audio = file_name(row["media_type"], row["segment_number"], call.get("started_at"), call.get("callee"))
            files.append({
                "file_id": row["id"], "audio": audio if row["has_audio"] else None,
                "text": f"转写-{index}.txt" if row["has_text"] else None,
                "recording_seconds": row["duration_ms"] / 1000 if row["duration_ms"] is not None else None,
                "expires_at": timestamp(row["archived_at"] + ARCHIVE_CACHE_SECONDS),
            })
        return files

    def attachment(self, call_key, file_id, kind):
        entry = next((file for file in self.files(call_key) if file["file_id"] == file_id and file[kind]), None)
        if entry is None:
            return None
        if kind == "audio":
            audio = self.store.get_audio(None, file_id)
            return (audio["audio"], audio["media_type"], entry[kind]) if audio and audio["audio"] else None
        row = self.store.get(None, file_id)
        text = row["transcript"].get("text") if row and isinstance(row["transcript"], dict) else None
        return (text.encode(), "text/plain; charset=utf-8", entry[kind]) if isinstance(text, str) else None
