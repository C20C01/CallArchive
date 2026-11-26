import json
import re
from contextlib import closing
from datetime import datetime, timedelta, timezone


LOCAL_TIME = timezone(timedelta(hours=8))
FORMAT_BY_TYPE = {"audio/mpeg": "mp3", "audio/wav": "wav", "audio/flac": "flac"}


def timestamp(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, LOCAL_TIME).isoformat()
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(LOCAL_TIME).isoformat()
    except (TypeError, ValueError):
        return None


def file_name(media_type, index, started_at, callee):
    extension = FORMAT_BY_TYPE.get(media_type, "audio")
    local_time = timestamp(started_at)
    if not local_time:
        return f"录音-{index}.{extension}"
    stem = datetime.fromisoformat(local_time).strftime("%Y-%m-%d_%H-%M-%S")
    if isinstance(callee, str) and re.fullmatch(r"\+?[0-9]{1,20}", callee):
        stem = f"{callee}_{stem}"
    if index > 1:
        stem = f"{stem}_{index}"
    return f"{stem}.{extension}"


def archive_recording_names(store, identifiers, *, by_job=False):
    if not identifiers:
        return {}
    key = "w.job_id" if by_job else "r.id"
    with closing(store.connect()) as db:
        rows = db.execute(f"""
            SELECT {key} AS identifier, r.media_type,
                   coalesce(o.metadata,json_extract(r.metadata,'$.call'),'{{}}') AS call,
                   1 + (SELECT count(*) FROM recordings previous
                        WHERE previous.call_id=r.call_id AND previous.rowid<r.rowid) AS segment_number
            FROM recordings r LEFT JOIN outbound_calls o ON o.call_id=r.call_id
            LEFT JOIN workflow w ON w.recording_id=r.id
            WHERE {key} IN (SELECT value FROM json_each(?))
        """, (json.dumps(list(identifiers)),)).fetchall()
    names = {}
    for row in rows:
        call = json.loads(row["call"])
        names[row["identifier"]] = file_name(
            row["media_type"], row["segment_number"], call.get("started_at"), call.get("callee")
        )
    return names
