"""Start a disposable local CallArchive workspace with synthetic calls and audio."""

import argparse
import io
import json
import math
import secrets
import struct
import wave
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn

from callarchive.backend.api import create_app
from callarchive.backend.store import ArchiveStore
from callarchive.observability import RuntimeLog


PEOPLE = {"10001": "示例小林", "10002": "示例小周", "10003": "示例小林"}
TRANSCRIPT = {"text": "这是用于演示的转写文本。音频为程序生成的提示音，没有真实通话内容。"}


def tone(seconds):
    rate = 8_000
    frame = b"".join(
        struct.pack("<h", int(500 * math.sin(2 * math.pi * 440 * index / rate)))
        for index in range(rate)
    )
    output = io.BytesIO()
    with wave.open(output, "wb") as audio:
        audio.setparams((1, 2, rate, 0, "NONE", "not compressed"))
        audio.writeframes(frame * seconds)
    return output.getvalue()


def seed(store, now):
    entries = [
        ("10001", "20001", "needs_input", (70,)),
        ("10002", "20002", "queued", (66, 82)),
        ("10003", "20003", "needs_review", (47,)),
        ("10002", "20004", "needs_input", (153,)),
        ("10001", "20005", "waiting", ()),
        ("10003", "20006", "archived", (102,)),
        ("10001", "20007", "archived", (74, 56)),
    ]
    for index, (caller, callee, state, lengths) in enumerate(entries, 1):
        call_id = f"demo-call-{index}"
        fields = {
            "external_call_id": call_id,
            "caller": caller,
            "callee": callee,
            "started_at": (now - timedelta(minutes=index * 12)).isoformat(),
        }
        if state == "waiting":
            with store.transaction() as db:
                db.execute("INSERT INTO calls (id,owner) VALUES (?,'demo')", (call_id,))
                db.execute(
                    """INSERT INTO outbound_calls (owner,source,external_id,call_id,metadata,complete)
                       VALUES ('demo','synthetic',?,?,?,0)""",
                    (call_id, call_id, json.dumps(fields)),
                )
            continue
        for number, seconds in enumerate(lengths, 1):
            result = store.submit(
                "demo", f"{call_id}-{number}", {"call": fields, "flow": "local", "options": {}},
                tone(seconds), "audio/wav", duration_ms=seconds * 1_000,
            )
            recording = result["recording_id"]
            with store.transaction() as db:
                db.execute("UPDATE recordings SET status=? WHERE id=?", (state, recording))
                if state in {"needs_input", "needs_review"}:
                    reason = "archive_target_missing" if state == "needs_input" else "archive_attachment_unknown"
                    db.execute("INSERT INTO workflow (recording_id,error) VALUES (?,?)", (recording, reason))
                    db.execute("INSERT INTO recording_recovery (recording_id,resume_status) VALUES (?,'delivering')",
                               (recording,))
                elif state == "archived":
                    db.execute("INSERT INTO workflow (recording_id,transcript) VALUES (?,?)",
                               (recording, json.dumps(TRANSCRIPT, ensure_ascii=False)))
                if state != "queued":
                    store.record_event(db, recording, state, actor="demo", now=now.timestamp())


def create_demo(directory, credential, *, now=None):
    store = ArchiveStore(directory / "archive.sqlite")
    seed(store, now or datetime.now(timezone.utc))
    return create_app(
        store.database,
        {credential: ("demo", False)},
        admins={"demo"},
        followup_names=PEOPLE,
        runtime_log=RuntimeLog(directory / "operations.sqlite"),
        attachments=directory / "attachments",
    )


def main():
    parser = argparse.ArgumentParser(description="在本机启动带合成话单的 CallArchive 演示。")
    parser.add_argument("--port", type=int, default=8878, help="本机端口，默认 8878。")
    args = parser.parse_args()
    credential = secrets.token_urlsafe(24)
    with TemporaryDirectory(prefix="callarchive-demo-") as directory:
        app = create_demo(Path(directory), credential)
        print(f"演示页面：http://127.0.0.1:{args.port}/", flush=True)
        print(f"本次演示访问凭据：{credential}", flush=True)
        uvicorn.run(app, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
