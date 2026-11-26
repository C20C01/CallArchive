import json
from datetime import datetime, timezone
from pathlib import Path

from pydantic import TypeAdapter

from . import CallRecord, Page


class ManifestSource:
    """Import a JSON manifest and files contained in its directory."""

    def __init__(self, manifest: Path):
        self.manifest = manifest.resolve()
        self.schema = TypeAdapter(list[CallRecord])

    def read(self):
        records = self.schema.validate_json(self.manifest.read_bytes())
        by_id = {call.external_call_id: call for call in records}
        if len(by_id) != len(records):
            raise ValueError("Duplicate call ID in manifest")
        return by_id

    def list_calls(self, start, end, cursor, limit):
        def key(call):
            return call.started_at.astimezone(timezone.utc).isoformat(), call.external_call_id

        last = tuple(json.loads(cursor)) if cursor is not None else ()
        calls = sorted((call for call in self.read().values()
                        if start <= call.started_at < end and key(call) > last), key=key)
        selected = calls[:limit]
        return Page(selected, json.dumps(key(selected[-1])) if len(calls) > limit else None)

    def get_recordings(self, call_id):
        call = self.read().get(call_id)
        if call is None:
            return [], False
        return call.recordings, call.recordings_complete

    def open_recording(self, recording):
        path = (self.manifest.parent / recording.reference).resolve()
        if not path.is_relative_to(self.manifest.parent):
            raise ValueError("Recording path must stay within the manifest directory")
        return path.open("rb")
