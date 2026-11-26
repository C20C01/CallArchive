import io
from contextlib import contextmanager

import httpx
from pydantic import BaseModel, Field, ValidationError

from . import CallRecord, CursorExpired, Page, RecordingRef

ROOT = "/v1/outbound-provider"


class CallsResponse(BaseModel):
    calls: list[CallRecord] = Field(max_length=1000)
    next_cursor: str | None = Field(default=None, min_length=1, max_length=2048)


class RecordingsResponse(BaseModel):
    recordings: list[RecordingRef] = Field(max_length=1000)
    complete: bool = Field(strict=True)


class OutboundUnavailable(OSError):
    pass


class HTTPSource:
    def __init__(self, client: httpx.Client, *, max_bytes=25 * 1024**2):
        self.client = client
        self.max_bytes = max_bytes

    def query(self, endpoint, payload, model):
        try:
            response = self.client.post(ROOT + endpoint, json=payload, follow_redirects=False)
            if endpoint == "/calls:find" and payload["cursor"] is not None and response.status_code in {409, 410}:
                try:
                    details = response.json()
                except ValueError:
                    details = None
                if isinstance(details, dict) and details.get("code") == "cursor_expired":
                    raise CursorExpired
            response.raise_for_status()
            return model.model_validate_json(response.content)
        except (httpx.HTTPError, ValidationError):
            raise OutboundUnavailable("Outbound lookup failed.") from None

    def list_calls(self, start, end, cursor, limit):
        page = self.query("/calls:find", {"start": start.isoformat(), "end": end.isoformat(),
                                          "limit": limit, "cursor": cursor}, CallsResponse)
        return Page(page.calls, page.next_cursor)

    def get_recordings(self, call_id):
        result = self.query("/recordings:find", {"call_id": call_id}, RecordingsResponse)
        return result.recordings, result.complete

    @contextmanager
    def open_recording(self, recording):
        try:
            with self.client.stream("POST", ROOT + "/recordings:open",
                                    json=recording.model_dump(), follow_redirects=False) as response:
                response.raise_for_status()
                with io.BytesIO() as buffer:
                    for chunk in response.iter_bytes(chunk_size=65536):
                        if buffer.tell() + len(chunk) > self.max_bytes:
                            raise ValueError("Source recording exceeds upload limit")
                        buffer.write(chunk)
                    buffer.seek(0)
                    yield buffer
        except httpx.HTTPError:
            raise OutboundUnavailable("Outbound recording could not be retrieved.") from None
