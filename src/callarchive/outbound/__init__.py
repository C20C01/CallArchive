"""Interfaces for outbound call providers."""

from datetime import datetime
from typing import BinaryIO, ContextManager, NamedTuple, Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class RecordingRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=256)
    reference: str = Field(min_length=1, max_length=2048)


class CallRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_call_id: str = Field(min_length=1, max_length=256)
    started_at: AwareDatetime
    caller: str | None = Field(default=None, min_length=1, max_length=64)
    callee: str | None = Field(default=None, min_length=1, max_length=64)
    connected: bool | None = Field(default=None, strict=True)
    call_duration_ms: int | None = Field(default=None, ge=0, strict=True)
    source_aliases: list[str] = Field(default_factory=list, max_length=16)
    recordings: list[RecordingRef] = Field(default_factory=list)
    recordings_complete: bool = False


class Page(NamedTuple):
    calls: list[CallRecord]
    cursor: str | None


class CursorExpired(Exception):
    """The provider requires the current time window to be read again."""


class OutboundSource(Protocol):
    def list_calls(self, start: datetime, end: datetime, cursor: str | None, limit: int) -> Page: ...

    def get_recordings(self, call_id: str) -> tuple[list[RecordingRef], bool]: ...

    def open_recording(self, recording: RecordingRef) -> ContextManager[BinaryIO]: ...
