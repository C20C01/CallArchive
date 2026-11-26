"""Contract for an external archive provider."""

from dataclasses import dataclass
from enum import Enum
from typing import Protocol


class MatchStatus(str, Enum):
    MATCHED = "matched"
    NEEDS_INPUT = "needs_input"
    CONFLICT = "conflict"


class WriteStatus(str, Enum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    RETRYABLE = "retryable"
    UNKNOWN = "unknown"


class VerifyStatus(str, Enum):
    CONFIRMED = "confirmed"
    CONFIRMED_ABSENT = "confirmed_absent"
    INCONCLUSIVE = "inconclusive"


@dataclass(frozen=True)
class Lookup:
    status: MatchStatus
    targets: tuple[str, ...] = ()


@dataclass(frozen=True)
class Attachment:
    kind: str
    name: str
    media_type: str
    content: bytes


@dataclass(frozen=True)
class WriteResult:
    status: WriteStatus
    receipt: str | None = None


@dataclass(frozen=True)
class VerifyResult:
    status: VerifyStatus
    receipt: str | None = None


class LookupUnavailable(Exception):
    def __init__(self, *, retryable: bool):
        self.retryable = retryable


class ExternalArchive(Protocol):
    id: str
    supports_idempotency: bool
    supports_verification: bool

    async def find_targets(self, fields: dict) -> Lookup: ...

    async def put_attachment(self, target: str, attachment: Attachment, request_id: str) -> WriteResult: ...

    async def verify_attachment(self, target: str, attachment: Attachment, request_id: str) -> VerifyResult: ...
