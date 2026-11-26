"""Notification channel interface."""

from dataclasses import dataclass
from enum import Enum
from typing import Protocol


class Delivery(str, Enum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Message:
    id: str
    destination: str
    title: str
    content: dict


@dataclass(frozen=True)
class Receipt:
    status: Delivery
    reference: str | None = None
    reason: str | None = None


class NotificationChannel(Protocol):
    supports_idempotency: bool

    def send(self, message: Message) -> Receipt: ...
