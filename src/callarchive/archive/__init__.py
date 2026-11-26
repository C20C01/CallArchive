"""Interfaces and errors for archive providers."""

from typing import Protocol


class ArchiveConflict(Exception):
    """A stable attachment already exists with different content."""


class ArchiveAdapter(Protocol):
    def find_targets(self, recording_id: str) -> tuple[str, ...]: ...

    def put_attachment(self, target: str, name: str, content: bytes) -> str: ...
