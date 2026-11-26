"""Optional progress reporting for transcription engines."""

from contextlib import contextmanager
from contextvars import ContextVar


_reporter = ContextVar("transcription_progress_reporter", default=None)


@contextmanager
def capture_progress(reporter):
    token = _reporter.set(reporter)
    try:
        yield
    finally:
        _reporter.reset(token)


def report_progress(stage: str, completed: int | None = None, total: int | None = None):
    """Report a stage and, when available, actual completed and total work units."""
    reporter = _reporter.get()
    if reporter is None:
        return
    progress = {"stage": stage}
    if completed is not None and total is not None and 0 <= completed <= total and total > 0:
        progress.update(completed=completed, total=total)
    reporter(progress)
