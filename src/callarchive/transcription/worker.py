from threading import Event, Thread

from ..observability import event
from .engines import TranscriptionEngine
from .jobs import JobStore
from .progress import capture_progress, report_progress


def process_one(store: JobStore, engine: TranscriptionEngine, lease_seconds=30, *, on_finished=None):
    claim = store.claim(engine.name, lease_seconds)
    if claim is None:
        return False
    event("transcription_started", job_id=claim.job_id)
    stop = Event()

    def renew_lease():
        while not stop.wait(lease_seconds / 3):
            if not store.renew(claim, lease_seconds):
                return

    heartbeat = Thread(target=renew_lease, daemon=True)
    heartbeat.start()
    try:
        with capture_progress(lambda progress: store.update_progress(claim, progress)):
            result = engine.transcribe(claim.audio, claim.options)
            report_progress("saving")
        if not isinstance(result, dict) or not isinstance(result.get("text"), str):
            raise ValueError("Invalid transcription result")
    except Exception as error:
        completed = store.finish(claim, error="engine_failed")
        if completed:
            event("transcription_finished", job_id=claim.job_id, level="WARNING",
                  status="failed", reason=type(error).__name__)
    else:
        completed = store.finish(claim, result)
        if completed:
            event("transcription_finished", job_id=claim.job_id, status="succeeded")
    finally:
        stop.set()
        heartbeat.join()
    if completed and claim.archive_group is not None and on_finished is not None:
        try:
            on_finished(claim.job_id)
        except Exception as error:
            event("transcription_completion_delivery_failed", job_id=claim.job_id,
                  level="WARNING", reason=type(error).__name__)
    return True
