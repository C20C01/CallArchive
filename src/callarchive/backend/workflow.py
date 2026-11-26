import json
import time

import httpx

from ..archive import ArchiveAdapter, ArchiveConflict
from .call_state import TRANSCRIPTION_ATTEMPT_LIMIT
from .recording_names import archive_recording_names, file_name
from .remote_archive import RemoteDelivery
from .store import ArchiveStore


JOBS = "/v1/transcription/jobs"
TRANSCRIPTION_POLL_SECONDS = 3
TRANSCRIPTION_STATUS_BATCH_LIMIT = 35
TRANSCRIPTION_STATES = frozenset({"queued", "running", "succeeded", "failed", "needs_review"})
EXTENSIONS = {"audio/wav": "wav", "audio/mpeg": "mp3", "audio/flac": "flac"}


class Workflow:
    def __init__(self, store: ArchiveStore, transcription: httpx.AsyncClient, archive: ArchiveAdapter, *, providers=None):
        self.store = store
        self.transcription = transcription
        self.archive = archive
        self.batch_status_supported = True
        self.status_cache = {}
        self.status_cache_expires = 0
        self.remote = {key: RemoteDelivery(store, provider) for key, provider in (providers or {}).items()}

    async def step(self, *, now=None):
        now = time.time() if now is None else now
        for delivery in self.remote.values():
            if await delivery.recheck_waiting(now):
                return True
        item = self.store.claim(now=now)
        if item is None:
            return False
        await self.process(item, now)
        return True

    async def run_due(self, new_recordings=(), *, now=None, limit=100, already_processed=()):
        """Process the new recordings, then one bounded pass over the existing work."""
        clock = time.time if now is None else lambda: now
        new_recordings = dict.fromkeys(new_recordings)
        steps = 0
        for recording_id in new_recordings:
            steps += await self.resume(recording_id, now=now)
        for delivery in self.remote.values():
            for _ in range(limit):
                if not await delivery.recheck_waiting(clock()):
                    break
                steps += 1
        remaining = self.store.due_recordings(now=clock(), limit=limit,
                                             exclude=set(already_processed).union(new_recordings))
        for recording_id in remaining:
            steps += await self.resume(recording_id, now=now)
        return steps

    async def resume_call(self, call_key):
        now = time.time()
        for delivery in self.remote.values():
            await delivery.recheck_waiting(now, call_key)
        for recording_id in self.store.call_recordings(call_key):
            await self.resume(recording_id)

    async def resume_job(self, job_id, *, now=None):
        recording_id = self.store.recording_for_job(job_id)
        if recording_id is None:
            return 0
        self.status_cache.pop(job_id, None)
        return await self.resume(recording_id, now=now, completed_job=job_id)

    async def resume(self, recording_id, *, now=None, completed_job=None):
        """Advance one recording through its immediately available stages."""
        clock = time.time if now is None else lambda: now
        cutoff = clock()
        for steps in range(64):
            acquired = clock()
            item = self.store.claim(now=cutoff, lease_at=acquired, recording_id=recording_id,
                                    completed_job=completed_job if steps == 0 else None)
            if item is None:
                return steps
            await self.process(item, acquired)
        return 64

    async def process(self, item, now):
        try:
            await self.advance(item, now)
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            if status in (408, 429) or status >= 500:
                self.defer_transcription(item, now)
            else:
                self.store.advance(item, "needs_review", error=f"transcription_http_{status}")
        except httpx.RequestError:
            self.defer_transcription(item, now)
        except (ValueError, KeyError, TypeError):
            self.store.advance(item, "needs_review", error="invalid_workflow_response")
        except OSError:
            self.store.advance(item, "needs_review", error="attachment_write_failed")
        except ArchiveConflict:
            self.store.advance(item, "needs_review", error="attachment_conflict")

    def defer_transcription(self, item, now, *, missing=False, failed=False):
        if failed and item["transcription_failures"] + 1 >= TRANSCRIPTION_ATTEMPT_LIMIT:
            self.store.advance(item, "needs_review", error="transcription_failed")
            return
        restart = failed or missing
        self.store.advance(item, "queued" if restart else item["status"],
                           retry_at=now + 1,
                           error="transcription_failed" if failed else "transcription_unavailable",
                           restart=restart)

    async def transcription_status(self, job_id, now):
        if self.status_cache_expires <= time.monotonic():
            self.status_cache.clear()
        cached = self.status_cache.pop(job_id, None)
        if cached is not None:
            return cached

        if self.batch_status_supported and isinstance(job_id, str):
            others = self.store.due_transcription_jobs(
                now=now, excluding=job_id, limit=TRANSCRIPTION_STATUS_BATCH_LIMIT - 1,
            )
            if others:
                job_ids = [job_id, *others]
                try:
                    batch = await self.transcription.get(JOBS, params={"job_id": job_ids})
                    if batch.status_code in (404, 405, 501):
                        self.batch_status_supported = False
                    elif batch.is_success:
                        jobs = batch.json().get("jobs", [])
                        self.status_cache = {
                            job["job_id"]: job["status"]
                            for job in jobs if job["job_id"] in job_ids and job["status"] in TRANSCRIPTION_STATES
                        }
                        self.status_cache_expires = time.monotonic() + TRANSCRIPTION_POLL_SECONDS
                except (httpx.RequestError, AttributeError, KeyError, TypeError, ValueError):
                    pass
                cached = self.status_cache.pop(job_id, None)
                if cached is not None:
                    return cached

        response = await self.transcription.get(f"{JOBS}/{job_id}")
        if response.status_code == 404:
            return None
        status = response.raise_for_status().json()["status"]
        if status not in TRANSCRIPTION_STATES:
            raise ValueError("Invalid transcription status")
        return status

    async def advance(self, item, now):
        stage = item["status"]
        if stage == "queued":
            metadata = json.loads(item["metadata"])
            if metadata.get("flow") == "remote":
                remote = self.remote.get(metadata.get("archive_instance"))
                if remote is None:
                    self.store.advance(item, "needs_review", error="archive_instance_unavailable")
                    return
                if not await remote.prepare(item, now=now):
                    return
            request_id = item["id"]
            if item["retry_generation"]:
                request_id += f'-r{item["retry_generation"]}'
            names = archive_recording_names(self.store, [item["id"]])
            filename = names.get(item["id"]) or file_name(item["media_type"], 1, None, None)
            response = await self.transcription.post(JOBS, files={
                "audio": (filename, item["audio"], item["media_type"]),
                "metadata": (None, json.dumps({"request_id": request_id, "archive_group": item["id"],
                                              "options": metadata["options"]})),
            })
            job_id = response.raise_for_status().json()["job_id"]
            if not isinstance(job_id, str) or not job_id.isalnum():
                raise ValueError("Invalid transcription task ID")
            self.store.advance(item, "transcribing", job_id=job_id)
            return

        url = f'{JOBS}/{item["job_id"]}'
        if stage == "transcribing":
            status = await self.transcription_status(item["job_id"], now)
            if status is None:
                self.defer_transcription(item, now, missing=True)
            elif status in ("queued", "running"):
                self.store.advance(item, stage, retry_at=now + TRANSCRIPTION_POLL_SECONDS)
            elif status == "succeeded":
                response = await self.transcription.get(f"{url}/result")
                if response.status_code in (404, 410):
                    self.defer_transcription(item, now, missing=True)
                    return
                transcript = response.raise_for_status().json()
                if not isinstance(transcript["text"], str):
                    raise ValueError("Invalid transcription text")
                self.store.advance(item, "acknowledging", transcript=json.dumps(transcript, ensure_ascii=False))
            elif status == "failed":
                self.defer_transcription(item, now, failed=True)
            elif status == "needs_review":
                self.store.advance(item, "needs_review", error="transcription_needs_review")
            else:
                raise ValueError("Unknown transcription task state")
        elif stage == "acknowledging":
            response = await self.transcription.post(f"{url}/ack")
            # A previous deployment may have expired the upstream job. The transcript is already durable here.
            if response.status_code not in (404, 410):
                response.raise_for_status()
            self.store.advance(item, "delivering")
        else:
            metadata = json.loads(item["metadata"])
            if metadata.get("flow") == "remote":
                remote = self.remote.get(metadata.get("archive_instance"))
                if remote is None:
                    self.store.advance(item, "needs_review", error="archive_instance_unavailable")
                else:
                    await remote.step(item, now=now)
                return
            receipts = self.deliver(item)
            self.store.advance(item, "archived", receipts=json.dumps(receipts))

    def deliver(self, item):
        (target,) = self.archive.find_targets(item["id"])
        extension = EXTENSIONS[item["media_type"]]
        text = json.loads(item["transcript"])["text"].encode("utf-8")
        return {
            "audio": self.archive.put_attachment(target, f"recording.{extension}", item["audio"]),
            "transcript": self.archive.put_attachment(target, "transcript.txt", text),
        }
