
import asyncio
import tempfile
import time
import unittest
from pathlib import Path

import httpx

from callarchive.backend.api import create_app
from callarchive.backend.store import ArchiveStore
from test_service import sample_audio


class OperationsTest(unittest.TestCase):
    def test_operations_is_owner_scoped_unless_explicitly_admin(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise(Path(directory) / "archive.sqlite"))

    async def exercise(self, database):
        credentials = {
            "operator": ("operator", False), "administrator": ("admin", False),
            "service": ("service", True),
        }
        app = create_app(database, credentials, admins={"admin"})
        store = ArchiveStore(database)
        metadata = {"flow": "local", "call": None, "options": {}}
        first = store.submit("operator", "request", metadata, sample_audio(), "audio/wav")
        store.submit("other", "request", metadata, sample_audio(), "audio/wav")
        claim = store.claim(now=100)
        store.advance(claim, "needs_review", error="attachment_write_failed")
        recording_id = first["recording_id"]

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            async def get(path, identity):
                return await client.get(path, headers={"Authorization": f"Bearer {identity}"})

            root = "/v1/archive/recordings"
            own = (await get(root, "operator")).json()
            self.assertEqual([row["recording_id"] for row in own["items"]], [recording_id])
            self.assertEqual((await get(root + "?all_owners=true", "operator")).status_code, 403)
            self.assertEqual((await get(root + "?all_owners=true", "service")).status_code, 403)

            all_rows = (await get(root + "?all_owners=true&limit=1", "administrator")).json()
            self.assertEqual(len(all_rows["items"]), 1)
            self.assertIsNotNone(all_rows["next_cursor"])
            next_url = root + "?all_owners=true&cursor=" + str(all_rows["next_cursor"])
            second_page = (await get(next_url, "administrator")).json()
            self.assertEqual([row["recording_id"] for row in second_page["items"]], [recording_id])

            path = f"{root}/{recording_id}/events"
            self.assertEqual((await get(path, "administrator")).status_code, 404)
            events = (await get(path + "?all_owners=true", "administrator")).json()["items"]
            self.assertEqual([event["status"] for event in events], ["queued", "needs_review"])
            self.assertEqual(events[-1]["error"], "attachment_write_failed")


    def test_retry_does_not_duplicate_work_and_preserves_prior_transcript(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            metadata = {"flow": "local", "call": None, "options": {}}
            initial = store.submit("operator", "request", metadata, sample_audio(), "audio/wav")
            recording_id = initial["recording_id"]
            store.advance(store.claim(now=100), "transcribing", job_id="job1")
            store.advance(store.claim(now=100), "acknowledging", transcript='{"text":"ok"}')
            store.advance(store.claim(now=100), "delivering")
            store.advance(store.claim(now=100), "needs_review", error="attachment_write_failed")
            self.assertIsNone(store.request_retry("other", recording_id, actor="other"))
            retry = store.request_retry("operator", recording_id, actor="operator")
            self.assertEqual(retry["status"], "delivering")
            self.assertEqual(retry, store.request_retry("operator", recording_id, actor="operator"))
            resumed = store.claim(now=100)
            self.assertEqual(resumed["transcript"], '{"text":"ok"}')
            self.assertIsNone(store.claim(now=100))
            self.assertEqual(store.events("operator", recording_id, after=0, limit=50)["items"][-1]["actor"],
                             "operator")


    def test_manual_retry_brings_forward_the_same_automatic_transcription_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            item = store.submit("operator", "automatic-retry", {"flow": "local", "call": None, "options": {}},
                                sample_audio(), "audio/wav")
            recording_id = item["recording_id"]
            now = time.time()
            store.advance(store.claim(now=now), "queued", retry_at=now+3600,
                          error="transcription_failed", restart=True)
            self.assertTrue(store.get("operator", recording_id)["can_retry"])
            self.assertIsNone(store.claim(now=now))
            requested = store.request_retry("operator", recording_id, actor="operator")
            self.assertEqual(store.request_retry("operator", recording_id, actor="operator"), requested)
            self.assertEqual(store.claim(now=now)["retry_generation"], 1)

    def test_definitive_transcription_failure_gets_new_request_id(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            item = store.submit("operator", "request", {"flow": "local", "call": None, "options": {}},
                                sample_audio(), "audio/wav")
            claim = store.claim(now=100)
            self.assertEqual(claim["retry_generation"], 0)
            store.advance(claim, "transcribing", job_id="job1")
            store.advance(store.claim(now=100), "needs_review", error="transcription_failed")
            store.request_retry("operator", item["recording_id"], actor="operator")
            next_claim = store.claim(now=100)
            self.assertEqual(next_claim["retry_generation"], 1)
            self.assertIsNone(next_claim["job_id"])
