
import asyncio
import tempfile
import unittest
from pathlib import Path

import httpx

from callarchive.archive.local import LocalArchive
from callarchive.backend.console_calls import ConsoleCalls
from callarchive.backend.store import ArchiveStore
from callarchive.backend.workflow import Workflow
from callarchive.transcription.api import create_app
from callarchive.transcription.engines import DemoEngine
from callarchive.transcription.jobs import JobStore
from callarchive.transcription.worker import process_one
from test_service import sample_audio


class WorkflowTest(unittest.TestCase):

    def test_targeted_resume_keeps_unrelated_work_queued_and_resumes_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_targeted_resume(Path(directory)))

    async def exercise_targeted_resume(self, directory):
        store = ArchiveStore(directory / "archive.sqlite")
        other_id = store.submit("operator", "other", {"flow": "local", "call": None, "options": {}},
                                sample_audio(), "audio/wav")["recording_id"]
        target_id = store.submit("operator", "target", {"flow": "local", "call": None, "options": {}},
                                 sample_audio(), "audio/wav")["recording_id"]
        database = directory / "transcription.sqlite"
        app = create_app(database, {"archive-test": ("archive", True)})
        archive = LocalArchive(directory / "attachments")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                     headers={"Authorization": "Bearer archive-test"}) as client:
            workflow = Workflow(store, client, archive)
            await asyncio.gather(workflow.resume(target_id, now=100), workflow.resume(target_id, now=100))
            target = store.get("operator", target_id)
            self.assertEqual(target["status"], "transcribing")
            self.assertEqual(store.get("operator", other_id)["status"], "queued")
            completed = []
            jobs = JobStore(database)
            self.assertTrue(process_one(jobs, DemoEngine(), on_finished=completed.append))
            self.assertFalse(process_one(jobs, DemoEngine(), on_finished=completed.append))
            self.assertEqual(completed, [target["transcription_job_id"]])
            from callarchive.backend.api import create_app as archive_app

            restarted = Workflow(ArchiveStore(store.database), client, archive)

            async def resume_job(job_id):
                await restarted.resume_job(job_id, now=101)

            api = archive_app(store.database, {}, completion_token="internal-test", resume_transcription=resume_job)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api), base_url="http://archive") as backend:
                url = f"/internal/transcriptions/{completed[0]}/complete"
                self.assertEqual((await backend.post(url)).status_code, 401)
                self.assertEqual(store.get("operator", target_id)["status"], "transcribing")
                headers = {"Authorization": "Bearer internal-test"}
                self.assertEqual((await backend.post(url, headers=headers)).status_code, 204)
                self.assertEqual((await backend.post(url, headers=headers)).status_code, 204)
            self.assertEqual(store.get("operator", target_id)["status"], "archived")
            self.assertEqual(store.get("operator", other_id)["status"], "queued")

    def test_single_cycle_prioritizes_submitted_recordings_and_exits_with_active_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_single_cycle(Path(directory)))

    async def exercise_single_cycle(self, directory):
        store = ArchiveStore(directory / "archive.sqlite")
        first, new = [store.submit("operator", name, {"flow": "local", "call": None, "options": {}},
                                   sample_audio(), "audio/wav")["recording_id"] for name in ("first", "new")]
        submitted = []
        async def respond(request):
            if request.method == "POST":
                submitted.append(await request.aread())
                return httpx.Response(202, json={"job_id": "job" + str(len(submitted))})
            return httpx.Response(200, json={"status": "running"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url="http://asr") as client:
            workflow = Workflow(store, client, LocalArchive(directory / "attachments"))
            await workflow.run_due((new,), now=100)
        self.assertEqual(len(submitted), 2)
        self.assertIn(new.encode(), submitted[0])
        self.assertIn(first.encode(), submitted[1])
        self.assertEqual(store.get("operator", first)["status"], "transcribing")
        self.assertEqual(store.get("operator", new)["status"], "transcribing")

    def test_transcription_stops_at_four_confirmed_failures_and_manual_retry_starts_a_new_round(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_attempt_limit(Path(directory)))

    async def exercise_attempt_limit(self, directory):
        store = ArchiveStore(directory / "archive.sqlite")
        recording_id = store.submit("operator", "retry-limit", {"flow": "local", "call": None, "options": {}},
                                    sample_audio(), "audio/wav")["recording_id"]
        submissions = []
        unavailable = True

        async def respond(request):
            nonlocal unavailable
            if request.method == "POST":
                if unavailable:
                    unavailable = False
                    return httpx.Response(503)
                submissions.append(await request.aread())
                return httpx.Response(202, json={"job_id": f"job{len(submissions)}"})
            return httpx.Response(200, json={"status": "failed"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url="http://asr") as client:
            workflow = Workflow(store, client, LocalArchive(directory / "attachments"))
            await workflow.run_due(now=100)
            for attempt in range(1, 5):
                await workflow.run_due(now=100 + attempt * 300)
                self.assertEqual(len(submissions), attempt)
                expected = "needs_review" if attempt == 4 else "queued"
                self.assertEqual(store.get("operator", recording_id)["status"], expected)
            await workflow.run_due(now=1600)
            self.assertEqual(len(submissions), 4)
            store.request_retry("operator", recording_id, actor="operator")
            await workflow.run_due(now=1600)
            self.assertEqual(len(submissions), 5)
            self.assertEqual(store.get("operator", recording_id)["status"], "queued")

    def test_long_batch_uses_actual_acquisition_time_for_the_recording_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            recording = store.submit("owner", "long-batch", {"flow": "local", "call": None, "options": {}},
                                     sample_audio(), "audio/wav")["recording_id"]
            claim = store.claim(now=100, lease_at=300, recording_id=recording)
            self.assertIsNotNone(claim)
            self.assertIsNone(store.claim(now=301, recording_id=recording))
            self.assertIsNotNone(store.claim(now=420, recording_id=recording))

    def test_idle_claim_uses_no_write_transaction(self):
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            with patch.object(store, "transaction", side_effect=AssertionError("idle write")):
                self.assertIsNone(store.claim(now=100))

    def test_two_due_transcriptions_share_one_status_request(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_status_batch(Path(directory)))

    def test_bulk_status_unsupported_falls_back_to_individual_queries(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_status_batch(Path(directory), bulk_supported=False))

    async def exercise_status_batch(self, directory, *, bulk_supported=True):
        store = ArchiveStore(directory / "archive.sqlite")
        for name in ("first", "second"):
            store.submit("operator", name, {"flow": "local", "call": None, "options": {}},
                         sample_audio(), "audio/wav")
        requests = []
        async def respond(request):
            requests.append(request.url)
            if request.method == "POST":
                return httpx.Response(202, json={"job_id": "job" + str(len(requests))})
            if request.url.path == "/v1/transcription/jobs":
                if not bulk_supported:
                    return httpx.Response(404)
                return httpx.Response(200, json={"jobs": [
                    {"job_id": job, "status": "running"} for job in request.url.params.get_list("job_id")
                ]})
            return httpx.Response(200, json={"status": "running"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url="http://asr") as client:
            workflow = Workflow(store, client, LocalArchive(directory / "attachments"))
            await workflow.step(now=100)
            # Prevent the first submitted job from taking the second queued item's turn.
            with store.transaction() as db:
                db.execute("UPDATE workflow SET retry_at=103")
            await workflow.step(now=101)
            requests.clear()
            self.assertTrue(await workflow.step(now=104))
            self.assertTrue(await workflow.step(now=104))
        self.assertEqual(requests[0].params.get_list("job_id"), ["job2", "job1"])
        self.assertEqual([request.path for request in requests[1:]], [] if bulk_supported else [
            "/v1/transcription/jobs/job2", "/v1/transcription/jobs/job1",
        ])

    def test_failed_transcription_remains_retriable_until_archive_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_retry(Path(directory)))

    async def exercise_retry(self, directory):
        class FailingEngine(DemoEngine):
            def transcribe(self, audio, options):
                raise TimeoutError("transcription timed out")

        store = ArchiveStore(directory / "archive.sqlite")
        recording_id = store.submit("operator", "retry-1", {"flow": "local", "call": None, "options": {}},
                                    sample_audio(), "audio/wav")["recording_id"]
        db = directory / "transcription.sqlite"
        jobs = JobStore(db)
        app = create_app(db, {"archive-test": ("archive", True)})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                     headers={"Authorization": "Bearer archive-test"}) as client:
            workflow = Workflow(store, client, LocalArchive(directory / "attachments"))
            self.assertTrue(await workflow.step(now=100))
            failed_id = store.get("operator", recording_id)["transcription_job_id"]
            self.assertTrue(process_one(jobs, FailingEngine()))
            failed = jobs.get("archive", failed_id)
            jobs.purge(86400, 604800, now=failed["finished"] + 8 * 86400)
            self.assertIsNotNone(jobs.get("archive", failed_id))
            status = await client.get("/v1/transcription/jobs", params={"job_id": failed_id})
            self.assertIsNone(status.json()["jobs"][0]["expires_at"])
            self.assertTrue(await workflow.step(now=101))
            self.assertFalse(await workflow.step(now=101))
            self.assertTrue(await workflow.step(now=102))
            self.assertTrue(process_one(jobs, DemoEngine()))
            self.assertTrue(await workflow.step(now=3702))
            self.assertTrue(await workflow.step(now=3703))
            success_id = store.get("operator", recording_id)["transcription_job_id"]
            self.assertIsNone(jobs.get("archive", failed_id))
            acknowledged = jobs.get("archive", success_id)["acknowledged"]
            jobs.purge(86400, 604800, now=acknowledged + 86399)
            self.assertIsNotNone(jobs.get("archive", success_id))
            jobs.purge(86400, 604800, now=acknowledged + 86400)
            self.assertIsNone(jobs.get("archive", success_id))
            self.assertTrue(await workflow.step(now=3704))
            self.assertEqual(store.get("operator", recording_id)["status"], "archived")


    def test_expired_unconfirmed_result_is_retranscribed_but_saved_text_goes_to_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_unconfirmed_expiry(Path(directory)))

    async def exercise_unconfirmed_expiry(self, directory):
        store = ArchiveStore(directory / "archive.sqlite")
        recording_id = store.submit("operator", "unconfirmed-1", {"flow": "local", "call": None, "options": {}},
                                    sample_audio(), "audio/wav")["recording_id"]
        db = directory / "transcription.sqlite"
        jobs = JobStore(db)
        app = create_app(db, {"archive-test": ("archive", True)})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                     headers={"Authorization": "Bearer archive-test"}) as client:
            workflow = Workflow(store, client, LocalArchive(directory / "attachments"))
            self.assertTrue(await workflow.step(now=100))
            first_id = store.get("operator", recording_id)["transcription_job_id"]
            self.assertTrue(process_one(jobs, DemoEngine()))
            first = jobs.get("archive", first_id)
            jobs.purge(86400, 604800, now=first["finished"] + 604799)
            self.assertIsNotNone(jobs.get("archive", first_id))
            jobs.purge(86400, 604800, now=first["finished"] + 604800)
            self.assertIsNone(jobs.get("archive", first_id))
            self.assertTrue(await workflow.step(now=101))
            self.assertFalse(await workflow.step(now=101))
            self.assertTrue(await workflow.step(now=102))
            second_id = store.get("operator", recording_id)["transcription_job_id"]
            self.assertNotEqual(first_id, second_id)
            self.assertEqual(jobs.get_audio("archive", second_id)["audio"], sample_audio())
            self.assertTrue(process_one(jobs, DemoEngine()))
            self.assertTrue(await workflow.step(now=3702))
            second = jobs.get("archive", second_id)
            jobs.purge(86400, 604800, now=second["finished"] + 604800)
            self.assertIsNone(jobs.get("archive", second_id))
            self.assertTrue(await workflow.step(now=3703))
            self.assertTrue(await workflow.step(now=3704))
            self.assertEqual(store.get("operator", recording_id)["status"], "archived")

    def test_local_archive_recovers_after_attachment_write(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_workflow(Path(directory)))

    def test_system_transcription_uses_current_call_and_segment_filename(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_filenames(Path(directory)))

    async def exercise_filenames(self, directory):
        store = ArchiveStore(directory / "archive.sqlite")
        metadata = {"flow": "local", "options": {},
                    "call": {"external_call_id": "call", "callee": "111", "started_at": "2026-11-18T05:00:00Z"}}
        first, second = [store.submit("operator", part, metadata, sample_audio(), "audio/wav")
                         for part in ("first", "second")]
        self.assertEqual(first["call_id"], second["call_id"])
        with store.transaction() as db:
            db.execute("""INSERT INTO outbound_calls (owner,source,external_id,call_id,metadata,complete)
                          VALUES ('operator','demo','call',?,
                              '{"callee":"222","started_at":"2026-11-18T06:37:35Z"}',1)""", (first["call_id"],))
        detail = ConsoleCalls(store, {}).pending_detail(first["call_id"])
        expected = ["222_2026-11-18_14-37-35.wav", "222_2026-11-18_14-37-35_2.wav"]
        self.assertEqual([recording["name"] for recording in detail["recordings"]], expected)
        db = directory / "transcription.sqlite"
        jobs = JobStore(db)
        app = create_app(db, {"archive-test": ("archive", True)})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                     headers={"Authorization": "Bearer archive-test"}) as client:
            workflow = Workflow(store, client, LocalArchive(directory / "attachments"))
            for index, (item, filename) in enumerate(zip((first, second), expected)):
                now = 100 + index * 4
                self.assertTrue(await workflow.step(now=now))
                job_id = store.get("operator", item["recording_id"])["transcription_job_id"]
                self.assertEqual(jobs.external_jobs("web", (), job_id=job_id, system_only=True)[0]["filename"], filename)
                self.assertTrue(process_one(jobs, DemoEngine()))
                for offset in (1, 2, 3):
                    self.assertTrue(await workflow.step(now=now + offset))
                self.assertEqual(store.get("operator", item["recording_id"])["status"], "archived")

    async def exercise_workflow(self, directory):
        db = directory / "archive.sqlite"
        store = ArchiveStore(db)
        item = store.submit("operator", "request-1", {"flow": "local", "call": None, "options": {}},
                            sample_audio(), "audio/wav")
        recording_id = item["recording_id"]
        transcription_db = directory / "transcription.sqlite"
        jobs = JobStore(transcription_db)
        app = create_app(transcription_db, {"archive-test": ("archive", True)})
        adapter = LocalArchive(directory / "attachments")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://test",
                                     headers={"Authorization": "Bearer archive-test"}) as client:
            workflow = Workflow(store, client, adapter)
            self.assertTrue(await workflow.step(now=100))
            self.assertTrue(process_one(jobs, DemoEngine()))
            self.assertTrue(await workflow.step(now=101))
            details = store.get("operator", recording_id)
            job_id = details["transcription_job_id"]
            self.assertEqual(jobs.external_jobs("web", (), job_id=job_id, system_only=True)[0]["filename"], "录音-1.wav")
            self.assertIsNone(jobs.get("archive", job_id)["acknowledged"])

            workflow = Workflow(ArchiveStore(db), client, adapter)
            self.assertTrue(await workflow.step(now=102))
            self.assertIsNotNone(jobs.get("archive", job_id)["acknowledged"])

            # Simulate an interruption after publication and before committing the receipt.
            claim = workflow.store.claim(now=103, lease=1)
            first_receipts = workflow.deliver(claim)
            workflow = Workflow(ArchiveStore(db), client, adapter)
            self.assertTrue(await workflow.step(now=104))
            self.assertFalse(await workflow.step(now=105))

        details = workflow.store.get("operator", recording_id)
        self.assertEqual(details["status"], "archived")
        self.assertEqual(details["receipts"], first_receipts)
        audio = directory / "attachments" / details["receipts"]["audio"]
        transcript = directory / "attachments" / details["receipts"]["transcript"]
        self.assertEqual(audio.read_bytes(), sample_audio())
        self.assertEqual(transcript.read_text(encoding="utf-8"), details["transcript"]["text"])
