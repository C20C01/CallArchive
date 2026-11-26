import asyncio
import io
import json
import sys
import tempfile
import time
import unittest
import wave
from pathlib import Path

import httpx

from callarchive.backend.console_calls import ConsoleCalls
from callarchive.backend.store import ArchiveStore
from callarchive.backend.transcription_cleanup import flush_retired_transcriptions
from callarchive.transcription.api import create_app
from callarchive.transcription.engines import CommandEngine, DemoEngine
from callarchive.transcription.jobs import JobStore
from callarchive.transcription.worker import process_one


def sample_audio(value=0):
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as audio:
        audio.setparams((1, 2, 8000, 0, "NONE", "not compressed"))
        audio.writeframes(bytes([value, 0]) * 800)
    return buffer.getvalue()


class ServiceTest(unittest.TestCase):
    def test_command_engine_passes_audio_and_options(self):
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "recognize.py"
            script.write_text(
                "import json, sys\n"
                "from pathlib import Path\n"
                "audio, options, result = map(Path, sys.argv[1:])\n"
                "assert audio.read_bytes().startswith(b'RIFF')\n"
                "settings = json.loads(options.read_text(encoding='utf-8'))\n"
                "result.write_text(json.dumps({'text': settings['hotwords'][0]}), encoding='utf-8')\n",
                encoding="utf-8",
            )
            engine = CommandEngine([sys.executable, str(script)])
            self.assertEqual(engine.transcribe(sample_audio(), {"hotwords": ["sample"]}),
                             {"text": "sample"})

    def test_upload_dedupe_permissions_and_result(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "jobs.sqlite"
            asyncio.run(self.exercise_api(database))

    async def exercise_api(self, database):
        service = {"Authorization": "Bearer archive-test"}
        user = {"Authorization": "Bearer user-test"}
        app = create_app(database, {"archive-test": ( "archive", True), "user-test": ("user", False)})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            root = "/v1/transcription/jobs"

            async def submit(audio):
                return await client.post(root, headers=service, files={
                    "audio": ("call.wav", audio, "audio/wav"),
                    "metadata": (None, json.dumps({"request_id": "call-1"})),
                })

            initial = await submit(sample_audio())
            self.assertEqual(initial.status_code, 202)
            job = initial.json()["job_id"]
            self.assertEqual((await submit(sample_audio())).json()["job_id"], job)
            self.assertEqual((await submit(sample_audio(1))).status_code, 409)
            self.assertEqual((await submit(b"bad audio")).status_code, 422)
            self.assertEqual((await client.get(f"{root}/{job}", headers=user)).status_code, 404)
            self.assertEqual((await client.get(f"{root}/{job}")).status_code, 401)
            self.assertEqual((await client.get(f"{root}/{job}/result", headers=service)).status_code, 409)
            self.assertTrue(process_one(JobStore(database), DemoEngine()))
            result = await client.get(f"{root}/{job}/result", headers=service)
            self.assertEqual(result.status_code, 200)
            self.assertTrue(result.json()["text"])
            for _ in range(2):
                self.assertEqual((await client.post(f"{root}/{job}/ack", headers=service)).status_code, 204)


    def test_external_cleanup_removes_audio_but_keeps_the_result_until_expiry(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.external_success(Path(directory) / "jobs.sqlite"))

    async def external_success(self, database):
        app = create_app(database, {"api-test": ("partner", False), "web-test": ("web", False)},
                         web_gateway_owner="web")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            root = "/v1/transcription/jobs"
            jobs = JobStore(database)
            for token, owner in (("api-test", "partner"), ("web-test", "web")):
                headers = {"Authorization": f"Bearer {token}"}
                response = await client.post(root, headers=headers, files={
                    "audio": ("call.wav", sample_audio(), "audio/wav"),
                    "metadata": (None, json.dumps({"request_id": "test"})),
                })
                job_id = response.json()["job_id"]
                path = f"{root}/{job_id}"
                self.assertTrue(process_one(jobs, DemoEngine()))
                finished = jobs.get(owner, job_id)["finished"]
                jobs.purge(86400, 604800, now=finished + 86399)
                self.assertEqual((await client.get(path + "/audio", headers=headers)).content, sample_audio())
                jobs.discard_completed_external_audio()
                self.assertEqual((await client.get(path + "/audio", headers=headers)).status_code, 410)
                self.assertTrue((await client.get(path + "/result", headers=headers)).json()["text"])
                jobs.purge(86400, 604800, now=finished + 86400)
                self.assertEqual((await client.get(path + "/audio", headers=headers)).status_code, 404)
                self.assertEqual((await client.get(path + "/result", headers=headers)).status_code, 404)

    def test_unacknowledged_system_result_expires_at_the_seven_day_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            jobs = JobStore(Path(directory) / "jobs.sqlite")
            key = jobs.submit("archive", True, "system-sample", {}, sample_audio(), "audio/wav", "demo")
            self.assertTrue(process_one(jobs, DemoEngine()))
            finished = jobs.get("archive", key)["finished"]
            jobs.purge(86400, 604800, now=finished + 604799)
            self.assertTrue(jobs.get("archive", key)["result"])
            jobs.purge(86400, 604800, now=finished + 604800)
            self.assertIsNone(jobs.get("archive", key))

    def test_resubmission_after_expiry_returns_a_live_job_id(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_expired_resubmission(Path(directory) / "jobs.sqlite"))

    async def exercise_expired_resubmission(self, database):
        app = create_app(database, {"api-test": ("partner", False)}, retention=30)
        headers = {"Authorization": "Bearer api-test"}
        root = "/v1/transcription/jobs"
        files = {"audio": ("call.wav", sample_audio(), "audio/wav"),
                 "metadata": (None, json.dumps({"request_id": "resubmit"}))}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            initial = await client.post(root, headers=headers, files=files)
            old_id = initial.json()["job_id"]
            jobs = JobStore(database)
            finished = time.time() - 31
            claim = jobs.claim("demo", now=finished - 1)
            self.assertTrue(jobs.finish(claim, {"text": "expired"}, now=finished))

            resubmitted = await client.post(root, headers=headers, files=files)
            self.assertEqual(resubmitted.status_code, 202)
            new_id = resubmitted.json()["job_id"]
            self.assertNotEqual(new_id, old_id)
            self.assertEqual((await client.get(f"{root}/{old_id}", headers=headers)).status_code, 404)
            status = await client.get(f"{root}/{new_id}", headers=headers)
            self.assertEqual(status.status_code, 200)
            self.assertEqual(status.json()["status"], "queued")

    def test_retired_archive_group_removes_only_its_own_jobs_and_rejects_replays(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_archival_retirement(Path(directory) / "jobs.sqlite"))

    async def exercise_archival_retirement(self, database):
        app = create_app(database, {
            "archive-test": ("archive", True), "other-test": ("other", True), "web-test": ("archive", False),
        })
        jobs = JobStore(database)
        group, active_group = "a" * 32, "b" * 32
        base = "/v1/transcription/jobs"
        retire = "/internal/transcription/archive-groups/retire"
        archive = {"Authorization": "Bearer archive-test"}
        other = {"Authorization": "Bearer other-test"}
        web = {"Authorization": "Bearer web-test"}

        def submit(owner, request_id, archive_group, engine):
            return jobs.submit(owner, True, request_id, {}, sample_audio(), "audio/wav", engine,
                               archive_group=archive_group)

        failed = submit("archive", "failed", group, "failed")
        self.assertTrue(jobs.finish(jobs.claim("failed"), error="engine_failed"))
        active_failed = submit("archive", "active-failed", active_group, "failed")
        self.assertTrue(jobs.finish(jobs.claim("failed"), error="engine_failed"))
        running = submit("archive", "running", group, "running")
        running_claim = jobs.claim("running")
        queued = submit("archive", "queued", group, "queued")
        other_job = submit("other", "other", group, "other")

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            body = {"group_ids": [group]}
            self.assertEqual((await client.post(retire, json=body, headers=web)).status_code, 403)
            self.assertEqual((await client.get(f"{base}/{failed}/audio", headers=archive)).content, sample_audio())
            for _ in range(2):
                response = await client.post(retire, json=body, headers=archive)
                self.assertEqual(response.status_code, 204, response.text)
            for job in (failed, running, queued):
                self.assertEqual((await client.get(f"{base}/{job}", headers=archive)).status_code, 404)
            self.assertIsNone(jobs.claim("queued"))
            self.assertFalse(jobs.finish(running_claim, {"text": "discarded"}))
            self.assertEqual((await client.get(f"{base}/{active_failed}/audio", headers=archive)).content,
                             sample_audio())
            self.assertEqual((await client.get(f"{base}/{other_job}", headers=other)).status_code, 200)
            replay = await client.post(base, headers=archive, files={
                "audio": ("call.wav", sample_audio(), "audio/wav"),
                "metadata": (None, json.dumps({"request_id": "late-upload", "archive_group": group})),
            })
            self.assertEqual(replay.status_code, 410)
            self.assertEqual(jobs.queue_status("demo")["waiting"], 0)

    def test_archive_removal_retries_transcription_cleanup_after_unavailable_service(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_archive_removal(Path(directory)))

    async def exercise_archive_removal(self, directory):
        archive = ArchiveStore(directory / "archive.sqlite")
        metadata = {"flow": "local", "call": None, "options": {}}
        recording = archive.submit("operator", "call-1", metadata, sample_audio(), "audio/wav")
        group = recording["recording_id"]
        upstream_db = directory / "jobs.sqlite"
        transcription = create_app(upstream_db, {"service": ("archive", True)})
        jobs = JobStore(upstream_db)
        retired = jobs.submit("archive", True, "retired", {}, sample_audio(), "audio/wav", "demo",
                              archive_group=group)
        self.assertTrue(jobs.finish(jobs.claim("demo"), error="engine_failed"))
        active = jobs.submit("archive", True, "active", {}, sample_audio(), "audio/wav", "demo",
                             archive_group="e" * 32)

        item = archive.claim(now=time.time())
        archive.advance(item, "transcribing", job_id=retired)
        self.assertTrue(ConsoleCalls(archive, {}).remove(recording["call_id"] or group, "operator"))

        def unavailable(request):
            raise httpx.ConnectError("unavailable", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(unavailable), base_url="http://test") as client:
            self.assertEqual(await flush_retired_transcriptions(archive, client), 0)
        self.assertIsNotNone(jobs.get("archive", retired))

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=transcription), base_url="http://test",
                                     headers={"Authorization": "Bearer service"}) as client:
            self.assertEqual(await flush_retired_transcriptions(archive, client), 1)
            self.assertEqual(await flush_retired_transcriptions(archive, client), 0)
        self.assertIsNone(jobs.get("archive", retired))
        self.assertIsNotNone(jobs.get("archive", active))

    def test_recovery_fences_previous_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "jobs.sqlite"
            original = JobStore(path)
            job = original.submit("user", False, "call-1", {}, sample_audio(), "test.wav", "demo")
            claim = original.claim("demo", lease_seconds=1, now=100)
            recovered = JobStore(path)
            self.assertIsNone(recovered.claim("demo", now=100))
            new_claim = recovered.claim("demo", now=101)
            self.assertEqual(new_claim.job_id, job)
            self.assertFalse(original.finish(claim, {"text": "stale"}, now=101))
            self.assertTrue(recovered.finish(new_claim, {"text": "recovered"}, now=102))
            self.assertEqual(recovered.get("user", job)["status"], "succeeded")
