import asyncio
import hashlib
import sqlite3
from contextlib import closing
import tempfile
import unittest
from threading import Event
from pathlib import Path

import httpx

from callarchive.backend.api import create_app as create_backend
from callarchive.backend.browser_jobs import BrowserJobs
from callarchive.backend.password_accounts import PasswordAccount
from callarchive.backend.store import ArchiveStore
from callarchive.http import Options
from callarchive.transcription.api import create_app as create_transcription
from callarchive.transcription.engines import DemoEngine
from callarchive.transcription.jobs import JobStore
from callarchive.transcription.worker import process_one
from test_service import sample_audio
from test_workbench import hidden_field


class BrowserTranscriptionTest(unittest.TestCase):
    def test_upload_and_download_result_without_creating_archive_recording(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise(Path(directory)))

    async def exercise(self, root):
        base = "/workbench/transcription"
        upstream_db = root / "transcription.sqlite"
        upstream_app = create_transcription(upstream_db, {"ui-token": ("web", False)})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=upstream_app), base_url="http://transcribe",
                                     headers={"Authorization": "Bearer ui-token"}) as upstream:
            backend = create_backend(root / "archive.sqlite", {
                "operator": ("operator", False), "other": ("other", False),
            }, workbench_transcription=upstream, enable_workbench=True)

            async def login(client, credential):
                login_page = await client.get("/workbench/login")
                response = await client.post("/workbench/login", data={
                    "csrf": hidden_field(login_page.text, "csrf"), "credential": credential,
                })
                self.assertEqual(response.status_code, 303)

            transport = httpx.ASGITransport(app=backend)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                await login(client, "operator")
                page = await client.get(base)
                data = {"csrf": hidden_field(page.text, "csrf"),
                        "request_id": hidden_field(page.text, "request_id"),
                        "hotwords": "测试\n项目", "speaker_count": "2"}
                files = {"audio": ("sample.wav", sample_audio(), "audio/wav")}
                first, second = [await client.post(base, data=data, files=files) for _ in range(2)]
                self.assertEqual(first.status_code, 303, first.text)
                self.assertEqual(first.headers["location"], second.headers["location"])
                detail = first.headers["location"]
                self.assertTrue(process_one(JobStore(upstream_db), DemoEngine()))
                self.assertFalse(process_one(JobStore(upstream_db), DemoEngine()))
                response = await client.get(detail + "/text")
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.text)
                self.assertIn("attachment;", response.headers["content-disposition"])
                self.assertIn(response.text, (await client.get(detail)).text)
                self.assertEqual((await client.get("/v1/archive/recordings",
                                                  headers={"Authorization": "Bearer operator"})).json()["items"], [])
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as other:
                await login(other, "other")
                self.assertEqual((await other.get(detail + "/text")).status_code, 404)

    def test_recording_transcription_page_uses_new_service(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_recording_transcription(Path(directory)))

    async def exercise_recording_transcription(self, root):
        base = "/transcriptions"
        api = base + "/api"
        upstream_db = root / "transcription.sqlite"
        upstream_app = create_transcription(upstream_db, {"browser-upstream": ("web", False)})
        upstream_transport = httpx.ASGITransport(app=upstream_app)
        async with httpx.AsyncClient(transport=upstream_transport, base_url="http://transcribe",
                                     headers={"Authorization": "Bearer browser-upstream"}) as upstream:
            backend = create_backend(root / "archive.sqlite", {
                "operator": ("operator", False), "other": ("other", False),
            }, operator_transcription=upstream)
            transport = httpx.ASGITransport(app=backend)

            async def login(client, credential):
                page = await client.get(base)
                self.assertNotIn('class="return-home"', page.text)
                self.assertEqual((await client.get(api + "/status")).status_code, 401)
                signed_in = await client.post(base, data={
                    "csrf": hidden_field(page.text, "csrf"), "credential": credential,
                })
                self.assertEqual(signed_in.status_code, 303)
                arrival = await client.get(base)
                self.assertIn('<main class="console-enter">', arrival.text)
                self.assertIn('class="return-home"', arrival.text)
                self.assertNotIn('<main class="console-enter">', (await client.get(base)).text)
                return hidden_field(arrival.text, "csrf")

            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                csrf = await login(client, "operator")
                files = {"audio": ("sample.wav", sample_audio(), "audio/wav")}
                data = {"request_id": "sample-1", "csrf": csrf}
                forbidden = await client.post(api, data={**data, "csrf": "wrong"}, files=files)
                self.assertEqual(forbidden.status_code, 403)
                first = await client.post(api, data=data, files=files)
                self.assertEqual(first.status_code, 202, first.text)
                self.assertEqual(first.json(), (await client.post(api, data=data, files=files)).json())
                task = first.json()["task_id"]
                browser_jobs = BrowserJobs(ArchiveStore(root / "archive.sqlite"))
                abandoned = [browser_jobs.reserve("operator", f"abandoned-{index}", sample_audio(),
                                                  Options().model_dump(), "sample.wav") for index in range(36)]
                status = (await client.get(api + "/status")).json()
                self.assertEqual(status["queue"], {"online": False, "busy": False, "waiting": 1})
                self.assertEqual([(item["task_id"], item["state"]) for item in status["tasks"]], [(task, "queued")])
                started = Event()
                release = Event()

                class PausedEngine(DemoEngine):
                    def transcribe(self, audio, options):
                        try:
                            from callarchive.transcription.progress import report_progress
                            report_progress("recognizing", 2, 6)
                        finally:
                            started.set()
                        if not release.wait(5):
                            raise TimeoutError("Progress verification did not finish")
                        return super().transcribe(audio, options)

                worker = asyncio.create_task(asyncio.to_thread(process_one, JobStore(upstream_db), PausedEngine()))
                try:
                    self.assertTrue(await asyncio.to_thread(started.wait, 5))
                    active = (await client.get(api + "/status")).json()["tasks"][0]
                    self.assertEqual(active["state"], "running")
                    self.assertEqual(active["progress"], {"stage": "recognizing", "completed": 2, "total": 6})
                finally:
                    release.set()
                    self.assertTrue(await worker)
                status = (await client.get(api + "/status")).json()
                self.assertIsNone(status["tasks"][0].get("progress"))
                self.assertEqual(status["queue"], {"online": True, "busy": False, "waiting": 0})
                result = (await client.get(f"{api}/{task}")).json()
                self.assertEqual(result["state"], "done")
                self.assertTrue(result["text"])
                self.assertIsNotNone(result["expires_at"])

                with JobStore(upstream_db).connection() as connection:
                    connection.execute("UPDATE jobs SET finished=created-86400 WHERE owner='web'")
                self.assertEqual((await client.get(api + "/status")).json()["tasks"], [])
                self.assertEqual((await client.get(f"{api}/{task}")).status_code, 404)
                with closing(sqlite3.connect(root / "archive.sqlite")) as connection:
                    self.assertIsNone(connection.execute(
                        "SELECT id FROM browser_transcriptions WHERE id=?", (task,)
                    ).fetchone())
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as other:
                await login(other, "other")
                store = JobStore(upstream_db)
                system_task = store.submit("archive", True, "system-1", {}, sample_audio(), "audio/wav", "demo")
                self.assertEqual(store.claim("demo").job_id, system_task)
                status = (await other.get(api + "/status")).json()
                self.assertEqual(status["tasks"], [])
                self.assertEqual(status["queue"], {"online": True, "busy": True, "waiting": 0})
                self.assertEqual((await other.get(f"{api}/{task}")).status_code, 404)

            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                csrf = await login(client, "operator")
                retried = await client.post(api, data={"request_id": "abandoned-0", "csrf": csrf}, files=files)
                self.assertEqual(retried.status_code, 202)
                self.assertEqual(retried.json()["task_id"], abandoned[0]["id"])

    def test_external_tasks_follow_browser_access_rules(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_external_tasks(Path(directory)))

    async def exercise_external_tasks(self, root):
        db = root / "transcription.sqlite"
        transcription = create_transcription(db, {
            "gateway-token": ("web", False), "customer-token": ("customer-api", False),
            "archive-token": ("archive", True),
        }, web_gateway_owner="web")
        transport = httpx.ASGITransport(app=transcription)
        async with httpx.AsyncClient(transport=transport, base_url="http://transcribe",
                                     headers={"Authorization": "Bearer gateway-token"}) as upstream:
            files = {"audio": ("external.wav", sample_audio(), "audio/wav"),
                     "metadata": (None, '{"request_id":"external-1"}')}
            submit = await upstream.post("/v1/transcription/jobs", files=files,
                                         headers={"Authorization": "Bearer customer-token"})
            self.assertEqual(submit.status_code, 202)
            external_id = submit.json()["job_id"]
            archive = await upstream.post("/v1/transcription/jobs",
                                          files={**files, "audio": ("recording", sample_audio(), "audio/wav")},
                                          headers={"Authorization": "Bearer archive-token"})
            self.assertEqual(archive.status_code, 202)
            system_id = archive.json()["job_id"]
            gateway_job = await upstream.post("/v1/transcription/jobs", files=files)
            self.assertEqual(gateway_job.status_code, 202)
            self.assertEqual((await upstream.get("/v1/transcription/external-jobs",
                             headers={"Authorization": "Bearer customer-token"})).status_code, 403)
            store = JobStore(db)
            while process_one(store, DemoEngine()):
                pass

            archive_store = ArchiveStore(root / "archive.sqlite")
            recording = archive_store.submit("operator", "legacy-name", {
                "flow": "local", "options": {},
                "call": {"callee": "222", "started_at": "2026-11-18T06:37:35Z"},
            }, sample_audio(), "audio/wav")
            with archive_store.transaction() as connection:
                connection.execute("INSERT INTO workflow (recording_id,job_id) VALUES (?,?)",
                                   (recording["recording_id"], system_id))
            accounts = []
            roles = {"operator": "external", "other": "external", "admin": "admin", "ops": "ops"}
            for name, role in roles.items():
                salt = name.encode().ljust(16, b"\0")
                verifier = hashlib.scrypt(name.encode(), salt=salt, n=2**14, r=8, p=1)
                accounts.append(PasswordAccount(name, role, salt, verifier))
            for admin_results, admin_system_results in ((False, False), (True, False), (True, True)):
                backend = create_backend(root / "archive.sqlite", {
                    name: (name, False) for name in ("operator", "other", "admin")
                }, admins={"admin"}, password_accounts=tuple(accounts), operator_transcription=upstream,
                    transcription_api_owners={"operator": ["customer-api"], "other": ["other-api"]},
                    transcription_admin_results=admin_results,
                    transcription_admin_system_results=admin_system_results)
                backend_transport = httpx.ASGITransport(app=backend)
                for name, allowed in (("operator", True), ("other", False), ("admin", admin_results), ("ops", False)):
                    async with httpx.AsyncClient(transport=backend_transport, base_url="http://test") as client:
                        page = await client.get("/transcriptions")
                        logged_in = await client.post("/transcriptions", data={
                            "csrf": hidden_field(page.text, "csrf"), "credential": name,
                        })
                        self.assertEqual(logged_in.status_code, 303)
                        detail_url = f"/transcriptions/api/external:{external_id}"
                        result = await client.get(detail_url)
                        self.assertEqual(result.status_code, 200 if allowed else 404)
                        if allowed:
                            self.assertTrue(result.json()["text"])
                        tasks = (await client.get("/transcriptions/api/status")).json()["tasks"]
                        by_source = {task["source"]: task for task in tasks}
                        sources = {"admin": {"API", "system"}, "operator": {"API"}, "other": set(),
                                   "ops": {"system"} if admin_system_results else set()}
                        self.assertEqual(set(by_source), sources[name])
                        if "API" in by_source:
                            external = by_source["API"]
                            self.assertEqual(external["task_id"], "external:" + external_id)
                            self.assertEqual(external["filename"], "external.wav")
                            self.assertEqual(external["can_view_result"], allowed)
                        for prefix in ("system", "external"):
                            response = await client.get(f"/transcriptions/api/{prefix}:{system_id}")
                            permitted = name in {"admin", "ops"} and prefix == "system" and admin_system_results
                            self.assertEqual(response.status_code, 200 if permitted else 404)
                            if permitted:
                                self.assertTrue(response.json()["text"])
                        if "system" in by_source:
                            system = by_source["system"]
                            self.assertEqual(system["task_id"], "system:" + system_id)
                            self.assertEqual(system["filename"], "222_2026-11-18_14-37-35.wav")
                            self.assertEqual(system["can_view_result"], admin_system_results)

            with store.connection() as connection:
                connection.execute("UPDATE jobs SET finished=created-86400 WHERE id IN (?, ?)",
                                   (external_id, system_id))
                connection.execute("UPDATE jobs SET audio=NULL, result=NULL WHERE id=?", (external_id,))
            listing = await upstream.get("/v1/transcription/external-jobs", params={"include_system": "true"})
            self.assertEqual([item["job_id"] for item in listing.json()["jobs"]], [system_id])
            old_result = await upstream.get(f"/v1/transcription/jobs/{external_id}/result",
                                            headers={"Authorization": "Bearer customer-token"})
            self.assertEqual(old_result.status_code, 404)
            self.assertIsNone(store.get("customer-api", external_id))
