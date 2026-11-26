import asyncio
import hashlib
import re
import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlencode

import httpx

from callarchive.backend.api import create_app
from callarchive.backend.password_accounts import load_password_accounts
from callarchive.observability import RuntimeLog
from callarchive.backend.store import ArchiveStore
from test_service import sample_audio


def hidden_field(page, name):
    return re.search(rf'name="{name}" value="([^"]+)"', page).group(1)


class WorkbenchTest(unittest.TestCase):
    def test_login_deep_link_csrf_owner_permissions_and_logout(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise(Path(directory) / "archive.sqlite"))

    async def exercise(self, database):
        store = ArchiveStore(database)
        item = store.submit("operator", "existing", {"flow": "local", "call": None, "options": {}},
                            sample_audio(), "audio/wav")
        detail = "/workbench/recordings/" + item["recording_id"]
        app = create_app(database, {
            "operator-token": ("operator", False), "other-token": ("other", False),
            "worker-token": ("worker", True),
        }, enable_workbench=True)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            redirect = await client.get(detail)
            self.assertEqual(redirect.status_code, 303)
            page = await client.get(redirect.headers["location"])
            csrf = hidden_field(page.text, "csrf")

            response = await client.post("/workbench/login", data={
                "csrf": csrf, "credential": "worker-token", "next": detail,
            })
            self.assertEqual(response.status_code, 401)
            csrf = hidden_field(response.text, "csrf")
            response = await client.post("/workbench/login", data={
                "csrf": csrf, "credential": "operator-token", "next": detail,
            })
            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], detail)
            self.assertIn("httponly", response.headers["set-cookie"].lower())
            page = await client.get(detail)
            self.assertEqual(page.status_code, 200)
            self.assertIn("no-store", page.headers["cache-control"])
            self.assertIn('<dialog id="recording-detail"', page.text)
            detail_query = {"recording": item["recording_id"], "fragment": "1", "view": "pending"}
            fragment = await client.get("/workbench", params=detail_query)
            self.assertTrue(fragment.text.startswith("<dialog"))
            self.assertNotIn("<table", fragment.text)
            self.assertIn('data-close-url="/workbench?view=pending"', fragment.text)
            self.assertEqual((await client.get("/workbench/details.js")).status_code, 200)

            remove = await client.get(detail + "/remove", params={"back": "/workbench?view=pending"})
            response = await client.post(detail + "/remove", data={
                "csrf": hidden_field(remove.text, "csrf"), "confirm": "yes", "back": "/workbench?view=pending",
            })
            self.assertEqual(response.headers["location"], "/workbench?view=pending&recording=" + item["recording_id"])
            removed = await client.get(response.headers["location"])
            self.assertIn('id="recording-detail"', removed.text)
            self.assertIn('disabled>移除</button>', removed.text)
            self.assertIn('disabled>重试</button>', removed.text)
            response = await client.post(detail + "/restore", data={
                "csrf": hidden_field(removed.text, "csrf"), "confirm": "yes", "back": "//invalid.example",
            })
            self.assertEqual(response.headers["location"], "/workbench?recording=" + item["recording_id"])
            self.assertEqual((await client.get(detail + "/audio")).content, sample_audio())
            self.assertEqual((await client.post(detail + "/retry", data={"csrf": "wrong"})).status_code, 403)
            first = await client.get("/workbench")
            csrf = hidden_field(remove.text, "csrf")
            self.assertIn("scope=\"col\"", first.text)
            self.assertIn('href="/">返回</a>', first.text)
            self.assertNotIn("/workbench/logout", first.text)
            landing = await client.get("/", follow_redirects=True)
            self.assertIn("录音归档与转写", landing.text)
            self.assertEqual((await client.post("/workbench/logout", data={"csrf": csrf})).status_code, 303)
            self.assertEqual((await client.get(detail)).status_code, 303)

            sign_in = "/workbench/login?" + urlencode({"next": "//other.invalid"})
            page = await client.get(sign_in)
            response = await client.post("/workbench/login", data={
                "csrf": hidden_field(page.text, "csrf"), "credential": "other-token",
                "next": "//other.invalid",
            })
            self.assertEqual(response.headers["location"], "/workbench")
            self.assertEqual((await client.get(detail)).status_code, 404)
            self.assertEqual((await client.get("/workbench", params=detail_query)).status_code, 404)

    def test_call_filters_search_beyond_first_page_and_keep_owner_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_filters(Path(directory) / "archive.sqlite"))

    async def exercise_filters(self, database):
        store = ArchiveStore(database)

        def submit(owner, number, caller):
            return store.submit(owner, f"call-{number}", {
                "flow": "local", "options": {},
                "call": {"started_at": "2026-11-24T09:43:27+08:00", "caller": caller, "callee": number},
            }, sample_audio(), "audio/wav")

        first = submit("operator", "10001", "20001")
        another = submit("operator", "10002", "20002")
        private = submit("other", "10001", "20001")
        for index in range(35):
            submit("operator", str(30000 + index), "30001")
        app = create_app(database, {"operator-token": ("operator", False)}, enable_workbench=True)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            login = await client.get("/workbench/login")
            await client.post("/workbench/login", data={
                "csrf": hidden_field(login.text, "csrf"), "credential": "operator-token",
            })
            page = await client.get("/workbench")
            self.assertNotIn(first["recording_id"], page.text)
            page = await client.get("/workbench", params={
                "view": "pending", "day": "2026-11-24", "mobile": "10001", "assignee": "20001", "status": "queued",
            })
            self.assertEqual(page.status_code, 200)
            self.assertIn(first["recording_id"], page.text)
            self.assertNotIn(private["recording_id"], page.text)
            self.assertNotIn(another["recording_id"], page.text)
            self.assertIn(">2026-11-24 09:43</time>", page.text)
            page = await client.get("/workbench", params={"day": "", "assignee": "operator", "mobile": ""})
            self.assertEqual(page.status_code, 200)
            self.assertIn("assignee=operator&amp;view=all&amp;cursor=", page.text)
            page = await client.get("/workbench", params={"view": "archived", "mobile": "10001"})
            self.assertNotIn(first["recording_id"], page.text)
            self.assertEqual((await client.get("/workbench?all_owners=1")).status_code, 403)


    def test_existing_passwords_map_to_three_roles(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_password_roles(Path(directory)))

    async def exercise_password_roles(self, directory):
        def account(owner, role, password, salt):
            hashed = hashlib.scrypt(password.encode(), salt=salt, n=2**14, r=8, p=1)
            return f"{owner} = {role}:{salt.hex()}:{hashed.hex()}"

        fixture = directory / "accounts.ini"
        fixture.write_text("\n".join([
            "[api_users]",
            account("manager", "admin", "fixture-manager", b"a" * 16),
            account("operator", "ops", "fixture-operator", b"b" * 16),
            account("vendor", "external", "fixture-vendor", b"c" * 16),
        ]), encoding="utf-8")
        database = directory / "archive.sqlite"
        item = ArchiveStore(database).submit("import-source", "existing", {"flow": "local", "call": None, "options": {}},
                                             sample_audio(), "audio/wav")
        app = create_app(database, {"machine-token": ("source", False)},
                         password_accounts=load_password_accounts(fixture),
                         runtime_log=RuntimeLog(directory / "log.sqlite"), workbench_transcription=object(),
                         enable_workbench=True)
        for password, can_archive, can_read_logs in (
            ("fixture-manager", True, True), ("fixture-operator", True, False), ("fixture-vendor", False, False),
        ):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                signin = await client.get("/workbench/login")
                self.assertNotIn('name="username"', signin.text)
                response = await client.post("/workbench/login", data={
                    "csrf": hidden_field(signin.text, "csrf"), "credential": password,
                })
                self.assertEqual(response.status_code, 303)
                home = await client.get(response.headers["location"])
                self.assertEqual('href="/workbench?view=pending"' in home.text, can_archive)
                self.assertEqual('href="/workbench/operations"' in home.text, can_read_logs)
                self.assertIn('href="/workbench/transcription"', home.text)
                archive_status = 200 if can_archive else 403
                self.assertEqual((await client.get("/workbench")).status_code, archive_status)
                self.assertEqual((await client.get("/workbench/upload")).status_code, archive_status)
                self.assertEqual((await client.get("/workbench", params={
                    "recording": item["recording_id"], "fragment": 1,
                })).status_code, archive_status)
                self.assertEqual((await client.get("/workbench/operations")).status_code, 200 if can_read_logs else 403)

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            signin = await client.get("/workbench/login")
            response = await client.post("/workbench/login", data={
                "csrf": hidden_field(signin.text, "csrf"), "credential": "machine-token",
            })
            self.assertEqual(response.status_code, 401)


    def test_bulk_remove_is_atomic_and_returns_to_filtered_list(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_bulk_remove(Path(directory) / "archive.sqlite"))

    async def exercise_bulk_remove(self, database):
        store = ArchiveStore(database)

        def submit(owner, name):
            return store.submit(owner, name, {"flow": "local", "call": None, "options": {}},
                                sample_audio(), "audio/wav")["recording_id"]

        first = submit("operator", "first")
        second = submit("operator", "second")
        other = submit("someone", "private")
        app = create_app(database, {"token": ("operator", False)}, enable_workbench=True)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            signin = await client.get("/workbench/login")
            await client.post("/workbench/login", data={
                "csrf": hidden_field(signin.text, "csrf"), "credential": "token",
            })
            page = await client.get("/workbench?view=pending")
            self.assertIn('id="select-page"', page.text)
            data = {"csrf": hidden_field(page.text, "csrf"), "back": "/workbench?view=pending", "confirm": "yes"}
            failure = await client.post("/workbench/bulk-remove", data={**data, "recording_ids": [first, other]})
            self.assertEqual(failure.headers["location"], "/workbench?view=pending&bulk=unavailable")
            self.assertEqual(store.get("operator", first)["status"], "queued")
            okay = await client.post("/workbench/bulk-remove", data={**data, "recording_ids": [first, second]})
            self.assertEqual(okay.headers["location"], "/workbench?view=pending&bulk=removed")
            self.assertEqual(store.get("operator", first)["status"], "removed")
            self.assertEqual(store.get("operator", second)["status"], "removed")
            self.assertEqual(store.get("someone", other)["status"], "queued")
