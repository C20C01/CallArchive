import asyncio
import json
from datetime import datetime, timezone
import tempfile
import unittest
from pathlib import Path

import httpx

from callarchive.backend.api import create_app
from callarchive.observability import RuntimeLog
from test_workbench import hidden_field


class ViewerTest(unittest.TestCase):
    def test_runtime_log_requires_admin_password_role(self):
        import hashlib

        from callarchive.backend.password_accounts import PasswordAccount

        async def exercise(database):
            accounts = tuple(
                PasswordAccount(name, name, salt, hashlib.scrypt(name.encode(), salt=salt, n=2**14, r=8, p=1))
                for name, salt in (("admin", b"a" * 16), ("ops", b"b" * 16), ("external", b"c" * 16))
            )
            log = RuntimeLog(database.with_name("runtime.sqlite"))
            app = create_app(database, {}, admins={"ops", "external"}, password_accounts=accounts,
                             runtime_log=log)
            transport = httpx.ASGITransport(app=app)
            for role in ("admin", "ops", "external"):
                async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                    signin = await client.get("/login")
                    self.assertEqual((await client.post("/login", data={
                        "credential": role, "csrf": hidden_field(signin.text, "csrf"),
                    })).status_code, 303)
                    expected = 200 if role == "admin" else 403
                    self.assertEqual((await client.get("/admin/task-logs")).status_code, expected)

        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(exercise(Path(directory) / "archive.sqlite"))

    def test_only_administrators_can_search_runtime_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise(Path(directory)))

    async def exercise(self, root):
        log = RuntimeLog(root / "log.sqlite")
        payload = {"time": datetime.now(timezone.utc).isoformat(), "level": "INFO", "component": "archive-worker",
                   "event": "archive_stage", "recording_id": "recording-test", "status": "archived"}
        log.append(payload, json.dumps(payload))
        app = create_app(root / "archive.sqlite", {
            "admin": ("admin", False), "operator": ("operator", False), "service": ("admin", True),
        }, admins={"admin"}, runtime_log=log, enable_workbench=True)
        transport = httpx.ASGITransport(app=app)
        url = "/v1/archive/operations/logs?related_id=recording-test"
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            for token in ("operator", "service"):
                response = await client.get(url, headers={"Authorization": f"Bearer {token}"})
                self.assertEqual(response.status_code, 403)
            response = await client.get(url, headers={"Authorization": "Bearer admin"})
            self.assertEqual(response.json()["items"][0]["event"], "archive_stage")
            self.assertFalse((await client.get(url.replace("recording-test", "other"),
                                              headers={"Authorization": "Bearer admin"})).json()["items"])
            signin = await client.get("/workbench/login")
            await client.post("/workbench/login", data={
                "csrf": hidden_field(signin.text, "csrf"), "credential": "admin",
            })
            response = await client.get("/workbench/operations?related_id=recording-test")
            self.assertIn("archive_stage", response.text)
            self.assertEqual(response.status_code, 200)


    def test_runtime_page_prioritizes_before_paging_and_preserves_admin_access(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_runtime_page(Path(directory)))

    async def exercise_runtime_page(self, root):
        log = RuntimeLog(root / "runtime.sqlite")
        sample = {"time": datetime.now(timezone.utc).isoformat(), "component": "archive-worker", "recording_id": "recording-1"}
        warning = {**sample, "level": "WARNING", "event": "archive_stage", "status": "needs_review",
                   "reason": "archive_attachment_unknown"}
        log.append(warning, json.dumps(warning))
        for status, component, event, fields in (
            ("needs_input", "archive-worker", "archive_stage", {"reason": "archive_target_missing"}),
            (None, "outbound-import", "outbound_import_finished", {"imported": 5, "pending": 2}),
        ):
            row = {**sample, "level": "INFO", "component": component, "event": event, **fields}
            if status:
                row["status"] = status
            log.append(row, json.dumps(row))
        typical = {**sample, "level": "INFO", "event": "process_started"}
        for _ in range(32):
            log.append(typical, json.dumps(typical))
        for event, fields in (("report_finished", {}), ("alerts_finished", {"confirmed": 11}),
                              ("report_retry_delivery", {"status": "confirmed", "report_day": "2026-11-25",
                                                        "page": 2, "total_pages": 3})):
            payload = {**sample, "component": "notification", "level": "INFO", "event": event, **fields}
            log.append(payload, json.dumps(payload))
        app = create_app(root / "archive.sqlite", {"admin-token": ("admin", False), "ops-token": ("ops", False)},
                         admins={"admin"}, runtime_log=log)
        path = "/admin/task-logs"
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            page = await client.get(path)
            self.assertNotIn('class="return-home"', page.text)
            self.assertNotIn("archive_attachment_unknown", page.text)
            self.assertEqual((await client.post(path, data={"credential": "admin-token", "csrf": "wrong"})).status_code, 403)
            page = await client.get(path)
            denied = await client.post(path, data={"credential": "ops-token", "csrf": hidden_field(page.text, "csrf")})
            self.assertEqual(denied.status_code, 403)
            allowed = await client.post(path, data={"credential": "admin-token", "csrf": hidden_field(denied.text, "csrf")})
            self.assertEqual(allowed.status_code, 303)
            page = await client.get(path)
            self.assertIn('<main class="console-enter">', page.text)
            self.assertIn("附件写入结果不明", page.text)
            self.assertNotIn("<summary>定时任务</summary>", page.text)

            home = await client.get("/")
            self.assertIn('href="/admin/task-logs"', home.text)
            self.assertNotIn("/admin/schedules", home.text)
            self.assertEqual((await client.get("/admin/schedules")).status_code, 404)
            self.assertEqual((await client.get("/admin/schedules/status")).status_code, 404)
            self.assertIn('href="/">返回主页</a>', page.text)
            self.assertNotIn("process_started", page.text)
            self.assertIn("no-store", page.headers["cache-control"])
            all_logs = (await client.get(path + "?mode=all")).text
            self.assertIn("process_started", all_logs)
            self.assertIn("日报补发", all_logs)
            self.assertIn("第 2/3 页", all_logs)
            self.assertNotIn("report_finished", all_logs)
            self.assertNotIn("alerts_finished", all_logs)
            attention = await client.get(path + "?mode=attention")
            self.assertIn("附件写入结果不明", attention.text)
            self.assertIn("下载失败次数 2", attention.text)
            self.assertNotIn("未找到应聘档案", attention.text)
            self.assertNotIn("日报补发", attention.text)
            scoped_attention = await client.get(path + "?mode=attention&component=outbound-import")
            self.assertIn("下载失败次数 2", scoped_attention.text)
            self.assertNotIn("附件写入结果不明", scoped_attention.text)
            other = await client.get(path + "?related_id=missing")
            self.assertIn("没有匹配的重点日志", other.text)
