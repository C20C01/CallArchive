import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from callarchive.backend.api import create_app
from callarchive.observability import RuntimeLog
from test_workbench import hidden_field


class NativeConsoleTest(unittest.TestCase):
    def test_home_credentials_and_role_links(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            app = create_app(root / "archive.sqlite", {
                "admin-token": ("admin", False), "external-token": ("external", False),
            }, admins={"admin"}, runtime_log=RuntimeLog(root / "runtime.sqlite"))
            with TestClient(app) as browser:
                page = browser.get("/")
                self.assertIn('name="credential"', page.text)
                self.assertEqual(browser.post("/login", data={"csrf": "bad", "credential": "admin-token"}).status_code, 403)
                page = browser.get("/")
                signed_in = browser.post("/login", data={
                    "csrf": hidden_field(page.text, "csrf"), "credential": "admin-token",
                    "destination": "/admin/task-logs?mode=all",
                }, follow_redirects=False)
                self.assertEqual(signed_in.headers["location"], "/admin/task-logs?mode=all")
                logs = browser.get("/admin/task-logs?mode=all")
                self.assertNotIn('name="credential"', logs.text)
                self.assertIn('<main class="console-enter">', logs.text)
                self.assertNotIn('<main class="console-enter">', browser.get("/admin/task-logs").text)
                home = browser.get("/")
                self.assertIn('/calls/pending', home.text)
                self.assertNotIn('class="home console-enter"', home.text)
            with TestClient(app) as browser:
                page = browser.get("/")
                home = browser.post("/login", data={"csrf": hidden_field(page.text, "csrf"),
                                                    "credential": "external-token"})
                self.assertIn('class="home console-enter"', home.text)
                home = browser.get("/")
                self.assertNotIn('class="home console-enter"', home.text)
                self.assertNotIn('/calls/pending', home.text)
                self.assertEqual(browser.get("/admin/task-logs").status_code, 403)

    def test_direct_archive_links_keep_destination_and_stage_reveal(self):
        from urllib.parse import parse_qs, urlsplit

        with tempfile.TemporaryDirectory() as directory:
            app = create_app(Path(directory) / "archive.sqlite", {"admin-token": ("admin", False)}, admins={"admin"})
            for destination, title in (("/calls/pending?status=waiting_recording", "待归档话单"),
                                       ("/calls/pending?mobile=%2B8610002&owner=%E7%94%B2", "待归档话单"),
                                       ("/calls/recent", "近期归档")):
                with TestClient(app) as browser:
                    signin = browser.get(destination)
                    self.assertEqual(parse_qs(urlsplit(str(signin.url)).query)["destination"], [destination])
                    self.assertIn(f"<h1>{title}</h1>", signin.text)
                    self.assertNotIn('class="return-home"', signin.text)
                    signed_in = browser.post("/login", data={
                        "csrf": hidden_field(signin.text, "csrf"), "credential": "admin-token",
                        "destination": destination,
                    })
                    self.assertEqual(signed_in.url.path, urlsplit(destination).path)
                    self.assertEqual(urlsplit(str(signed_in.url)).query, urlsplit(destination).query)
                    self.assertIn('<main class="console-enter">', signed_in.text)
                    self.assertIn('class="return-home"', signed_in.text)
                    self.assertNotIn('<main class="console-enter">', browser.get(destination).text)

    def test_pending_and_recent_use_native_archive_data(self):
        import json
        import time

        from callarchive.backend.store import ARCHIVE_CACHE_SECONDS, ArchiveStore
        from test_service import sample_audio

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ArchiveStore(root / "archive.sqlite")
            resumed = []
            app = create_app(store.database, {"admin-token": ("admin", False),
                                             "external-token": ("external", False)},
                             admins={"admin"}, followup_names={"111": "Agent", "222": "Agent"},
                             resume_call=resumed.append)

            def submit(request_id, call_id, caller):
                return store.submit("provider", request_id, {
                    "flow": "local", "options": {}, "call": {
                        "external_call_id": call_id, "caller": caller, "callee": "123456789",
                        "started_at": "2026-11-23T17:00:00Z",
                    },
                }, sample_audio(), "audio/wav", duration_ms=5000)

            first = submit("first", "call-a", "111")
            second = submit("second", "call-a", "111")
            other = submit("other", "call-b", "222")
            archived = submit("recent", "call-recent", "333")
            with store.transaction() as db:
                db.execute("UPDATE recordings SET status='needs_input' WHERE id=?", (first["recording_id"],))
                db.execute("INSERT INTO workflow (recording_id,error) VALUES (?,?)",
                           (first["recording_id"], "archive_target_missing"))
                db.execute("INSERT INTO recording_recovery (recording_id,resume_status) VALUES (?,?)",
                           (first["recording_id"], "delivering"))
                db.execute("UPDATE recordings SET status='archived' WHERE id=?", (archived["recording_id"],))
                db.execute("INSERT INTO workflow (recording_id,transcript) VALUES (?,?)",
                           (archived["recording_id"], json.dumps({"text": "Native archive text"})))
                store.record_event(db, archived["recording_id"], "archived", actor="test")
                db.execute("INSERT INTO calls (id,owner) VALUES ('waiting','provider')")
                db.execute("""INSERT INTO outbound_calls (owner,source,external_id,call_id,metadata,complete)
                              VALUES ('provider','demo','wait','waiting',?,0)""",
                           (json.dumps({"callee": "123456789", "started_at": "2026-11-24T08:00:00+08:00"}),))

            with TestClient(app) as browser:
                self.assertEqual(browser.get("/api/admin/calls").status_code, 401)
                page = browser.get("/")
                browser.post("/login", data={"csrf": hidden_field(page.text, "csrf"), "credential": "admin-token"})
                page = browser.get("/calls/pending")
                self.assertEqual(page.status_code, 200)
                import re
                csrf = re.search(r'const csrf = "([^"]+)"', page.text)[1]
                headers = {"X-CallArchive-CSRF": csrf}
                listing = browser.get("/api/admin/calls", params={"date": "2026-11-24", "owner": "Agent"}).json()
                self.assertEqual({row["talk_id"] for row in listing["calls"]}, {first["call_id"], other["call_id"]})
                item = next(row for row in listing["calls"] if row["talk_id"] == first["call_id"])
                self.assertEqual(item["recording_seconds"], 10)
                self.assertTrue(item["can_retry"])
                detail_url = f"/api/admin/calls/{first['call_id']}"
                detail = browser.get(detail_url).json()
                self.assertEqual(len(detail["recordings"]), 2)
                stamp = "123456789_2026-11-24_01-00-00"
                self.assertEqual([recording["download_name"] for recording in detail["recordings"]],
                                 [stamp + ".wav", stamp + "_2.wav"])
                audio_url = detail_url + "/recording"
                audio = browser.get(audio_url, params={"file_id": second["recording_id"]}, headers={"Range": "bytes=0-7"})
                self.assertEqual((audio.status_code, len(audio.content)), (206, 8))
                self.assertIn(stamp + "_2.wav", audio.headers["content-disposition"])
                self.assertEqual(browser.get(audio_url, params={"file_id": other["recording_id"]}).status_code, 404)
                retry_url = f"/api/calls/{first['call_id']}/retry"
                self.assertEqual(browser.post(retry_url).status_code, 403)
                self.assertEqual(browser.post(retry_url, headers=headers).status_code, 200)
                first_retry = browser.post("/api/calls/waiting/retry", headers=headers)
                self.assertEqual(first_retry.status_code, 200)
                self.assertEqual(first_retry.json()["status"], "scheduled")
                duplicate = browser.post("/api/calls/waiting/retry", headers=headers)
                self.assertEqual(duplicate.status_code, 200)
                self.assertEqual(duplicate.json()["status"], "queued")
                self.assertEqual(resumed, [first["call_id"], "waiting"])
                result = browser.post("/api/calls/remove", json={"talk_ids": [first["call_id"], "waiting"]},
                                      headers=headers).json()
                self.assertEqual(result["removed"], [first["call_id"], "waiting"])
                self.assertIsNone(store.get(None, second["recording_id"]))
                self.assertIsNone(store.get_audio(None, second["recording_id"]))
                self.assertEqual(browser.get(detail_url).status_code, 404)

                self.assertEqual(browser.get("/calls/recent").status_code, 200)
                recent_url = "/api/archives/recent"
                self.assertEqual(browser.get(recent_url).json()["calls"][0]["talk_id"], archived["call_id"])
                attachment = f"{recent_url}/{archived['call_id']}/attachment"
                recent_audio = browser.get(attachment, params={"file_id": archived["recording_id"], "kind": "audio"})
                self.assertEqual(recent_audio.status_code, 200)
                self.assertIn(stamp + ".wav", recent_audio.headers["content-disposition"])
                params = {"file_id": archived["recording_id"], "kind": "text", "inline": "true"}
                self.assertEqual(browser.get(attachment, params=params).text, "Native archive text")
                with store.transaction() as db:
                    db.execute("UPDATE recording_events SET occurred_at=? WHERE recording_id=? AND status='archived'",
                               (time.time() - ARCHIVE_CACHE_SECONDS - 1, archived["recording_id"]))
                self.assertEqual(browser.get(recent_url).json()["calls"], [])
                self.assertEqual(browser.get(attachment, params=params).status_code, 404)
                self.assertEqual(store.purge_archive_cache(), 1)
                self.assertEqual(store.get(None, archived["recording_id"])["status"], "archived")
                self.assertIsNone(store.get_audio(None, archived["recording_id"]))

            with TestClient(app) as browser:
                page = browser.get("/")
                browser.post("/login", data={"csrf": hidden_field(page.text, "csrf"), "credential": "external-token"})
                self.assertEqual(browser.get("/calls/pending").status_code, 403)
                self.assertEqual(browser.get("/api/archives/recent").status_code, 403)
