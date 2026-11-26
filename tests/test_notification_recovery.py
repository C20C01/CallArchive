
from datetime import date
from io import BytesIO, StringIO
from pathlib import Path
import json
import tempfile
import unittest
import wave

import httpx

from callarchive.backend.reporting import ReportDelivery
from callarchive.backend.store import ArchiveStore
from callarchive.configuration import LoggingSettings
from callarchive.notifications.feishu import FeishuChannel
from callarchive.observability import configure_logging, LOG


DAY = date(2026, 11, 25)
WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/synthetic"


def fixture_audio():
    buffer = BytesIO()
    with wave.open(buffer, "wb") as stream:
        stream.setnchannels(1)
        stream.setsampwidth(2)
        stream.setframerate(8000)
        stream.writeframes(bytes(16000))
    return buffer.getvalue()


class NotificationRecoveryTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.store = ArchiveStore(self.root / "archive.sqlite")
        self.reports = ReportDelivery(self.store, page_size=1)

    def channel(self, responses):
        responses = iter(responses)
        http = httpx.Client(transport=httpx.MockTransport(lambda _: next(responses)))
        self.addCleanup(http.close)
        return FeishuChannel(http, WEBHOOK)

    def add_issue(self, number, *, status="needs_review", reason="attachment_write_failed"):
        item = self.store.submit(
            "owner", number,
            {"flow": "local", "call": {"caller": "10001", "callee": number}, "options": {}},
            fixture_audio(), "audio/wav",
        )
        self.store.advance(self.store.claim(now=100), status, error=reason)
        return item

    def test_manual_report_replay_sends_only_definitely_rejected_pages(self):
        for number in ("20001", "20002", "20003"):
            self.add_issue(number)
        channel = self.channel([httpx.Response(200, json={"code": code}) for code in (19021, 0, 90000, 0)])
        self.assertEqual(self.reports.deliver(DAY, "owner", "feishu", "ops", channel),
                         {"confirmed": 1, "rejected": 1, "unknown": 1})
        self.assertEqual(self.reports.retry_report(DAY, "owner", "feishu", "ops", channel), {})
        self.assertEqual(self.reports.deliver(DAY, "owner", "feishu", "ops", channel, replay_rejected=True),
                         {"confirmed": 2, "unknown": 1})

    def test_manual_alert_replay_does_not_send_a_new_alert(self):
        self.add_issue("20001")
        channel = self.channel([httpx.Response(200, json={"code": code}) for code in (19021, 0)])
        self.assertEqual(self.reports.deliver_alerts("owner", "feishu", "ops", channel), {"rejected": 1})
        self.add_issue("20002")
        self.assertEqual(self.reports.deliver_alerts("owner", "feishu", "ops", channel, replay_rejected=True),
                         {"confirmed": 1})
        with self.store.transaction() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM report_messages").fetchone()[0], 1)

    def test_refusal_reason_is_safe_and_readable_in_runtime_log(self):
        saved_handlers, saved_level = LOG.handlers[:], LOG.level
        self.addCleanup(LOG.setLevel, saved_level)
        self.addCleanup(setattr, LOG, "handlers", saved_handlers)
        stream = StringIO()
        log = configure_logging(LoggingSettings(database=self.root / "events.sqlite"), "notification", stream=stream)
        channel = self.channel([httpx.Response(200, json={"code": 19021, "msg": "private upstream text"})])
        self.reports.deliver(DAY, "owner", "feishu", "ops", channel)
        self.assertEqual(log.list()["items"][0]["reason"], "feishu_19021")
        self.assertNotIn("private upstream text", stream.getvalue())

    def test_removed_call_is_redacted_without_resending_or_forgetting_delivery(self):
        item = self.add_issue("20001")
        channel = self.channel([httpx.Response(200, json={"code": 0}) for _ in range(2)])
        self.reports.deliver(DAY, "owner", "feishu", "ops", channel)
        self.reports.deliver_alerts("owner", "feishu", "ops", channel)
        with self.store.transaction() as db:
            self.store.retire_call(db, item["call_id"])
        self.assertEqual(self.reports.deliver(DAY, "owner", "feishu", "ops", channel), {"confirmed": 1})
        self.reports.deliver_alerts("owner", "feishu", "ops", channel)
        with self.store.transaction() as db:
            rows = db.execute("SELECT status, body FROM report_messages").fetchall()
        self.assertEqual([row["status"] for row in rows], ["confirmed", "confirmed"])
        for row in rows:
            self.assertNotIn("10001", row["body"])
            self.assertNotIn("20001", row["body"])


    def test_aged_settled_body_is_redacted_without_affecting_an_active_failure(self):
        self.add_issue("20001")
        self.add_issue("20002")
        channel = self.channel([httpx.Response(200, json={"code": code}) for code in (0, 19021, 0)])
        self.reports.deliver(DAY, "owner", "feishu", "ops", channel)
        with self.store.transaction() as db:
            db.execute("UPDATE report_messages SET created_at = 1")
        result = self.reports.deliver(DAY, "owner", "feishu", "ops", channel, replay_rejected=True)
        self.assertEqual(result, {"confirmed": 2})
        with self.store.transaction() as db:
            rows = db.execute("SELECT body FROM report_messages ORDER BY page").fetchall()
        self.assertNotIn("10001", rows[0]["body"])

    def test_archive_maintenance_redacts_without_notification_delivery(self):
        from callarchive.backend.__main__ import maintain_archive

        self.add_issue("20001")
        channel = self.channel([httpx.Response(200, json={"code": 0})])
        self.reports.deliver(DAY, "owner", "feishu", "ops", channel)
        with self.store.transaction() as db:
            db.execute("UPDATE report_messages SET created_at=1")

        maintain_archive(self.store, self.reports, now=86401)
        with self.store.transaction() as db:
            row = db.execute("SELECT status,body FROM report_messages").fetchone()
        self.assertEqual(row["status"], "confirmed")
        self.assertEqual(json.loads(row["body"]), {})

    def test_cleanup_preserves_an_active_send_lease(self):
        item = self.add_issue("20001")
        channel = self.channel([httpx.Response(200, json={"code": 19021})])
        self.reports.deliver(DAY, "owner", "feishu", "ops", channel)
        with self.store.transaction() as db:
            db.execute("UPDATE report_messages SET status='sending', attempt='owned', expires=strftime('%s','now')+120")
            self.store.retire_call(db, item["call_id"])
        self.reports.deliver(DAY, "owner", "feishu", "ops", channel, replay_rejected=True)
        with self.store.transaction() as db:
            row = db.execute("SELECT status, attempt, body FROM report_messages").fetchone()
        self.assertEqual((row["status"], row["attempt"]), ("sending", "owned"))
        self.assertIn("20001", row["body"])


    def test_waiting_alert_key_upgrade_removes_the_number_and_keeps_deduplication(self):
        self.add_issue("20001", status="needs_input", reason="archive_target_missing")
        channel = self.channel([httpx.Response(200, json={"code": 0})])
        self.assertEqual(self.reports.deliver_alerts("owner", "feishu", "ops", channel), {"confirmed": 1})
        with self.store.transaction() as db:
            original = db.execute("SELECT id FROM report_messages").fetchone()["id"]
            cycle = db.execute("SELECT episode FROM report_wait_cycles").fetchone()["episode"]
            old_key = json.dumps(["alert-call", "owner", "feishu", "ops", "waiting", "", "20001", cycle])
            db.execute("UPDATE report_messages SET report_key=?", (old_key,))
        self.assertEqual(self.reports.deliver_alerts("owner", "feishu", "ops", channel), {"confirmed": 1})
        with self.store.transaction() as db:
            rows = db.execute("SELECT id, report_key FROM report_messages").fetchall()
        self.assertEqual([row["id"] for row in rows], [original])
        self.assertNotIn("20001", rows[0]["report_key"])

    def test_archived_details_are_redacted_after_the_call_cache_expires(self):
        item = self.add_issue("20001")
        channel = self.channel([httpx.Response(200, json={"code": 19021})])
        self.reports.deliver(DAY, "owner", "feishu", "ops", channel)
        with self.store.transaction() as db:
            db.execute("UPDATE recordings SET status='archived' WHERE id=?", (item["recording_id"],))
            self.store.record_event(db, item["recording_id"], "archived", actor="fixture")
            db.execute("UPDATE recording_events SET occurred_at=1 WHERE status='archived'")
        self.reports.deliver(DAY, "owner", "feishu", "ops", channel)
        with self.store.transaction() as db:
            row = db.execute("SELECT body FROM report_messages").fetchone()
        self.assertNotIn("20001", row["body"])
