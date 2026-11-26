
import json
import tempfile
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from callarchive.backend.console_calls import ConsoleCalls
from callarchive.backend.reporting import ReportDelivery, collect_issues
from callarchive.backend.store import ArchiveStore
from callarchive.notifications import Delivery, Receipt
from callarchive.notifications.feishu import build_card
from test_service import sample_audio


class Sender:
    supports_idempotency = False

    def __init__(self, results):
        self.results = iter(results)
        self.calls = []

    def send(self, message):
        self.calls.append(message)
        return Receipt(next(self.results))


class ReportTest(unittest.TestCase):
    def test_daily_report_uses_one_day_of_archive_results_even_after_a_failed_report(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            delivery = ReportDelivery(store, zone=timezone.utc)
            start = date(2026, 11, 24)
            cutoff = datetime(2026, 11, 24, 9, 43, tzinfo=timezone.utc).timestamp()
            delivery.deliver(start, "owner", "test", "operator", Sender([Delivery.CONFIRMED]),
                             archive_window=(cutoff - 86400, cutoff), fresh_only=True)
            for identifier, closed_at in (("first", cutoff + 10), ("second", cutoff + 86400 + 10)):
                item = store.submit("owner", identifier, {"flow": "remote", "call": {}, "options": {}},
                                    sample_audio(), "audio/wav")
                with store.transaction() as db:
                    db.execute("""INSERT INTO outbound_calls (owner,source,external_id,call_id,metadata,complete)
                                  VALUES ('owner','source',?,?,'{}',1)""", (identifier, item["call_id"]))
                    db.execute("UPDATE recordings SET status='archived' WHERE id=?", (item["recording_id"],))
                    store.record_event(db, item["recording_id"], "archived", actor="test", now=closed_at)
            missed = Sender([Delivery.REJECTED])
            delivery.deliver(start + timedelta(days=1), "owner", "test", "operator", missed,
                             archive_window=(cutoff, cutoff + 86400), fresh_only=True)
            self.assertEqual(missed.calls[0].content["archived_calls"], 1)
            duplicate = Sender([Delivery.CONFIRMED])
            duplicate.retryable_rejections = {None}
            delivery.deliver(start + timedelta(days=1), "owner", "test", "operator", duplicate,
                             archive_window=(cutoff, cutoff + 86400), fresh_only=True)
            self.assertEqual(duplicate.calls, [])
            delivered = Sender([Delivery.CONFIRMED])
            delivery.deliver(start + timedelta(days=2), "owner", "test", "operator", delivered,
                             archive_window=(cutoff + 86400, cutoff + 172800), fresh_only=True)
            self.assertEqual(delivered.calls[0].content["archived_calls"], 1)

    def test_archive_details_expire_with_or_without_notification(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            item = store.submit("owner", "archived", {"flow": "remote", "call": {}, "options": {}},
                                sample_audio(), "audio/wav")
            call_id = item["call_id"]
            second = store.submit("owner", "archived-segment-2", {"flow": "remote", "call": {}, "options": {}},
                                  sample_audio(), "audio/wav", linked_call=call_id)
            now = time.time()
            started_at = now - 5 * 86400
            metadata = {"started_at": datetime.fromtimestamp(started_at, timezone.utc).isoformat()}
            with store.transaction() as db:
                db.execute("INSERT INTO outbound_calls (owner,source,external_id,call_id,metadata,complete) VALUES ('owner','source','external',?,?,1)", (call_id, json.dumps(metadata)))
                db.execute("INSERT INTO outbound_recordings (call_id,external_id,reference,metadata,recording_id,error) VALUES (?,'audio','ref','{}',?,NULL)",
                           (call_id, item["recording_id"]))
                db.execute("INSERT INTO outbound_recordings (call_id,external_id,reference,metadata,recording_id,error) VALUES (?,'audio-2','ref','{}',?,NULL)",
                           (call_id, second["recording_id"]))
                db.execute("UPDATE recordings SET status='archived' WHERE call_id=?", (call_id,))
                for recording_id in (item["recording_id"], second["recording_id"]):
                    store.record_event(db, recording_id, "archived", actor="test")
                    db.execute("""INSERT INTO remote_attachments
                        (recording_id,target,kind,request_id,status,receipt) VALUES (?,'target','audio',?,'confirmed','ok')""",
                               (recording_id, recording_id))
                db.execute("UPDATE recording_events SET occurred_at=? WHERE status='archived'", (now - 1,))
            with store.transaction() as db:
                db.execute("CREATE TABLE archive_completions(call_id TEXT,owner TEXT,closed_at REAL)")
                db.execute("INSERT INTO archive_completions VALUES ('legacy-only','owner',?)", (now - 1,))
            store = ArchiveStore(store.database)
            with store.transaction() as db:
                self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='archive_completions'").fetchone())
            reports = ReportDelivery(store)
            day = date(2026, 11, 24)
            failed = Sender([Delivery.REJECTED])
            window = (now - 86400, now)
            reports.deliver(day, "owner", "test", "operator", failed, archive_window=window)
            self.assertEqual(failed.calls[0].content["issues"], [])
            self.assertEqual(failed.calls[0].content["archived_calls"], 1)
            summary = build_card(failed.calls[0])["body"]["elements"][0]
            self.assertEqual(summary["columns"][2]["elements"][0]["content"], "**1**")
            self.assertEqual(summary["columns"][2]["elements"][1]["content"], "已归档")
            store.purge_archive_cache(now + 86398)
            self.assertIsNotNone(store.get_audio("owner", item["recording_id"]))
            self.assertIsNotNone(store.get("owner", item["recording_id"]))
            store.purge_archive_cache(now + 86401)
            self.assertIsNone(store.get_audio("owner", item["recording_id"]))
            self.assertIsNone(store.get("owner", item["recording_id"]))
            with store.transaction() as db:
                row = db.execute("SELECT 1 FROM outbound_import_exclusions WHERE identifier='external'").fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(db.execute("SELECT count(*) FROM remote_attachments").fetchone()[0], 0)
            late = Sender([Delivery.CONFIRMED])
            reports.deliver(day, "owner", "another-channel", "operator", late, archive_window=window)
            self.assertEqual(late.calls[0].content["archived_calls"], 0)
            next_day = Sender([Delivery.CONFIRMED])
            reports.deliver(date(2026, 11, 25), "owner", "test", "operator", next_day,
                            archive_window=(now, now + 86400))
            self.assertEqual(next_day.calls[0].content["archived_calls"], 0)
            store.purge_archive_cache(now + 2 * 86400 + 1)
            with store.transaction() as db:
                self.assertEqual(db.execute("SELECT count(*) FROM outbound_import_exclusions").fetchone()[0], 0)
                self.assertAlmostEqual(ArchiveStore.outbound_scan_cutoff(db, now, 30 * 86400), started_at, places=5)


    def test_replay_only_failed_page_and_preserve_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            for index in range(3):
                store.submit("owner", f"request-{index}", {"flow": "local", "call": None, "options": {}},
                             sample_audio(), "audio/wav")
                item = store.claim(now=100)
                store.advance(item, "needs_review", error="attachment_write_failed")

            run = ReportDelivery(store, page_size=1)
            day = date(2026, 11, 24)
            sender = Sender([Delivery.CONFIRMED, Delivery.REJECTED, Delivery.UNKNOWN])
            self.assertEqual(run.deliver(day, "owner", "test", "operator", sender),
                             {"confirmed": 1, "rejected": 1, "unknown": 1})
            with store.transaction() as db:
                db.execute("UPDATE recordings SET status='archived'")
            retry = Sender([Delivery.CONFIRMED])
            result = ReportDelivery(store, page_size=100).deliver(
                day, "owner", "test", "operator", retry, replay_rejected=True,
            )
            self.assertEqual(result, {"confirmed": 2, "unknown": 1})
            self.assertEqual(retry.calls, [sender.calls[1]])
            fresh = Sender([Delivery.CONFIRMED])
            run.deliver(date(2026, 11, 25), "owner", "test", "operator", fresh)
            self.assertEqual(fresh.calls[0].content["issues"], [])



    def test_daily_splits_oversized_card_with_totals_and_continuation(self):
        class LimitedSender(Sender):
            def fits(self, message):
                return len(message.content["issues"]) <= 1

        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            for number in ("10001", "10002"):
                store.submit("owner", number, {"flow": "local", "call": {"callee": number}, "options": {}},
                             sample_audio(), "audio/wav")
                store.advance(store.claim(now=100), "needs_input", error="archive_target_missing")
            with store.transaction() as db:
                db.execute("INSERT INTO calls (id,owner) VALUES ('waiting','owner')")
                db.execute("INSERT INTO outbound_calls (owner,source,external_id,call_id,metadata,complete) VALUES ('owner','source','external','waiting','{}',0)")
            sender = LimitedSender([Delivery.CONFIRMED, Delivery.CONFIRMED])
            delivery = ReportDelivery(store, workbench_url="https://archive.example", page_size=20)
            result = delivery.deliver(date(2026, 11, 24), "owner", "test", "operator", sender,
                                      archive_window=(0, 1000))
            self.assertEqual(result, {"confirmed": 2})
            first, second = map(build_card, sender.calls)
            self.assertIn("（1/2）", first["header"]["title"]["content"])
            self.assertIn("（2/2）", second["header"]["title"]["content"])
            top, divider, panel = first["body"]["elements"]
            self.assertEqual(top["columns"][0]["elements"][0]["content"], "**2**")
            self.assertEqual(divider["tag"], "hr")
            self.assertFalse(any(element["tag"] == "column_set" for element in panel["elements"]))
            continuation, note = second["body"]["elements"]
            self.assertIn("（续）", continuation["header"]["title"]["content"])
            buttons = [element for element in continuation["elements"] if element["tag"] == "column_set"]
            self.assertEqual(len(buttons), 1)
            button = buttons[0]["columns"][0]["elements"][0]
            self.assertEqual(button["text"]["content"], "详情")
            self.assertIn("等待录音上传 1 通", note["content"])

    def test_single_alert_uses_recording_transition_and_skips_resolved_items(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            call = {"callee": "+8610002", "caller": "10001", "started_at": "2026-11-24T00:06:00Z"}
            store.submit("owner", "alert", {"flow": "local", "call": call, "options": {}},
                                sample_audio(), "audio/wav")
            store.advance(store.claim(now=100), "needs_review", error="attachment_write_failed")
            reports = ReportDelivery(store, workbench_url="https://archive.example/workbench",
                                     followup_names={"10001": "跟进人甲"}, zone=timezone(timedelta(hours=8)))
            sender = Sender([Delivery.CONFIRMED])
            self.assertEqual(reports.deliver_alerts("owner", "test", "operator", sender), {"confirmed": 1})
            self.assertEqual(sender.calls[0].content["kind"], "alert")
            self.assertEqual(sender.calls[0].content["issue"]["started_at"], "2026-11-24T08:06:00+08:00")
            url = urlsplit(sender.calls[0].content["details_url"])
            self.assertEqual(url.path, "/calls/pending")
            self.assertEqual(parse_qs(url.query), {"mobile": ["+8610002"], "owner": ["跟进人甲"]})
            self.assertEqual(reports.deliver_alerts("owner", "test", "operator", sender), {"confirmed": 1})
            self.assertEqual(len(sender.calls), 1)
            with store.transaction() as db:
                db.execute("UPDATE recordings SET status='archived'")
            self.assertEqual(reports.deliver_alerts("owner", "test", "operator", sender), {})

    def test_daily_uses_full_owner_filter_and_groups_linked_numbers(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            name = "\u5218 + \u7ec4"
            names = {"13000000001": name, "008613000000002": name}
            for index, caller in enumerate(("+86 13000000001", "13000000002", "333", "13000000002")):
                call = {"caller": caller, "callee": f"1000{index}"}
                store.submit("owner", f"daily-{index}", {"flow": "local", "call": call, "options": {}},
                             sample_audio(), "audio/wav")
                status = "needs_input" if index < 3 else "transcribing"
                store.advance(store.claim(now=100), status, error="archive_target_missing" if index < 3 else None)
            reports = ReportDelivery(store, workbench_url="https://archive.example/workbench?discard=1",
                                     followup_names=names)
            sender = Sender([Delivery.CONFIRMED])
            reports.deliver(date(2026, 11, 24), "owner", "test", "operator", sender)
            card = build_card(sender.calls[0])
            groups = [element for element in card["body"]["elements"] if element["tag"] == "collapsible_panel"]
            self.assertEqual(len(groups), 2)
            buttons = [group["elements"][-1]["columns"][0]["elements"][0] for group in groups]
            urls = [urlsplit(button["behaviors"][0]["default_url"]) for button in buttons]
            self.assertEqual([url.path for url in urls], ["/calls/pending", "/calls/pending"])
            self.assertEqual([parse_qs(url.query) for url in urls], [{"owner": ["333"]}, {"owner": [name]}])
            self.assertIn("333", groups[0]["header"]["title"]["content"])
            self.assertIn(name, groups[1]["header"]["title"]["content"])
            console = ConsoleCalls(store, names)
            for owner in (name, "13000000001", "0086 13000000002"):
                matches = console.pending(owner=owner)["calls"]
                self.assertEqual({row["mobile"] for row in matches}, {"10000", "10001", "10003"})

    def test_daily_groups_all_segments_by_call_and_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            call = {"callee": "10002", "started_at": "2026-11-24T00:06:00Z"}
            metadata = {"flow": "local", "call": call, "options": {}}
            linked = None
            for index, error in enumerate(("attachment_write_failed", "attachment_conflict", "attachment_conflict")):
                item = store.submit("owner", f"segment-{index}", metadata, sample_audio(), "audio/wav",
                                    linked_call=linked)
                linked = item["call_id"]
                store.advance(store.claim(now=100), "needs_review", error=error)
                with store.transaction() as db:
                    db.execute("INSERT OR REPLACE INTO recording_media VALUES (?,60000)", (item["recording_id"],))
            reports = ReportDelivery(store)
            sender = Sender([Delivery.CONFIRMED])
            reports.deliver(date(2026, 11, 24), "owner", "test", "operator", sender)
            content = sender.calls[0].content
            self.assertEqual(content["admin_count"], 1)
            issue, = content["issues"]
            self.assertEqual(issue["reasons"], ["attachment_conflict", "attachment_write_failed"])
            self.assertEqual(issue["recording_duration_ms"], 180000)
            panel = build_card(sender.calls[0])["body"]["elements"][0]
            self.assertIn("已有附件与本次内容不一致、附件写入失败 · 1 通", panel["elements"][0]["content"])
            with store.transaction() as db:
                db.execute("DELETE FROM recording_media WHERE recording_id=?", (item["recording_id"],))
            sender = Sender([Delivery.CONFIRMED])
            reports.deliver(date(2026, 11, 25), "owner", "test", "operator", sender)
            self.assertIsNone(sender.calls[0].content["issues"][0]["recording_duration_ms"])

    def test_waiting_alert_is_once_per_callee_cycle_across_calls_and_manual_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            call = {"callee": "10002"}
            metadata = {"flow": "remote", "archive_instance": "main", "call": call, "options": {}}
            reports = ReportDelivery(store)
            sender = Sender([Delivery.CONFIRMED, Delivery.CONFIRMED])

            def waiting(key):
                item = store.submit("owner", key, metadata, sample_audio(), "audio/wav")
                store.advance(store.claim(now=100), "needs_input", retry_at=3600, error="archive_target_missing")
                return item["recording_id"]

            first = waiting("first")
            reports.deliver_alerts("owner", "test", "operator", sender)
            store.request_retry("owner", first, actor="operator")
            second = waiting("second")
            reports.deliver_alerts("owner", "test", "operator", sender)
            self.assertEqual(len(sender.calls), 1)

            def resolve(recording_id):
                with store.transaction() as db:
                    db.execute("UPDATE recordings SET status='archived' WHERE id=?", (recording_id,))
                    store.record_event(db, recording_id, "archived", actor="test")

            resolve(first)
            reports.deliver_alerts("owner", "test", "operator", sender)
            self.assertEqual(len(sender.calls), 1)
            resolve(second)
            waiting("third")
            reports.deliver_alerts("owner", "test", "operator", sender)
            self.assertEqual(len(sender.calls), 2)

    def test_waiting_alert_survives_callee_correction_and_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            item = store.submit("owner", "corrected-call", {"flow": "local", "options": {},
                                "call": {"callee": "10001"}}, sample_audio(), "audio/wav")
            call_id = item["call_id"]
            store.advance(store.claim(now=100), "needs_input", error="archive_target_missing")
            with store.transaction() as db:
                db.execute("""INSERT INTO outbound_calls(owner,source,external_id,call_id,metadata,complete)
                              VALUES ('owner','source','call',?,'{"callee":"10001"}',1)""", (call_id,))

            sender = Sender([Delivery.UNKNOWN, Delivery.CONFIRMED, Delivery.CONFIRMED])
            ReportDelivery(store).deliver_alerts("owner", "test", "operator", sender)
            with store.transaction() as db:
                db.execute("UPDATE outbound_calls SET metadata=? WHERE call_id=?",
                           (json.dumps({"callee": "10002"}), call_id))
            ReportDelivery(store).deliver_alerts("owner", "test", "operator", sender)
            self.assertEqual([message.content["issue"]["callee"] for message in sender.calls], ["10001"])

            with store.transaction() as db:
                recording_id = item["recording_id"]
                store.record_event(db, recording_id, "queued", actor="worker")
                store.record_event(db, recording_id, "needs_input", actor="worker", error="archive_target_missing")
            ReportDelivery(store).deliver_alerts("owner", "test", "operator", sender)
            self.assertEqual([message.content["issue"]["callee"] for message in sender.calls], ["10001", "10002"])

    def test_report_recovery_only_retries_same_day_explicit_limits(self):
        class RateLimitedSender(Sender):
            retryable_rejections = {"http_429", "11232"}

            def send(self, message):
                self.calls.append(message)
                return next(self.results)

        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            for index in range(4):
                store.submit("owner", f"blocked-{index}", {"flow": "local", "call": {}, "options": {}},
                             sample_audio(), "audio/wav")
                store.advance(store.claim(now=100), "needs_review", error="attachment_write_failed")
            day = date(2026, 11, 24)
            reports = ReportDelivery(store, page_size=1)
            sender = RateLimitedSender([Receipt(Delivery.CONFIRMED), Receipt(Delivery.UNKNOWN),
                                        Receipt(Delivery.REJECTED, "http_400"),
                                        Receipt(Delivery.REJECTED, "11232")])
            reports.deliver(day, "owner", "test", "operator", sender)
            retry = RateLimitedSender([Receipt(Delivery.CONFIRMED)])
            self.assertEqual(reports.retry_report(day + timedelta(days=1), "owner", "test", "operator", retry), {})
            self.assertEqual(reports.retry_report(day, "owner", "test", "operator", retry), {"confirmed": 1})
            self.assertEqual(retry.calls, [sender.calls[-1]])
            self.assertEqual(reports.retry_report(day, "owner", "test", "operator", retry), {})


    def test_only_sustained_transcription_failure_appears_in_alerts_and_daily(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            store.submit("owner", "asr", {"flow": "local", "call": {"callee": "10001"}, "options": {}},
                         sample_audio(), "audio/wav")
            reports = ReportDelivery(store)
            alerts = Sender([Delivery.CONFIRMED])
            for attempt in range(1, 5):
                state = "needs_review" if attempt == 4 else "queued"
                store.advance(store.claim(now=100), state, error="transcription_failed")
                reports.deliver_alerts("owner", "test", "operator", alerts)
                daily = Sender([Delivery.CONFIRMED])
                reports.deliver(date(2026, 11, 24) + timedelta(days=attempt), "owner", "test", "operator", daily)
                self.assertEqual(len(alerts.calls), int(attempt == 4))
                self.assertEqual(daily.calls[0].content["admin_count"], int(attempt == 4))
            self.assertEqual(alerts.calls[0].content["issue"]["reason"], "transcription_failed")


    def test_upgrade_reuses_settled_alert_from_same_waiting_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            item = store.submit("owner", "legacy-wait",
                                {"flow": "local", "call": {"callee": "10002"}, "options": {}},
                                sample_audio(), "audio/wav")
            store.advance(store.claim(now=100), "needs_input", error="archive_target_missing")
            reports = ReportDelivery(store)
            with store.transaction() as db:
                revision = db.execute("SELECT max(sequence) FROM recording_events").fetchone()[0]
                key = json.dumps(["alert", "owner", "test", "operator", item["recording_id"], revision])
                body = json.dumps({"content": {"issue": {"category": "follow_up", "reason": "archive_target_missing"}}})
                db.execute("INSERT INTO report_messages (id,report_key,page,body,status) VALUES (?,?,1,?,'unknown')",
                           ("legacy", key, body))
            sender = Sender([])
            self.assertEqual(reports.deliver_alerts("owner", "test", "operator", sender), {"unknown": 1})
            self.assertEqual(sender.calls, [])


    def test_late_corrupt_segment_suppresses_follow_up_on_all_surfaces(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            console = ConsoleCalls(store, {})
            item = store.submit("owner", "first", {"flow": "local", "options": {},
                                "call": {"callee": "10001"}}, sample_audio(), "audio/wav")
            call_id = item["call_id"]
            store.advance(store.claim(now=100), "needs_input", error="archive_target_missing")
            with store.transaction() as db:
                db.execute("""INSERT INTO outbound_calls(owner,source,external_id,call_id,metadata,complete)
                              VALUES ('owner','source','call',?,'{"callee":"10002"}',0)""", (call_id,))
                waiting, issues = collect_issues(db, "owner")
            self.assertEqual(waiting, 1)
            self.assertEqual([(i["category"], i["callee"]) for i in issues], [("follow_up", "10002")])
            self.assertEqual(console.pending_detail(call_id)["status"], "waiting_applicant")

            with store.transaction() as db:
                db.execute("""INSERT INTO outbound_recordings(call_id,external_id,reference,metadata,error)
                              VALUES (?,'second','ref','{}','invalid_audio')""", (call_id,))
                _, issues = collect_issues(db, "owner")
            self.assertEqual([(i["category"], i["reason"]) for i in issues], [("admin", "source_recording_invalid")])
            detail = console.pending_detail(call_id)
            self.assertEqual(detail["status"], "recording_invalid")
            self.assertTrue(detail["can_retry"])
            self.assertTrue(console.retry(call_id, "operator"))
            with store.transaction() as db:
                self.assertIsNone(db.execute("SELECT error FROM outbound_recordings").fetchone()[0])
                self.assertIsNotNone(db.execute("SELECT 1 FROM outbound_console_retries").fetchone())

    def test_source_download_failures_use_four_attempts_and_group_by_call(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            with store.transaction() as db:
                db.execute("INSERT INTO calls (id,owner) VALUES ('call','owner')")
                db.execute("""INSERT INTO outbound_calls (owner,source,external_id,call_id,metadata,complete)
                              VALUES ('owner','source','external','call','{}',1)""")
                db.executemany(
                    """INSERT INTO outbound_recordings (call_id,external_id,reference,metadata,error,download_attempts)
                       VALUES ('call',?,'ref','{"callee":"10002"}',?,?)""",
                    [("failed-1", "download_failed", 3), ("failed-2", "download_failed", 4),
                     ("invalid", "invalid_audio", 1)],
                )
            reports = ReportDelivery(store)
            sender = Sender([Delivery.CONFIRMED])
            reports.deliver(date(2026, 11, 24), "owner", "test", "operator", sender)
            issue, = sender.calls[0].content["issues"]
            self.assertEqual(issue["reasons"], ["source_recording_invalid", "source_recording_unavailable"])
            with store.transaction() as db:
                db.execute("DELETE FROM outbound_recordings WHERE external_id IN ('failed-2','invalid')")
            alerts = Sender([])
            self.assertEqual(reports.deliver_alerts("owner", "test", "operator", alerts), {})

    def test_system_transcription_alert_is_distinct_from_earlier_admin_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            item = store.submit("owner", "asr-family", {"flow": "local", "call": {}, "options": {}},
                                sample_audio(), "audio/wav")
            store.advance(store.claim(now=100), "needs_review", error="attachment_write_failed")
            reports = ReportDelivery(store)
            sender = Sender([Delivery.CONFIRMED, Delivery.CONFIRMED])
            reports.deliver_alerts("owner", "test", "operator", sender)
            store.request_retry("owner", item["recording_id"], actor="operator")
            for attempt in range(1, 5):
                state = "needs_review" if attempt == 4 else "queued"
                store.advance(store.claim(now=100), state, error="transcription_failed")
                reports.deliver_alerts("owner", "test", "operator", sender)
            self.assertEqual([message.content["issue"]["reason"] for message in sender.calls],
                             ["attachment_write_failed", "transcription_failed"])
