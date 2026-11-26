import tempfile
import unittest
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

from callarchive.backend.scheduling import OutboundSchedule, report_day
from callarchive.backend.store import ArchiveStore


class SchedulingTest(unittest.TestCase):
    def test_keeps_the_in_flight_window_until_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "archive.sqlite"
            now = datetime(2026, 11, 24, 9, 43, tzinfo=timezone.utc)
            schedule = OutboundSchedule(ArchiveStore(db), "operator", "source", interval=300, lookback=900)
            first = schedule.window(now)
            self.assertEqual(first.end.minute, 40)
            self.assertEqual(first.end - first.start, timedelta(minutes=15))
            again = OutboundSchedule(ArchiveStore(db), "operator", "source", interval=300, lookback=900)
            self.assertEqual(again.window(now + timedelta(minutes=10)), first)
            again.finish(first)
            self.assertIsNone(again.window(now))
            next_window = again.window(now + timedelta(minutes=10))
            self.assertEqual(next_window.start, first.end)
            self.assertEqual(next_window.end, first.end + timedelta(minutes=10))


    def test_overlapping_scan_covers_late_calls_and_preserves_long_gap_catchup(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            now = datetime(2026, 11, 24, 9, 40, tzinfo=timezone.utc)
            schedule = OutboundSchedule(store, "owner", "source", interval=300, lookback=86400, overlap=10800)
            first = schedule.window(now)
            schedule.finish(first)
            second = schedule.window(now + timedelta(minutes=5))
            self.assertEqual(second.start, second.end - timedelta(hours=3))
            schedule.finish(second)
            after_outage = schedule.window(now + timedelta(hours=5))
            self.assertEqual(after_outage.start, second.end)

    def test_midnight_overlap_can_be_requested_without_losing_the_successful_watermark(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            zone = timezone(timedelta(hours=8))
            midnight = datetime(2026, 11, 25, tzinfo=zone)
            schedule = OutboundSchedule(store, "owner", "source", interval=300, lookback=86400, overlap=10800)
            first = schedule.window(midnight - timedelta(days=2))
            schedule.finish(first)
            catch_up = schedule.window(midnight, overlap=86400)
            self.assertEqual(catch_up.start, first.end)
            self.assertEqual(catch_up.end, midnight)
            schedule.finish(catch_up)
            second_midnight = midnight + timedelta(days=1)
            recent = schedule.window(second_midnight - timedelta(hours=1))
            schedule.finish(recent)
            backfill = schedule.window(second_midnight, overlap=86400)
            self.assertEqual(backfill.end - backfill.start, timedelta(days=1))

    def test_scheduled_import_clamps_an_unfinished_window_older_than_retention(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            now = datetime(2026, 11, 25, tzinfo=timezone.utc)
            schedule = OutboundSchedule(store, "owner", "source", interval=300, lookback=86400, overlap=10800)
            schedule.finish(schedule.window(now - timedelta(days=10)))
            schedule.window(now - timedelta(days=9))
            oldest = now - timedelta(days=7)
            current = schedule.window(now, oldest=oldest)
            self.assertEqual((current.start, current.end), (oldest, now))
            schedule.finish(current)
            self.assertIsNone(schedule.window(now, oldest=oldest))

    def test_daily_report_is_due_at_the_configured_local_time(self):
        local = timezone(timedelta(hours=8))
        now = datetime(2026, 11, 24, 1, 42, tzinfo=timezone.utc)
        self.assertIsNone(report_day(now, local, time(9, 43)))
        self.assertEqual(report_day(now + timedelta(minutes=1), local, time(9, 43)).isoformat(), "2026-11-24")


class RecurringScheduleTest(unittest.TestCase):
    def test_production_slots_follow_local_day_and_recovery_rules(self):
        from callarchive.backend.scheduling import recurring_slots

        zone = timezone(timedelta(hours=8))
        def slots(day, hours, minutes):
            now = datetime(2026, 11, day, hours, minutes, tzinfo=zone)
            return recurring_slots(now, zone, time(9, 43))

        self.assertEqual(slots(25, 7, 59)["outbound"].strftime("%H:%M"), "07:00")
        self.assertEqual(slots(25, 8, 7)["outbound"].strftime("%H:%M"), "08:05")
        self.assertEqual(slots(25, 21, 59)["outbound"].strftime("%H:%M"), "21:55")
        self.assertEqual(slots(25, 22, 52)["outbound"].strftime("%H:%M"), "22:00")
        self.assertNotIn("daily", slots(25, 9, 42))
        self.assertEqual(slots(25, 17, 1)["daily"].strftime("%d %H:%M"), "25 09:43")
        self.assertNotIn("daily", slots(26, 0, 1))
        self.assertNotIn("daily_retry", slots(25, 9, 44))
        self.assertEqual(slots(25, 9, 46)["daily_retry"].strftime("%H:%M"), "09:45")
        self.assertEqual(slots(25, 9, 46)["alerts"].strftime("%H:%M"), "09:46")

    def test_concurrent_claim_and_expired_executor_write_are_fenced(self):
        from callarchive.backend.scheduling import RecurringSchedule

        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            first = RecurringSchedule(store)
            second = RecurringSchedule(store)
            original = first.claim("outbound", "source", 100, now=105, lease_seconds=30)
            self.assertIsNotNone(original)
            self.assertIsNone(second.claim("outbound", "source", 110, now=120, lease_seconds=30))
            replacement = second.claim("outbound", "source", 110, now=136, lease_seconds=30)
            self.assertIsNotNone(replacement)
            self.assertFalse(first.finish(original, now=137))
            from callarchive.backend.scheduling import ScheduleLeaseLost
            guarded = ArchiveStore(store.database, write_guard=lambda db: first.require_lease(db, original, now=137))
            with self.assertRaises(ScheduleLeaseLost), guarded.transaction() as db:
                db.execute("INSERT INTO calls(id, owner) VALUES ('superseded','owner')")
            self.assertTrue(second.finish(replacement, now=138))
            self.assertIsNone(first.claim("outbound", "source", 110, now=180, lease_seconds=30))


class WorkerScheduleIntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def test_heartbeat_while_idle_does_not_run_scheduled_work(self):
        from unittest.mock import patch
        from callarchive.backend.periodic import BackendScheduler
        from callarchive.configuration import Settings

        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            scheduler = BackendScheduler(Settings(), store)
            now = 100
            delays = []

            async def sleep(delay):
                nonlocal now
                delays.append(delay)
                now += delay

            scheduler.record_heartbeat(now)
            with (
                patch("callarchive.backend.periodic.time.time", side_effect=lambda: now),
                patch("callarchive.backend.periodic.asyncio.sleep", side_effect=sleep),
                patch.object(scheduler, "tick", side_effect=AssertionError("idle heartbeat ran a task")),
            ):
                await scheduler.wait_until_due(230)
            self.assertEqual(delays, [60, 60, 10])
            with store.transaction() as db:
                self.assertEqual(db.execute(
                    "SELECT observed_at FROM scheduler_signals WHERE name='heartbeat'"
                ).fetchone()[0], 220)

    async def test_worker_sleeps_until_next_slot_and_respects_abandoned_lease(self):
        from unittest.mock import AsyncMock, patch
        from callarchive.backend.periodic import BackendScheduler
        from callarchive.configuration import Settings

        local = timezone(timedelta(hours=8))
        now = datetime(2026, 11, 25, 22, 1, tzinfo=local)
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            settings = Settings.model_validate({
                "outbound": {"owner": "operator", "source": "source"},
            })
            scheduler = BackendScheduler(settings, store)
            scope, _ = scheduler.tasks["outbound"]
            slot = int(now.replace(minute=0).timestamp())
            scheduler.schedule.claim("outbound", scope, slot, now=slot, lease_seconds=300)
            with (
                patch("callarchive.backend.periodic.datetime", wraps=datetime) as clock,
                patch("callarchive.backend.periodic.time.time", return_value=now.timestamp()),
                patch("callarchive.backend.periodic.HEARTBEAT_SECONDS", 7200),
                patch("callarchive.backend.periodic.asyncio.sleep", new_callable=AsyncMock) as sleep,
            ):
                clock.now.return_value = now
                sleep.side_effect = StopAsyncIteration
                with self.assertRaises(StopAsyncIteration):
                    await scheduler.run()
                sleep.assert_awaited_once_with(240)
                scheduler.schedule.finish(
                    scheduler.schedule.claim("outbound", scope, slot, now=slot + 300, lease_seconds=300),
                    now=slot + 301,
                )
                sleep.reset_mock()
                with self.assertRaises(StopAsyncIteration):
                    await scheduler.run()
                sleep.assert_awaited_once_with(59 * 60)

    async def test_worker_waits_for_completion_or_next_slot_whichever_is_first(self):
        import asyncio
        from unittest.mock import patch
        from callarchive.backend.periodic import BackendScheduler
        from callarchive.configuration import Settings

        local = timezone(timedelta(hours=8))
        now = datetime(2026, 11, 25, 9, 44, tzinfo=local)
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            settings = Settings.model_validate({"notifications": {"owner": "operator"}})
            scheduler = BackendScheduler(settings, store)
            task = asyncio.get_running_loop().create_future()
            scheduler.running["daily"] = task
            scheduler.running["alerts"] = task

            async def wait(tasks, *, timeout, return_when):
                self.assertEqual(set(tasks), {task})
                self.assertEqual(timeout, 60)
                self.assertEqual(return_when, asyncio.FIRST_COMPLETED)
                task.set_result(None)
                raise StopAsyncIteration

            with (
                patch("callarchive.backend.periodic.datetime", wraps=datetime) as clock,
                patch("callarchive.backend.periodic.time.time", return_value=now.timestamp()),
                patch.object(scheduler, "tick", return_value=None),
                patch("callarchive.backend.periodic.asyncio.wait", side_effect=wait) as waited,
                patch("callarchive.backend.periodic.asyncio.sleep", side_effect=AssertionError("polled instead of waiting")),
            ):
                clock.now.return_value = now
                with self.assertRaises(StopAsyncIteration):
                    await scheduler.run()
                waited.assert_awaited_once()


    async def test_new_manual_issue_dispatches_between_old_alert_slots_and_survives_restart(self):
        import asyncio
        from unittest.mock import patch
        from callarchive.backend.periodic import BackendScheduler
        from callarchive.configuration import Settings

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = Settings.model_validate({
                "backend": {"database": root / "archive.sqlite"},
                "notifications": {"owner": "operator", "output": root / "preview"},
            })
            store = ArchiveStore(settings.backend.database)
            now = datetime(2026, 11, 25, 8, 23, tzinfo=timezone(timedelta(hours=8)))

            async def tick(scheduler, when):
                with patch("callarchive.backend.periodic.time.time", return_value=when.timestamp()):
                    await scheduler.tick(when)
                    await asyncio.gather(*scheduler.running.values())

            scheduler = BackendScheduler(settings, store)
            await tick(scheduler, now)
            self.assertEqual(list(settings.notifications.output.glob("*.json")), [])
            item = store.submit("operator", "call-a", {"flow": "local", "options": {},
                                "call": {"callee": "10001"}}, b"fixture", "audio/wav")

            def change(db):
                db.execute("UPDATE recordings SET status='needs_review' WHERE id=?", (item["recording_id"],))
                store.record_event(db, item["recording_id"], "needs_review", actor="test",
                                   error="archive_attachment_unknown")

            with self.assertRaises(ValueError), store.transaction() as db:
                change(db)
                raise ValueError("rollback")
            await tick(scheduler, now + timedelta(seconds=5))
            self.assertEqual(list(settings.notifications.output.glob("*.json")), [])
            with store.transaction() as db:
                change(db)
            await tick(scheduler, now + timedelta(seconds=10))
            preview = list(settings.notifications.output.glob("*.json"))
            self.assertEqual(len(preview), 1)
            await tick(BackendScheduler(settings, store), now + timedelta(seconds=20))
            self.assertEqual(list(settings.notifications.output.glob("*.json")), preview)

    async def test_alert_error_retries_at_next_fallback_without_periodic_empty_sweeps(self):
        import asyncio
        from unittest.mock import patch
        from callarchive.backend.periodic import BackendScheduler
        from callarchive.configuration import Settings

        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            settings = Settings.model_validate({"notifications": {"owner": "operator"}})
            scheduler = BackendScheduler(settings, store)
            start = datetime(2026, 11, 25, 8, 23, tzinfo=timezone(timedelta(hours=8)))

            async def tick(offset):
                when = start + timedelta(seconds=offset)
                with patch("callarchive.backend.periodic.time.time", return_value=when.timestamp()):
                    await scheduler.tick(when)
                    await asyncio.gather(*scheduler.running.values())

            with patch("callarchive.backend.periodic.run_report",
                       side_effect=[ConnectionError("temporary"), {}]) as report:
                await tick(0)
                await tick(5)
                self.assertEqual(report.call_count, 1)
                await tick(180)
                self.assertEqual(report.call_count, 2)
                await tick(480)
                self.assertEqual(report.call_count, 2)

    async def test_idle_alert_check_stays_asleep_until_revision_changes(self):
        from unittest.mock import patch
        from callarchive.backend.periodic import BackendScheduler
        from callarchive.configuration import Settings

        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            settings = Settings.model_validate({"notifications": {"owner": "operator"}})
            scheduler = BackendScheduler(settings, store)
            scheduler.observed_alert_revision = 1

            with (
                patch("callarchive.backend.periodic.time.time", side_effect=[100, 105, 105, 110]),
                patch.object(scheduler.alert_changes, "revision", side_effect=[1, 2]) as revision,
                patch("callarchive.backend.periodic.asyncio.sleep") as sleep,
            ):
                await scheduler.wait_until_due(150)
                self.assertEqual(revision.call_count, 2)
                self.assertEqual([call.args for call in sleep.await_args_list], [(5,), (5,)])

    async def test_backend_restarts_without_repeating_daily_and_resumes_watermark(self):
        from unittest.mock import patch
        from callarchive.backend.periodic import BackendScheduler
        from callarchive.configuration import Settings

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "calls.json"
            source.write_text("[]", encoding="utf-8")
            settings = Settings.model_validate({
                "backend": {"database": root / "archive.sqlite", "scheduler_enabled": True},
                "outbound": {"manifest": source, "owner": "operator", "source": "source", "min_free_mib": 0},
                "notifications": {"owner": "operator", "output": root / "preview"},
            })
            store = ArchiveStore(settings.backend.database)
            local = timezone(timedelta(hours=8))
            at = datetime(2026, 11, 25, 9, 43, tzinfo=local)

            async def complete_tick(scheduler, now):
                with patch("callarchive.backend.periodic.time.time", return_value=now.timestamp()):
                    await scheduler.tick(now)
                    import asyncio
                    await asyncio.gather(*scheduler.running.values())

            await complete_tick(BackendScheduler(settings, store), at)
            preview = list(settings.notifications.output.glob("*.json"))
            self.assertEqual(len(preview), 1)
            later = at + timedelta(minutes=13)
            await complete_tick(BackendScheduler(settings, store), later)
            self.assertEqual(list(settings.notifications.output.glob("*.json")), preview)
            with store.transaction() as db:
                watermark = db.execute("SELECT watermark FROM outbound_schedules").fetchone()[0]
                scheduled = db.execute("SELECT status FROM recurring_tasks WHERE task='daily'").fetchone()[0]
            self.assertEqual(watermark, int(later.replace(minute=55).timestamp()))
            self.assertEqual(scheduled, "succeeded")


class OutboundHealthTest(unittest.TestCase):
    def test_warns_only_once_for_stale_scan_then_reports_recovery(self):
        from unittest.mock import Mock, patch
        from callarchive.backend.scheduling import OutboundHealth

        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            with patch("callarchive.backend.scheduling.event", new_callable=Mock) as recorded:
                health = OutboundHealth(store, "operator", "source", max_lag=90, started_at=100)
                health.check(now=150)
                recorded.assert_not_called()
                health.check(now=191)
                health.check(now=205)
                self.assertEqual(recorded.call_count, 1)
                self.assertEqual(recorded.call_args.args, ("outbound_poll_stalled",))
                schedule = OutboundSchedule(store, "operator", "source", interval=30, lookback=30)
                schedule.finish(schedule.window(datetime.fromtimestamp(213, timezone.utc)))
                health.check(now=213)
                health.check(now=214)
                self.assertEqual(recorded.call_count, 2)
                self.assertEqual(recorded.call_args.args, ("outbound_poll_recovered",))
