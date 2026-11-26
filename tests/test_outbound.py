
import json
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from pathlib import Path

from callarchive.backend.console_calls import ConsoleCalls
from callarchive.backend.outbound import OutboundImport
from callarchive.backend.store import ArchiveStore
from callarchive.outbound import CallRecord, CursorExpired, Page
from callarchive.outbound.manifest import ManifestSource
from test_service import sample_audio



class OutboundSourceContinuityTest(unittest.TestCase):
    def setUp(self):
        from callarchive.configuration import Settings

        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "archive.sqlite"
        self.store = ArchiveStore(self.database)
        self.now = datetime(2026, 11, 26, 10, tzinfo=timezone.utc)
        self.started = self.now - timedelta(hours=2)
        self.settings = Settings(outbound={
            "owner": "owner", "active_source": "new", "min_free_mib": 0,
            "sources": {name: {"driver": "http", "url": f"https://{name}.example"}
                        for name in ("old", "new")},
        })

    def provider_patch(self):
        from io import BytesIO
        from unittest.mock import Mock

        from callarchive.outbound import RecordingRef

        self.providers = {}
        for name in ("old", "new"):
            self.providers[name] = Mock(
                spec=["list_calls", "get_recordings", "open_recording"],
            )
            self.providers[name].list_calls.return_value = Page([], None)
            self.providers[name].get_recordings.return_value = (
                [RecordingRef(id="found", reference="audio")], True,
            )
            self.providers[name].open_recording.side_effect = lambda recording: BytesIO(sample_audio())
        return patch("callarchive.backend.import_outbound.HTTPSource",
                     side_effect=lambda client: self.providers[client.base_url.host.split(".")[0]])

    def seed_waiting(self, started=None):
        loader = OutboundImport(self.store, "owner", "old", None)
        with self.store.transaction() as db:
            return loader.save_call(db, "old-call", {"started_at": (started or self.started).isoformat()}, False)["call_id"]

    def test_direct_http_accepts_a_namespace_supplied_by_cli(self):
        from callarchive.backend.import_outbound import run_import
        from callarchive.configuration import Settings

        settings = Settings(outbound={"owner": "owner", "url": "https://old.example"})
        with self.provider_patch():
            run_import(settings, source_id="old", database=self.database,
                       start=self.started, end=self.now, now=self.now)
            self.providers["old"].list_calls.assert_called_once()

    def test_explicit_source_uses_its_configured_adapter(self):
        from callarchive.backend.import_outbound import run_import

        with self.provider_patch():
            self.providers["old"].list_calls.return_value = Page(
                [CallRecord(external_call_id="old-call", started_at=self.started, recordings_complete=True)], None,
            )
            run_import(self.settings, source_id="old", database=self.database,
                       start=self.started - timedelta(seconds=1), end=self.now, now=self.now)
            self.providers["old"].list_calls.assert_called_once()
            self.providers["new"].list_calls.assert_not_called()
        with self.store.transaction() as db:
            self.assertEqual(tuple(db.execute("SELECT source,external_id FROM outbound_calls").fetchone()),
                             ("old", "old-call"))

    def test_scheduled_active_scan_continues_old_waiting_recordings_without_discovery(self):
        from callarchive.backend.import_outbound import run_import

        call_id = self.seed_waiting()
        with self.provider_patch():
            self.assertEqual(run_import(self.settings, database=self.database,
                                        scheduled_at=self.now, now=self.now), (1, 0))
            self.providers["new"].list_calls.assert_called_once()
            self.providers["new"].get_recordings.assert_not_called()
            self.providers["old"].list_calls.assert_not_called()
            self.providers["old"].get_recordings.assert_called_once_with("old-call")
        self.assertEqual(len(self.store.call_recordings(call_id)), 1)

    def test_manual_retry_uses_the_calls_original_adapter(self):
        from callarchive.backend.import_outbound import retry_call_recordings

        call_id = self.seed_waiting(datetime.now(timezone.utc) - timedelta(hours=2))
        self.assertTrue(self.store.request_call_retry(call_id, actor="operator"))
        with self.provider_patch():
            retry_call_recordings(self.settings, self.store, call_id)
            self.providers["old"].get_recordings.assert_called_once_with("old-call")
            self.providers["new"].get_recordings.assert_not_called()
        self.assertEqual(len(self.store.call_recordings(call_id)), 1)

class OutboundTest(unittest.TestCase):
    def test_import_reads_all_pages_and_keeps_a_checkpoint_on_cursor_cycles(self):
        from callarchive.backend.import_outbound import run_import
        from callarchive.configuration import Settings

        now = datetime(2026, 11, 26, 10, tzinfo=timezone.utc)
        start = now - timedelta(hours=1)
        settings = Settings(outbound={"owner": "owner", "source": "adapter", "min_free_mib": 0})

        def page(start, end, cursor, limit):
            position = int(cursor or 0)
            call = CallRecord(external_call_id=str(position), started_at=start + timedelta(seconds=position),
                              recordings_complete=True)
            return Page([call], str(position + 1) if position < 100 else None)

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "archive.sqlite"
            adapter = SimpleNamespace(list_calls=page, open_recording=None)
            with patch("callarchive.backend.import_outbound.configured_source", return_value=adapter):
                run_import(settings, database=database, start=start, end=now, now=now)
            with ArchiveStore(database).transaction() as db:
                self.assertEqual(db.execute("SELECT count(*) FROM outbound_calls").fetchone()[0], 101)
                self.assertEqual(tuple(db.execute("SELECT cursor,done FROM outbound_windows").fetchone()), (None, 1))

            other_start = start - timedelta(hours=1)
            with patch("callarchive.backend.import_outbound.configured_source", return_value=adapter):
                adapter.list_calls = lambda start, end, cursor, limit: Page([], "a" if cursor != "a" else "b")
                with self.assertRaisesRegex(ValueError, "cursor"):
                    run_import(settings, database=database, start=other_start, end=start, now=now)
                with ArchiveStore(database).transaction() as db:
                    self.assertEqual(tuple(db.execute("SELECT cursor,done FROM outbound_windows WHERE done=0").fetchone()),
                                     ("b", 0))

    def test_qianke_resumes_the_previous_window_across_midnight(self):
        from callarchive.backend.import_outbound import run_import
        from callarchive.backend.scheduling import OutboundSchedule
        from callarchive.configuration import Settings
        from callarchive.outbound.qianke import QiankeSource

        midnight = datetime(2026, 11, 26, tzinfo=timezone(timedelta(hours=8)))
        previous = midnight - timedelta(minutes=5)
        after = midnight + timedelta(hours=1)
        settings = Settings(outbound={"owner": "owner", "source": "adapter", "min_free_mib": 0,
                                      "regular_lookback_seconds": 900})
        provider = QiankeSource(None, "", "", "", ())
        requests = []

        def list_calls(start, end, cursor, limit):
            requests.append((start, end, cursor))
            if len(requests) == 1:
                return Page([], "resume")
            if len(requests) == 2:
                raise OSError("provider interrupted")
            if end == after and start <= midnight:
                call = CallRecord(external_call_id="arrived-late", started_at=midnight,
                                  recordings_complete=True)
                return Page([call], None)
            return Page([], None)

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "archive.sqlite"
            schedule = OutboundSchedule(ArchiveStore(database), "owner", "adapter", interval=300, lookback=900)
            schedule.finish(schedule.window(previous - timedelta(minutes=5)))
            with patch("callarchive.backend.import_outbound.configured_source", return_value=provider), \
                 patch.object(provider, "list_calls", side_effect=list_calls):
                with self.assertRaisesRegex(OSError, "interrupted"):
                    run_import(settings, database=database, scheduled_at=previous, now=previous)
                run_import(settings, database=database, scheduled_at=after, now=after)

            self.assertEqual(requests, [
                (previous - timedelta(minutes=15), previous, None),
                (previous - timedelta(minutes=15), previous, "resume"),
                (previous - timedelta(minutes=15), previous, "resume"),
                (previous, after, None),
            ])
            with ArchiveStore(database).transaction() as db:
                self.assertEqual(db.execute("SELECT watermark FROM outbound_schedules").fetchone()[0], after.timestamp())
                self.assertEqual(db.execute("SELECT external_id FROM outbound_calls").fetchone()[0], "arrived-late")

    def test_recording_followup_uses_twenty_minutes_and_three_hours(self):
        class SingleLookup(ManifestSource):
            def __init__(self, path):
                super().__init__(path)
                self.lookups = []

            def get_recordings(self, external_call_id):
                self.lookups.append(external_call_id)
                return ([], False)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            now = datetime(2026, 11, 25, 9, tzinfo=timezone.utc)
            manifest = root / "calls.json"
            manifest.write_text(json.dumps([
                {"external_call_id": "recent", "started_at": (now - timedelta(hours=8)).isoformat(),
                 "recordings_complete": False},
                {"external_call_id": "older", "started_at": (now - timedelta(days=2)).isoformat(),
                 "recordings_complete": False},
            ]), encoding="utf-8")
            source = SingleLookup(manifest)
            loader = OutboundImport(ArchiveStore(root / "archive.sqlite"), "owner", "source", source)
            # This adapter has no window endpoint.
            with patch.object(source, "get_recordings_in_window", None, create=True), \
                 patch.object(source, "get_calls_in_window", None, create=True):
                loader.scan_page(now - timedelta(days=3), now)
                loader.refresh_waiting(now=now)
                self.assertEqual(set(source.lookups), {"recent", "older"})
                source.lookups.clear()
                loader.refresh_waiting(now=now + timedelta(minutes=19))
                self.assertEqual(source.lookups, [])
                loader.refresh_waiting(now=now + timedelta(minutes=20))
                self.assertEqual(source.lookups, ["recent"])
                source.lookups.clear()
                loader.refresh_waiting(now=now + timedelta(hours=3))
                self.assertEqual(set(source.lookups), {"recent", "older"})

    def test_manual_import_respects_configured_archive_history(self):
        from callarchive.backend.import_outbound import run_import
        from callarchive.configuration import Settings

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            now = datetime(2026, 11, 25, 9, tzinfo=timezone.utc)
            manifest = root / "calls.json"
            manifest.write_text(json.dumps([
                {"external_call_id": name, "started_at": (now - timedelta(days=age)).isoformat(),
                 "recordings_complete": True}
                for name, age in (("old", 3), ("cutoff", 2), ("recent", 1))
            ]), encoding="utf-8")
            settings = Settings(archive={"history_retention_days": 2}, outbound={
                "manifest": manifest, "owner": "operator", "source": "source", "min_free_mib": 0,
            })
            database = root / "archive.sqlite"
            run_import(settings, database=database, start=now - timedelta(days=10), end=now, now=now)
            with ArchiveStore(database).transaction() as db:
                self.assertEqual([row["external_id"] for row in db.execute("SELECT external_id FROM outbound_calls")],
                                 ["recent"])

    def test_daily_cycle_advances_new_recordings_before_followup(self):
        from callarchive.backend.import_outbound import run_import
        from callarchive.configuration import Settings

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "calls.json"
            (root / "current.wav").write_bytes(sample_audio())
            manifest.write_text(json.dumps([{
                "external_call_id": "current", "started_at": "2026-11-24T08:00:00Z",
                "recordings": [{"id": "current-audio", "reference": "current.wav"}],
                "recordings_complete": True,
            }]), encoding="utf-8")
            settings = Settings(outbound={"manifest": manifest, "owner": "operator", "source": "source",
                                         "min_free_mib": 0})
            sequence = []
            with patch.object(OutboundImport, "refresh_waiting", side_effect=lambda **kwargs: sequence.append("old")):
                end = datetime(2026, 11, 24, 9, tzinfo=timezone.utc)
                def advance_new(recording_ids):
                    sequence.append("new")
                    self.assertEqual(len(recording_ids), 1)
                    data = ArchiveStore(root / "archive.sqlite").get_audio("operator", recording_ids[0])
                    self.assertEqual(data["audio"], sample_audio())

                result = run_import(settings, database=root / "archive.sqlite", scheduled_at=end,
                                    now=end, advance_new=advance_new)
            self.assertEqual(sequence, ["new", "old"])
            self.assertEqual(result, (1, 0))

    def test_mid_scan_cursor_expiry_restarts_once_without_repeating_calls(self):
        from callarchive.backend.import_outbound import run_import
        from callarchive.configuration import Settings

        now = datetime(2026, 11, 24, 10, tzinfo=timezone.utc)
        first = CallRecord(external_call_id="first", started_at=now - timedelta(minutes=30),
                           recordings_complete=True)
        last = first.model_copy(update={"external_call_id": "last"})
        settings = Settings(outbound={"owner": "operator", "source": "demo"})

        for expires_again in (False, True):
            with self.subTest(expires_again=expires_again), tempfile.TemporaryDirectory() as directory:
                database = Path(directory) / "archive.sqlite"
                requests = []

                def list_calls(start, end, cursor, limit):
                    requests.append(cursor)
                    if cursor is None:
                        return Page([first], "cursor")
                    if len(requests) == 2 or expires_again:
                        raise CursorExpired
                    return Page([last], None)

                source = SimpleNamespace(list_calls=list_calls, open_recording=lambda _: self.fail("unexpected audio"))
                with patch("callarchive.backend.import_outbound.configured_source", return_value=source):
                    args = {"settings": settings, "database": database, "scheduled_at": now, "now": now}
                    if expires_again:
                        with self.assertRaisesRegex(ValueError, "expired again"):
                            run_import(**args)
                    else:
                        self.assertEqual(run_import(**args), (0, 0))
                self.assertEqual(requests, [None, "cursor", None, "cursor"])
                with ArchiveStore(database).transaction() as db:
                    ids = [row[0] for row in db.execute("SELECT external_id FROM outbound_calls ORDER BY external_id")]
                    self.assertEqual(ids, ["first"] if expires_again else ["first", "last"])
                    self.assertEqual(db.execute("SELECT watermark FROM outbound_schedules").fetchone()[0],
                                     None if expires_again else now.timestamp())

    def test_resume_page_and_import_late_recording_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "calls.json"
            (root / "first.wav").write_bytes(sample_audio())
            (root / "second.wav").write_bytes(sample_audio(2))
            calls = [{
                "external_call_id": "call-1", "started_at": "2026-11-24T08:00:00Z",
                "recordings": [{"id": "one", "reference": "first.wav"}],
                "recordings_complete": False,
            }, {
                "external_call_id": "call-2", "started_at": "2026-11-24T09:00:00Z",
                "recordings_complete": False,
            }]
            manifest.write_text(json.dumps(calls), encoding="utf-8")
            start = datetime(2026, 11, 24, tzinfo=timezone.utc)
            end = datetime(2026, 11, 25, tzinfo=timezone.utc)
            store = ArchiveStore(root / "archive.sqlite")
            loader = OutboundImport(store, "operator", "demo-source", ManifestSource(manifest))
            self.assertFalse(loader.scan_page(start, end, page_size=1))
            class ExpiredOnce(ManifestSource):
                def list_calls(self, start, end, cursor, limit):
                    if cursor is not None and not getattr(self, "expired", False):
                        self.expired = True
                        raise CursorExpired
                    return super().list_calls(start, end, cursor, limit)

            loader = OutboundImport(store, "operator", "demo-source", ExpiredOnce(manifest))
            self.assertFalse(loader.scan_page(start, end, page_size=1))
            self.assertFalse(loader.scan_page(start, end, page_size=1))
            self.assertTrue(loader.scan_page(start, end, page_size=1))
            self.assertEqual(loader.download(), (1, 0))

            calls[0]["recordings"].append({"id": "two", "reference": "second.wav"})
            calls[0]["recordings_complete"] = True
            manifest.write_text(json.dumps(calls), encoding="utf-8")
            loader.refresh_waiting()
            self.assertEqual(loader.download(), (1, 0))
            self.assertTrue(loader.scan_page(start, end, page_size=1))
            self.assertEqual(loader.download(), (0, 0))
            with store.transaction() as db:
                rows = db.execute("SELECT call_id, status FROM recordings").fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual({row["status"] for row in rows}, {"queued"})
            self.assertIsNotNone(rows[0]["call_id"])
            self.assertEqual(rows[0]["call_id"], rows[1]["call_id"])
            detail = ConsoleCalls(store, {}).pending_detail(rows[0]["call_id"])
            self.assertEqual(detail["call_time"], "2026-11-24T16:00:00+08:00")
            self.assertEqual([item["download_name"] for item in detail["recordings"]],
                             ["2026-11-24_16-00-00.wav", "2026-11-24_16-00-00_2.wav"])

    def test_corrected_short_call_retires_existing_recording_after_active_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "calls.json"
            (root / "first.wav").write_bytes(sample_audio())
            call = {
                "external_call_id": "outbound-id", "started_at": "2026-11-24T08:00:00Z",
                "recordings": [{"id": "one", "reference": "first.wav"}],
                "recordings_complete": True,
            }
            start = datetime(2026, 11, 24, tzinfo=timezone.utc)
            end = datetime(2026, 11, 25, tzinfo=timezone.utc)
            manifest.write_text(json.dumps([call]), encoding="utf-8")
            store = ArchiveStore(root / "archive.sqlite")
            loader = OutboundImport(store, "operator", "demo-source", ManifestSource(manifest),
                                    skip_call_duration_ms=3000)
            self.assertTrue(loader.scan_page(start, end))
            self.assertEqual(loader.download(), (1, 0))
            claim = store.claim(now=time.time())
            call["call_duration_ms"] = 3000
            manifest.write_text(json.dumps([call]), encoding="utf-8")
            self.assertTrue(loader.scan_page(start.replace(hour=1), end))
            self.assertEqual(loader.download(), (0, 0))
            self.assertIsNotNone(store.get_audio("operator", claim["id"]))
            store.advance(claim, "transcribing")
            self.assertIsNone(store.claim(now=time.time()))
            self.assertEqual(loader.download(), (0, 0))
            self.assertIsNone(store.get("operator", claim["id"]))
            self.assertTrue(loader.scan_page(start.replace(hour=2), end))
            self.assertEqual(loader.download(), (0, 0))
            with store.transaction() as db:
                self.assertEqual(db.execute("SELECT count(*) FROM outbound_calls").fetchone()[0], 0)
                exclusions = db.execute("SELECT kind,identifier FROM outbound_import_exclusions").fetchall()
                self.assertEqual({tuple(row) for row in exclusions}, {("call", "outbound-id"), ("recording", "one")})


class OutboundRetryTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.manifest = self.directory / "calls.json"
        self.write_manifest()
        self.store = ArchiveStore(self.directory / "archive.sqlite")
        self.loader = OutboundImport(self.store, "operator", "source", ManifestSource(self.manifest))
        self.scan()

    def write_manifest(self):
        self.manifest.write_text(json.dumps([{
            "external_call_id": "call-1", "started_at": "2026-11-24T08:00:00Z",
            "recordings": [{"id": "one", "reference": "first.wav"}],
            "recordings_complete": True,
        }]), encoding="utf-8")

    def scan(self):
        start = datetime(2026, 11, 24, tzinfo=timezone.utc)
        self.loader.scan_page(start, start.replace(day=25))

    def test_undecodable_audio_waits_for_manual_retry(self):
        recording = self.directory / "first.wav"
        recording.write_bytes(b"not audio")
        self.assertEqual(self.loader.download(), (0, 1))
        recording.write_bytes(sample_audio())
        self.assertEqual(self.loader.download(), (0, 0))

        with self.store.transaction() as db:
            call_key = db.execute("SELECT call_id FROM outbound_calls").fetchone()[0]
        self.assertTrue(ConsoleCalls(self.store, {}).retry(call_key, "operator"))
        source = self.loader.source
        with patch.object(source, "reopen_recording", source.open_recording, create=True):
            with patch.object(source, "open_recording", side_effect=AssertionError("recording URL was not refreshed")):
                self.assertEqual(self.loader.download(), (1, 0))

    def test_missing_recording_stops_after_four_attempts_until_manual_retry(self):
        for _ in range(4):
            self.assertEqual(self.loader.download(), (0, 1))
        self.assertEqual(self.loader.download(), (0, 0))
        (self.directory / "first.wav").write_bytes(sample_audio())
        with self.store.transaction() as db:
            call_key = db.execute("SELECT call_id FROM outbound_calls").fetchone()[0]
        self.assertTrue(ConsoleCalls(self.store, {}).retry(call_key, "operator"))
        self.assertEqual(self.loader.download(), (1, 0))

    def test_historical_qianke_groups_by_beijing_date_and_isolates_window_errors(self):
        from callarchive.outbound.qianke import QiankeSource

        class GroupedSource(ManifestSource):
            recording_window_timezone = QiankeSource.recording_window_timezone

            def __init__(self, path):
                super().__init__(path)
                self.lookups = []

            def get_recordings_in_window(self, ids, start, end):
                self.lookups.append((set(ids), start, end))
                if "failed" in ids and sum("failed" in group for group, _, _ in self.lookups) <= 2:
                    raise OSError("temporary upstream failure")
                return {call_id: ([], call_id != "failed") for call_id in ids}

        start = datetime(2026, 11, 24, 15, tzinfo=timezone.utc)
        call_times = {"failed": start, "one": start + timedelta(hours=1),
                      "two": start + timedelta(hours=1, minutes=15)}
        self.manifest.write_text(json.dumps([{
            "external_call_id": identifier, "started_at": occurred.isoformat(),
            "recordings_complete": False,
        } for identifier, occurred in call_times.items()]), encoding="utf-8")
        source = GroupedSource(self.manifest)
        loader = OutboundImport(self.store, "operator", "source", source)
        self.assertTrue(loader.scan_page(start, start + timedelta(hours=3)))
        now = start + timedelta(hours=3)
        loader.refresh_waiting(now=now)
        self.assertEqual(source.lookups, [
            ({"failed"}, start - timedelta(hours=23), start + timedelta(hours=1)),
            ({"one", "two"}, start + timedelta(hours=1), now),
        ])
        with self.store.transaction() as db:
            calls = dict(db.execute("SELECT external_id, complete FROM outbound_calls").fetchall())
            failed_call = db.execute("SELECT call_id FROM outbound_calls WHERE external_id='failed'").fetchone()[0]
        self.assertEqual(calls, {"call-1": 1, "failed": 0, "one": 1, "two": 1})
        loader.refresh_waiting(now=now + timedelta(minutes=5))
        self.assertEqual(len(source.lookups), 2)
        self.assertTrue(ConsoleCalls(self.store, {}).retry(failed_call, "operator"))
        loader.refresh_waiting(now=now + timedelta(minutes=10))
        self.assertEqual(len(source.lookups), 3)
        loader.refresh_waiting(now=now + timedelta(minutes=15))
        self.assertEqual(len(source.lookups), 4)
        loader.refresh_waiting(now=now + timedelta(minutes=20))
        self.assertEqual(len(source.lookups), 4)

    def test_disk_reserve_covers_payload_without_consuming_an_attempt(self):
        audio = sample_audio()
        (self.directory / "first.wav").write_bytes(audio)
        reserve = 1024 * 1024**2
        with patch("shutil.disk_usage", return_value=SimpleNamespace(free=reserve + len(audio) - 1)):
            self.assertEqual(self.loader.download(), (0, 0))
        with self.store.transaction() as db:
            entry = db.execute("SELECT * FROM outbound_recordings").fetchone()
        self.assertIsNone(entry["recording_id"])
        self.assertIsNone(entry["error"])
        with patch("shutil.disk_usage", return_value=SimpleNamespace(free=reserve + len(audio))):
            self.assertEqual(self.loader.download(), (1, 0))
