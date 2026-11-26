import io
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from callarchive.backend.outbound import OutboundImport
from callarchive.backend.store import ArchiveStore
from callarchive.outbound import Page
from callarchive.outbound.qianke import QiankeSource
from test_service import sample_audio


class RecordedQianke(QiankeSource):
    def __init__(self, started_at):
        super().__init__(None, "app", "company", "secret", ())
        self.row = {"talkId": 7, "callTime": int(started_at.timestamp() * 1000),
                    "type": 1, "status": 1, "caller": "10001"}

    def list_calls(self, start, end, cursor, limit):
        return Page([self.record(self.row)], None)

    def open_recording(self, recording):
        return io.BytesIO(sample_audio())


class OutboundImportRegressionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = ArchiveStore(Path(temporary.name) / "archive.sqlite")
        self.start = datetime(2026, 11, 24, tzinfo=timezone.utc)
        self.source = RecordedQianke(self.start + timedelta(minutes=1))
        self.part = {"id": "one", "name": "sample.wav"}
        self.loader = OutboundImport(self.store, "operator", "source", self.source,
                                     min_free_mib=0, skip_call_duration_ms=3000, archive_instance="beisen")

    def scan(self, hours):
        self.assertTrue(self.loader.scan_page(self.start, self.start + timedelta(hours=hours)))

    def test_consumed_source_recheck_does_not_report_a_new_retry(self):
        self.source.row.update(called="10002", audioInfos=[self.part])
        self.scan(1)
        self.assertEqual(self.loader.download(), (1, 0))
        with self.store.transaction() as db:
            call = db.execute("SELECT call_id FROM outbound_calls").fetchone()
            db.execute("UPDATE outbound_calls SET complete=1 WHERE call_id=?", (call["call_id"],))
            db.execute("INSERT INTO outbound_console_retries VALUES (?,1)", (call["call_id"],))
        self.assertFalse(self.store.request_call_retry(call["call_id"], actor="operator"))

    def test_historical_lookup_excludes_newly_identified_short_call(self):
        self.source.row["called"] = "10002"
        self.scan(1)
        self.source.row.update(talkLen=1500, audioInfos=[self.part])
        self.loader.refresh_waiting(now=self.start + timedelta(hours=6))

        self.assertEqual(self.loader.download(), (0, 0))
        with self.store.transaction() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM recordings").fetchone()[0], 0)
            self.assertIsNotNone(db.execute(
                "SELECT 1 FROM outbound_import_exclusions WHERE kind='recording' AND identifier='one'"
            ).fetchone())

    def test_historical_lookup_uses_late_callee_for_archive(self):
        self.scan(1)
        self.source.row.update(called="10002", talkLen=6000, audioInfos=[self.part])
        self.loader.refresh_waiting(now=self.start + timedelta(hours=6))

        self.assertEqual(self.loader.download(), (1, 0))
        self.assert_saved_callee("10002")

    def test_download_uses_callee_corrected_after_staging(self):
        self.source.row["audioInfos"] = [self.part]
        self.scan(1)
        self.source.row["called"] = "10002"
        self.scan(2)
        del self.source.row['called']
        self.scan(3)

        self.assertEqual(self.loader.download(), (1, 0))
        self.assert_saved_callee("10002")

    def assert_saved_callee(self, callee):
        with self.store.transaction() as db:
            self.assertEqual(db.execute(
                "SELECT json_extract(metadata, '$.call.callee') FROM recordings"
            ).fetchone()[0], callee)

    def test_recovers_committed_audio_after_linking_is_interrupted(self):
        self.source.row["audioInfos"] = [self.part]
        self.scan(1)
        submit = self.store.submit

        def submit_then_interrupt(*args, **kwargs):
            submit(*args, **kwargs)
            raise InterruptedError("stopped between commits")

        self.store.submit = submit_then_interrupt
        self.assertEqual(self.loader.download(), (0, 1))
        self.store.submit = submit

        def unavailable(recording):
            raise AssertionError("already stored audio should not be downloaded")

        self.source.open_recording = unavailable
        self.assertEqual(self.loader.download(), (1, 0))
        with self.store.transaction() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM recordings").fetchone()[0], 1)
            linked = db.execute("SELECT recording_id, error FROM outbound_recordings").fetchone()
            self.assertEqual(tuple(linked), (db.execute("SELECT id FROM recordings").fetchone()[0], None))
