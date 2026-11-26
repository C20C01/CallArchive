import tempfile
import unittest
from pathlib import Path

from callarchive.backend.console_calls import ConsoleCalls
from callarchive.backend.store import ArchiveStore, RemovalUnavailable
from test_service import sample_audio


class RemovalTest(unittest.TestCase):
    def test_permanent_call_removal_records_transcription_cleanup_with_the_removal(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "archive.sqlite"
            store = ArchiveStore(db_path)
            metadata = {"flow": "local", "call": {"external_call_id": "call-1"}, "options": {}}
            active = store.submit("owner", "active", metadata, sample_audio(), "audio/wav")
            untouched = store.submit("owner", "queued", metadata, sample_audio(), "audio/wav")
            claim = store.claim(now=100)
            store.advance(claim, "needs_review", job_id="failed-job", error="transcription_failed")

            self.assertTrue(ConsoleCalls(store, {}).remove(active["call_id"], "operator"))
            reopened = ArchiveStore(db_path)
            with reopened.transaction() as db:
                pending = [row["recording_id"] for row in db.execute("SELECT * FROM retired_transcription_groups")]
            self.assertEqual(pending, [active["recording_id"]])
            self.assertIsNone(reopened.get("owner", active["recording_id"]))
            self.assertIsNone(reopened.get("owner", untouched["recording_id"]))

    def test_removed_recording_stops_and_can_restore_only_for_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            item = store.submit("owner", "request", {"flow": "local", "call": None, "options": {}},
                                sample_audio(), "audio/wav")
            recording_id = item["recording_id"]
            claim = store.claim(now=100)
            with self.assertRaises(RemovalUnavailable):
                store.set_removed("owner", recording_id, actor="owner", removed=True, now=101)
            self.assertIsNone(store.set_removed("other", recording_id, actor="other", removed=True, now=101))
            store.advance(claim, "transcribing", job_id="job-1")
            removal = store.set_removed("owner", recording_id, actor="owner", removed=True, now=101)
            self.assertEqual(removal["status"], "removed")
            self.assertEqual(store.set_removed("owner", recording_id, actor="owner", removed=True, now=101), removal)
            self.assertIsNone(store.claim(now=101))
            status = store.set_removed("owner", recording_id, actor="owner", removed=False, now=102)
            self.assertEqual(status["status"], "transcribing")
            self.assertEqual(store.get_audio("owner", recording_id)["audio"], sample_audio())
            self.assertEqual(store.claim(now=102)["job_id"], "job-1")
