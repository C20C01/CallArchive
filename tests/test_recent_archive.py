import json
import tempfile
import unittest
from pathlib import Path

from callarchive.backend.console_recent import RecentArchives
from callarchive.backend.store import ArchiveStore


class RecentArchiveTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = ArchiveStore(Path(directory.name) / "archive.sqlite")
        self.recent = RecentArchives(self.store)
        self.call_id = "outbound-1"
        call = {"callee": "123456789", "started_at": "2023-11-14T16:00:00Z"}
        with self.store.transaction() as db:
            db.execute("INSERT INTO calls (id, owner) VALUES (?, 'provider')", (self.call_id,))
            db.execute("""INSERT INTO outbound_calls (owner, source, external_id, call_id, metadata, complete)
                          VALUES ('provider', 'demo', 'external-1', ?, ?, 1)""",
                       (self.call_id, json.dumps(call)))
        self.segments = [
            self.store.submit("provider", f"segment-{index}", {"call": call}, b"audio", "audio/wav",
                              linked_call=self.call_id, duration_ms=5000)["recording_id"]
            for index in (1, 2)
        ]
        with self.store.transaction() as db:
            db.executemany("""INSERT INTO outbound_recordings (call_id, external_id, reference, metadata, recording_id)
                              VALUES (?, ?, 'reference', '{}', ?)""",
                           [(self.call_id, str(index), recording_id)
                            for index, recording_id in enumerate(self.segments, 1)])

    def archive_segment(self, index, when):
        recording_id = self.segments[index]
        with self.store.transaction() as db:
            db.execute("UPDATE recordings SET status='archived' WHERE id=?", (recording_id,))
            self.store.record_event(db, recording_id, "archived", actor="test", now=when)

    def test_completed_discovery_with_unarchived_segment_is_hidden(self):
        self.archive_segment(0, 1_699_999_990)
        self.assertEqual(self.recent.list(day=None, mobile=None, offset=0, now=1_700_000_000)["calls"], [])
        self.assertEqual(self.recent.files(self.call_id, now=1_700_000_000), [])

    def test_outbound_segments_expire_together_after_call_completes(self):
        self.archive_segment(0, 1_699_900_000)
        self.archive_segment(1, 1_700_000_000)
        last_visible = 1_700_086_399
        self.assertEqual(self.store.purge_archive_cache(now=last_visible), 0)
        listing = self.recent.list(day=None, mobile=None, offset=0, now=last_visible)["calls"]
        self.assertEqual([(call["talk_id"], call["recording_count"]) for call in listing], [(self.call_id, 2)])
        self.assertEqual(listing[0]["archived_at"], "2023-11-15T06:13:20+08:00")
        files = self.recent.files(self.call_id, now=last_visible)
        self.assertEqual([file["file_id"] for file in files], self.segments)
        self.assertEqual([file["expires_at"] for file in files], ["2023-11-16T06:13:20+08:00"] * 2)
        self.assertEqual(self.recent.files(self.call_id, now=last_visible + 1), [])
        self.assertEqual(self.recent.list(day=None, mobile=None, offset=0, now=last_visible + 1)["calls"], [])
        self.assertEqual(self.store.purge_archive_cache(now=last_visible + 1), 2)
