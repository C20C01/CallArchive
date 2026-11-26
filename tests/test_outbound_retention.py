import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from callarchive.backend.store import ArchiveStore


class OutboundRetentionTests(unittest.TestCase):
    def test_backfills_call_time_and_expires_only_identified_markers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "archive.sqlite"
            store = ArchiveStore(path)
            started_at = 1_790_000_000.125
            metadata = json.dumps({"started_at": datetime.fromtimestamp(started_at, timezone.utc).isoformat()})
            with store.transaction() as db:
                db.execute("ALTER TABLE outbound_import_exclusions RENAME TO exclusions_before_upgrade")
                db.execute("""
                    CREATE TABLE outbound_import_exclusions (
                        owner TEXT NOT NULL, source TEXT NOT NULL, kind TEXT NOT NULL,
                        identifier TEXT NOT NULL, PRIMARY KEY (owner,source,kind,identifier)
                    )
                """)
                db.execute("DROP TABLE exclusions_before_upgrade")
                db.execute("INSERT INTO calls(id,owner) VALUES ('pending','owner')")
                db.execute("""
                    INSERT INTO outbound_calls(owner,source,external_id,call_id,metadata,complete)
                    VALUES ('owner','source','current','pending',?,0)
                """, (metadata,))
                db.execute("INSERT INTO outbound_call_aliases VALUES ('owner','source','alias','pending')")
                db.execute("""
                    INSERT INTO outbound_recordings(call_id,external_id,reference,metadata)
                    VALUES ('pending','pending-audio','ref','{}')
                """)
                db.executemany("INSERT INTO outbound_import_exclusions VALUES ('owner','source',?,?)",
                               [("call", "alias"), ("call", "unknown"), ("recording", "pending-audio")])

            store = ArchiveStore(path)
            with store.transaction() as db:
                times = dict(db.execute("SELECT identifier,call_started_at FROM outbound_import_exclusions"))
                self.assertEqual(times, {"alias": started_at, "pending-audio": started_at, "unknown": None})
            now = started_at + 7 * 86400
            store.purge_archive_cache(now)
            with store.transaction() as db:
                self.assertEqual([row[0] for row in db.execute("SELECT identifier FROM outbound_import_exclusions")],
                                 ["unknown"])
                self.assertEqual(db.execute("SELECT call_id FROM outbound_calls").fetchone()[0], "pending")
                self.assertEqual(ArchiveStore.outbound_scan_cutoff(db, now, 30 * 86400), started_at)
