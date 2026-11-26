import io
import json
import sqlite3
import threading
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from callarchive.configuration import LoggingSettings, Settings
from callarchive.observability import RuntimeLog, configure_logging, event


class RuntimeLogTest(unittest.TestCase):
    def test_logs_use_age_independent_of_insert_order(self):
        with tempfile.TemporaryDirectory() as directory:
            log = RuntimeLog(Path(directory) / "events.sqlite")
            now = datetime.now(timezone.utc)
            for days in (6, 8, 1):
                payload = {"time": (now - timedelta(days=days)).isoformat(), "component": "test", "days": days}
                log.append(payload, json.dumps(payload))
            self.assertEqual([row["days"] for row in log.list()["items"]], [1, 6])
            with closing(log.connect()) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM runtime_events").fetchone()[0], 2)


    def test_events_are_correlatable_and_only_include_allowed_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "events.sqlite"
            output = io.StringIO()
            configure_logging(LoggingSettings(format="json", database=db), "archive-worker", stream=output)
            try:
                event("stage_changed", recording_id="rec-1", job_id="transcription-2",
                      status="archived", unexpected="should-never-appear", reason="https://secret.example/token")
                event("stage_changed", level="DEBUG", recording_id="rec-1", status="debug_hidden")
                record = json.loads(output.getvalue())
                self.assertEqual(record["component"], "archive-worker")
                self.assertEqual(record["recording_id"], "rec-1")
                self.assertEqual(record["job_id"], "transcription-2")
                self.assertNotIn("reason", record)
                self.assertNotIn("unexpected", record)
                events = RuntimeLog(db).list(related_id="transcription-2", limit=10)["items"]
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0]["status"], "archived")
                self.assertEqual(RuntimeLog(db).list(related_id="different", limit=10)["items"], [])
            finally:
                configure_logging(LoggingSettings(level="CRITICAL"), "test", stream=io.StringIO())


    def test_opening_a_log_waits_for_existing_reader_before_enabling_wal(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "operations.sqlite"
            reader = sqlite3.connect(database, check_same_thread=False)
            reader.execute("CREATE TABLE existing (value INTEGER)")
            reader.execute("BEGIN")
            reader.execute("SELECT * FROM existing").fetchall()

            def without_busy_timeout(instance):
                connection = sqlite3.connect(instance.database, timeout=0)
                connection.row_factory = sqlite3.Row
                return connection

            release = threading.Timer(0.15, reader.commit)
            release.start()
            try:
                with patch.object(RuntimeLog, "connect", without_busy_timeout):
                    RuntimeLog(database)
            finally:
                release.join()
                reader.close()


    def test_unavailable_operations_database_keeps_stderr_logging(self):
        with tempfile.TemporaryDirectory() as directory:
            output = io.StringIO()
            try:
                self.assertIsNone(configure_logging(LoggingSettings(format="json", database=Path(directory)),
                                                   "archive-worker", stream=output))
                event("process_started")
                records = [json.loads(line) for line in output.getvalue().splitlines()]
                self.assertEqual([row["event"] for row in records], ["runtime_log_unavailable", "process_started"])
                self.assertNotIn(directory, output.getvalue())
            finally:
                configure_logging(LoggingSettings(level="CRITICAL"), "test", stream=io.StringIO())

    def test_scheduled_archive_logs_outbound_failure_and_keeps_existing_work_moving(self):
        import asyncio

        from callarchive.backend.__main__ import run_due
        from callarchive.backend.workflow import Workflow

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "events.sqlite"
            settings = Settings(outbound={"owner": "operator", "source": "demo"},
                                logging={"database": path})
            configure_logging(settings.logging, "archive-worker", stream=io.StringIO())
            try:
                with patch("callarchive.backend.__main__.run_import",
                           side_effect=ValueError("provider response detail")), patch.object(
                    Workflow, "run_due", return_value=0,
                ) as resume:
                    with self.assertRaisesRegex(ValueError, "provider response detail"):
                        asyncio.run(run_due(root / "archive.sqlite", root / "attachments",
                                            "test", "http://127.0.0.1:8765", settings))
                    resume.assert_awaited_once()
                entries = RuntimeLog(path).list(important_only=True)["items"]
                self.assertEqual([(row["event"], row["reason"]) for row in entries],
                                 [("outbound_import_stopped", "ValueError")])
                self.assertNotIn("provider response detail", str(entries))
            finally:
                configure_logging(LoggingSettings(level="CRITICAL"), "test", stream=io.StringIO())

    def test_outbound_import_logs_invalid_schedule_and_preserves_exit_status(self):
        from callarchive.backend.import_outbound import main

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "operations.sqlite"
            settings = LoggingSettings(format="json", database=path)
            defaults = Settings(logging=settings)
            try:
                with patch("sys.argv", ["import_outbound"]), patch(
                    "callarchive.backend.import_outbound.config_from_args", return_value=defaults
                ), self.assertRaises(SystemExit) as raised:
                    main()
                self.assertEqual(raised.exception.code, 2)
                row = RuntimeLog(path).list(important_only=True)["items"][0]
                self.assertEqual((row["event"], row["reason"]),
                                 ("outbound_import_stopped", "outbound_schedule_arguments"))
            finally:
                configure_logging(LoggingSettings(level="CRITICAL"), "test", stream=io.StringIO())
