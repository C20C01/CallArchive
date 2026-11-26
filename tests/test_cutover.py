import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import AsyncMock

import httpx

from callarchive.archive.local import LocalArchive
from callarchive.backend.store import ArchiveStore
from callarchive.backend.workflow import Workflow
from callarchive.transcription.api import create_app
from callarchive.transcription.engines import DemoEngine
from callarchive.transcription.jobs import JobStore
from callarchive.transcription.worker import process_one
from test_remote_archive import Crashed, Remote
from test_service import sample_audio


def copy_sqlite(source, target):
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as original:
        with closing(sqlite3.connect(target)) as copy:
            original.backup(copy)
            if copy.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("Cutover fixture failed SQLite integrity check")


class CutoverTest(unittest.IsolatedAsyncioTestCase):
    async def test_paired_restore_resumes_original_asr_and_verifies_interrupted_archive_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_archive = root / "archive.sqlite"
            source_asr = root / "transcription.sqlite"
            token = "archive-test"
            identity = ("archive", True)
            provider = Remote()
            provider.verify_attachment = AsyncMock(wraps=provider.verify_attachment)
            store = ArchiveStore(source_archive)
            app = create_app(source_asr, {token: identity})

            def asr_client(api):
                return httpx.AsyncClient(transport=httpx.ASGITransport(app=api), base_url="http://asr",
                                         headers={"Authorization": f"Bearer {token}"})

            def workflow(database, transcription):
                return Workflow(database, transcription, LocalArchive(root / "attachments"),
                                providers={provider.id: provider})

            metadata = {"flow": "remote", "archive_instance": provider.id, "call": None, "options": {}}
            partial = store.submit("operator", "partial", metadata, sample_audio(), "audio/wav")["recording_id"]
            running = store.submit("operator", "running", metadata, sample_audio(2), "audio/wav")["recording_id"]
            now = time.time()
            jobs = JobStore(source_asr)
            async with asr_client(app) as client:
                worker = workflow(store, client)
                await worker.resume(partial, now=now)
                completed = []
                self.assertTrue(process_one(jobs, DemoEngine(), on_finished=completed.append))
                await worker.resume(running, now=now)
                running_job = store.get("operator", running)["transcription_job_id"]
                self.assertEqual(jobs.claim("demo", lease_seconds=1, now=now - 2).job_id, running_job)
                with self.assertRaises(Crashed):
                    await worker.resume_job(completed[0], now=now + 3)

            self.assertEqual(len(provider.saved), 1)
            interrupted_request = next(iter(provider.saved))

            # All fixture writers are quiescent while the two backups are taken.
            backup_archive = root / "backup-archive.sqlite"
            backup_asr = root / "backup-asr.sqlite"
            copy_sqlite(source_archive, backup_archive)
            copy_sqlite(source_asr, backup_asr)

            candidate_archive = root / "candidate-archive.sqlite"
            candidate_asr = root / "candidate-asr.sqlite"
            copy_sqlite(backup_archive, candidate_archive)
            copy_sqlite(backup_asr, candidate_asr)

            reopened = ArchiveStore(candidate_archive)
            restarted_jobs = JobStore(candidate_asr)
            self.assertEqual(reopened.get("operator", partial)["status"], "delivering")
            self.assertEqual(restarted_jobs.get("archive", running_job)["status"], "running")
            finished_after_restart = []
            self.assertTrue(process_one(restarted_jobs, DemoEngine(), on_finished=finished_after_restart.append))
            self.assertEqual(finished_after_restart, [running_job])
            self.assertFalse(process_one(restarted_jobs, DemoEngine()))

            # A missed completion callback is recovered by the scheduled archive pass.
            async with asr_client(create_app(candidate_asr, {token: identity})) as client:
                restarted = workflow(reopened, client)
                await restarted.run_due(now=now + 124)
                self.assertEqual(await restarted.run_due(now=now + 125), 0)

            for recording_id, job_id in ((partial, completed[0]), (running, running_job)):
                details = reopened.get("operator", recording_id)
                self.assertEqual(details["status"], "archived")
                self.assertEqual(details["transcription_job_id"], job_id)
                self.assertEqual(set(details["receipts"]), {"one", "two"})
                self.assertIsNotNone(restarted_jobs.get("archive", job_id)["acknowledged"])
            (verification,) = provider.verify_attachment.await_args_list
            self.assertEqual(verification.args[2], interrupted_request)
            self.assertEqual(len(provider.calls), 8)
            self.assertEqual(len(provider.saved), 8)

            # The rollback snapshots are untouched by the candidate replay.
            for backup, table, identity, state in (
                (backup_archive, "recordings", partial, "delivering"),
                (backup_asr, "jobs", running_job, "running"),
            ):
                with closing(sqlite3.connect(backup.as_uri() + "?mode=ro", uri=True)) as snapshot:
                    self.assertEqual(snapshot.execute(
                        f"SELECT status FROM {table} WHERE id=?", (identity,)
                    ).fetchone()[0], state)
