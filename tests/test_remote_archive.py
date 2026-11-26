import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import httpx

from callarchive.archive.provider import Lookup, MatchStatus, VerifyResult, VerifyStatus, WriteResult, WriteStatus
from callarchive.archive.local import LocalArchive
from callarchive.backend.api import create_app as create_backend
from callarchive.backend.console_calls import ConsoleCalls
from callarchive.backend.console_recent import RecentArchives
from callarchive.backend.reporting import collect_issues
from callarchive.backend.remote_archive import RemoteDelivery
from callarchive.backend.store import ArchiveStore, SubmissionConflict
from callarchive.backend.workflow import Workflow
from callarchive.transcription.api import create_app as create_transcription
from callarchive.transcription.engines import DemoEngine
from callarchive.transcription.jobs import JobStore
from callarchive.transcription.worker import process_one
from test_service import sample_audio


class Crashed(BaseException):
    pass


class Remote:
    id = "directory-v1"
    supports_idempotency = False
    supports_verification = True

    def __init__(self):
        self.saved = {}
        self.calls = []
        self.crash = True

    async def find_targets(self, fields):
        return Lookup(MatchStatus.MATCHED, ("one", "two"))

    async def put_attachment(self, target, attachment, request_id):
        self.calls.append((target, attachment.name))
        assert request_id not in self.saved
        self.saved[request_id] = f"{target}/{attachment.name}"
        if self.crash:
            self.crash = False
            raise Crashed
        return WriteResult(WriteStatus.CONFIRMED, self.saved[request_id])

    async def verify_attachment(self, target, attachment, request_id):
        return VerifyResult(VerifyStatus.CONFIRMED, self.saved[request_id])


class ExternalArchiveTest(unittest.TestCase):

    def test_waiting_calls_share_automatic_lookup_and_resume_without_manual_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_auto_recheck(Path(directory)))

    async def exercise_auto_recheck(self, root):
        zone = timezone(timedelta(hours=8))

        def at(day, hour, minute=0):
            return datetime(2026, 1, day, hour, minute, tzinfo=zone).timestamp()

        store = ArchiveStore(root / "archive.sqlite")
        provider = Remote()
        provider.lookup_by_callee = True
        provider.crash = False
        provider.find_targets = AsyncMock(return_value=Lookup(MatchStatus.NEEDS_INPUT))
        worker = Workflow(store, None, LocalArchive(root / "attachments"), providers={provider.id: provider})
        ids = []
        for n in range(2):
            item = store.submit("owner", f"wait-{n}", {
                "flow": "remote", "archive_instance": provider.id, "call": {"callee": "10002"}, "options": {},
            }, sample_audio(), "audio/wav")
            ids.append(item["recording_id"])
            start = at(1, 9, 15)
            store.advance(store.claim(now=start), "transcribing", job_id="done")
            store.advance(store.claim(now=start), "acknowledging", transcript='{"text":"result"}')
            store.advance(store.claim(now=start), "delivering")
            self.assertTrue(await worker.step(now=start))
        self.assertEqual(provider.find_targets.await_count, 1)
        self.assertFalse(await worker.step(now=at(1, 12, 14)))
        self.assertTrue(await worker.step(now=at(1, 12, 15)))
        self.assertEqual(provider.find_targets.await_count, 2)
        self.assertTrue(await worker.step(now=at(4, 6, 14)))
        self.assertTrue(await worker.step(now=at(4, 9, 14)))
        self.assertFalse(await worker.step(now=at(4, 9, 15)))
        self.assertFalse(await worker.step(now=at(4, 21, 13)))
        provider.find_targets.return_value = Lookup(MatchStatus.MATCHED, ("target",))
        close = at(4, 21, 14)
        self.assertTrue(await worker.step(now=close))
        for _ in range(5):
            await worker.step(now=close)
        self.assertEqual(provider.find_targets.await_count, 5)
        self.assertEqual(len(provider.calls), 4)
        for recording_id in ids:
            self.assertEqual(store.get("owner", recording_id)["status"], "archived")


    def test_waiting_archive_recheck_uses_corrected_outbound_number(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            provider = Remote()
            provider.lookup_by_callee = True
            provider.find_targets = AsyncMock(return_value=Lookup(MatchStatus.MATCHED, ("target",)))
            remote = RemoteDelivery(store, provider)
            item = store.submit("owner", "corrected", {
                "flow": "remote", "archive_instance": provider.id, "call": {"callee": "10001"}, "options": {},
            }, sample_audio(), "audio/wav")
            store.advance(store.claim(now=100), "needs_input", retry_at=101, error="archive_target_missing",
                          occurred_at=100)
            with store.transaction() as db:
                db.execute("""INSERT INTO outbound_calls(owner,source,external_id,call_id,metadata,complete)
                              VALUES ('owner','source','call',?,'{"callee":"10002"}',1)""", (item["call_id"],))
            self.assertTrue(asyncio.run(remote.recheck_waiting(101)))
            provider.find_targets.assert_awaited_once_with({"callee": "10002"})

    def test_changed_routing_rebuilds_unwritten_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_changed_routing(Path(directory), written=False))

    def test_changed_routing_stops_after_a_write_has_started(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_changed_routing(Path(directory), written=True))

    def test_pre_upgrade_plan_is_checked_before_continuing_a_partial_write(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_changed_routing(Path(directory), written=True, legacy=True))

    async def exercise_changed_routing(self, root, *, written, legacy=False):
        store = ArchiveStore(root / "archive.sqlite")
        provider = Remote()
        provider.lookup_by_callee = True
        provider.crash = False
        provider.find_targets = AsyncMock(side_effect=lambda fields: Lookup(MatchStatus.MATCHED, (fields["callee"],)))
        remote = RemoteDelivery(store, provider)
        item = store.submit("owner", "recording", {
            "flow": "remote", "archive_instance": provider.id,
            "call": {"callee": "10002" if legacy else "10001"}, "options": {},
        }, sample_audio(), "audio/wav")
        with store.transaction() as db:
            db.execute("""INSERT INTO outbound_calls(owner,source,external_id,call_id,metadata,complete)
                          VALUES ('owner','source','call',?,'{"callee":"10001"}',1)""", (item["call_id"],))
        claimed = store.claim(now=100)
        self.assertTrue(await remote.prepare(claimed, now=100))
        store.advance(claimed, "delivering", transcript='{"text":"result"}')
        if written:
            await remote.step(store.claim(now=101), now=101)
            self.assertEqual([target for target, _ in provider.calls], ["10001"])
        with store.transaction() as db:
            db.execute("UPDATE outbound_calls SET metadata=? WHERE call_id=?",
                       ('{"callee":"10002"}', item["call_id"]))
            if legacy:
                db.execute("UPDATE remote_attachments SET routing_key=NULL WHERE recording_id=?", (claimed["id"],))
        await remote.step(store.claim(now=102), now=102)
        if written:
            self.assertEqual([target for target, _ in provider.calls], ["10001"])
            self.assertEqual(store.get("owner", item["recording_id"])["status"], "needs_review")
            self.assertEqual([row["status"] for row in remote.attachments(claimed)], ["confirmed", "pending"])
        else:
            self.assertEqual([target for target, _ in provider.calls], ["10002"])
            self.assertEqual({row["target"] for row in remote.attachments(claimed)}, {"10002"})

    def test_generic_archive_rechecks_without_a_callee(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_generic_recheck(Path(directory)))

    async def exercise_generic_recheck(self, root):
        store = ArchiveStore(root / "archive.sqlite")
        provider = Remote()
        provider.find_targets = AsyncMock(side_effect=[
            Lookup(MatchStatus.NEEDS_INPUT), Lookup(MatchStatus.MATCHED, ("record-owner",)),
        ])
        delivery = RemoteDelivery(store, provider)
        item = store.submit("owner", "generic", {
            "flow": "remote", "archive_instance": provider.id, "call": {"external_call_id": "case"}, "options": {},
        }, sample_audio(), "audio/wav")
        self.assertFalse(await delivery.prepare(store.claim(now=100), now=100))
        self.assertFalse(await delivery.recheck_waiting(100 + 3 * 3600 - 1))
        self.assertTrue(await delivery.recheck_waiting(100 + 3 * 3600))
        self.assertEqual(provider.find_targets.await_count, 2)
        self.assertEqual(store.get("owner", item["recording_id"])["status"], "queued")

    def test_manual_recheck_is_coalesced_for_the_same_callee(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_manual_recheck(Path(directory)))

    async def exercise_manual_recheck(self, root):
        store = ArchiveStore(root / "archive.sqlite")
        provider = Remote()
        provider.lookup_by_callee = True
        provider.find_targets = AsyncMock(return_value=Lookup(MatchStatus.NEEDS_INPUT))
        worker = Workflow(store, None, LocalArchive(root / "attachments"), providers={provider.id: provider})
        for n in range(2):
            item = store.submit("owner", f"manual-{n}", {
                "flow": "remote", "archive_instance": provider.id, "call": {"callee": "10002"}, "options": {},
            }, sample_audio(), "audio/wav")
            store.advance(store.claim(now=100), "delivering", transcript='{"text":"result"}')
            self.assertTrue(await worker.step(now=100))
        recording_id = item["recording_id"]
        first = store.request_retry("owner", recording_id, actor="operator")
        self.assertEqual(store.request_retry("owner", recording_id, actor="operator"), first)
        self.assertTrue(await worker.step(now=101))
        self.assertFalse(await worker.step(now=102))
        self.assertEqual(provider.find_targets.await_count, 2)

    def test_crash_after_write_uses_verification_and_keeps_per_target_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise(Path(directory) / "test.sqlite"))

    async def exercise(self, database):
        store = ArchiveStore(database)
        report = store.submit("owner", "test", {
            "flow": "remote", "archive_instance": "directory-v1", "call": {"callee": "10002"}, "options": {},
        }, sample_audio(), "audio/wav")
        store.advance(store.claim(now=100), "transcribing", job_id="job1")
        store.advance(store.claim(now=100), "acknowledging", transcript='{"text":"result"}')
        store.advance(store.claim(now=100), "delivering")

        provider = Remote()
        delivery = RemoteDelivery(store, provider)
        claim = store.claim(now=100, lease=1)
        with self.assertRaises(Crashed):
            await delivery.step(claim, now=100)
        self.assertEqual(len(provider.calls), 1)
        self.assertEqual(len(provider.saved), 1)

        store = ArchiveStore(database)
        delivery = RemoteDelivery(store, provider)
        for tick in range(101, 110):
            claim = store.claim(now=tick)
            if claim is None:
                break
            await delivery.step(claim, now=tick)
        state = store.get("owner", report["recording_id"])
        self.assertEqual(state["status"], "archived")
        self.assertEqual(len(provider.calls), 4)
        self.assertEqual(set(state["receipts"]), {"one", "two"})
        self.assertEqual(set(state["receipts"]["one"]), {"audio", "transcript"})



    def test_unverifiable_write_is_not_repeated(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_unknown(Path(directory) / "test.sqlite"))

    async def exercise_unknown(self, database):
        store = ArchiveStore(database)
        item = store.submit("owner", "test", {
            "flow": "remote", "archive_instance": "directory-v1", "call": {"callee": "10002"}, "options": {},
        }, sample_audio(), "audio/wav")
        store.advance(store.claim(now=100), "transcribing", job_id="job1")
        store.advance(store.claim(now=100), "acknowledging", transcript='{"text":"result"}')
        store.advance(store.claim(now=100), "delivering")
        provider = Remote()
        provider.supports_verification = False
        delivery = RemoteDelivery(store, provider)
        with self.assertRaises(Crashed):
            await delivery.step(store.claim(now=100, lease=1), now=100)
        for tick in range(101, 106):
            claim = store.claim(now=tick)
            if claim is None:
                break
            await delivery.step(claim, now=tick)
        details = store.get("owner", item["recording_id"])
        self.assertEqual(details["status"], "needs_review")
        self.assertEqual(details["error"], "archive_attachment_unknown")
        self.assertEqual(len(provider.calls), 4)
        self.assertEqual(details["receipts"]["one"]["audio"]["status"], "unknown")
        self.assertEqual(details["receipts"]["two"]["transcript"]["status"], "confirmed")




    def test_removed_api_call_rejects_late_recording_even_after_archive_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            metadata = {"flow": "local", "call": {"external_call_id": "call-1"}, "options": {}}
            first = store.submit("owner", "first", metadata, sample_audio(), "audio/wav")
            second = store.submit("owner", "second", metadata, sample_audio(), "audio/wav")
            self.assertEqual(first["call_id"], second["call_id"])
            claimed = store.claim(now=100)
            store.advance(claimed, "archived", occurred_at=100)

            self.assertTrue(ConsoleCalls(store, {}).remove(first["call_id"], "operator"))
            self.assertEqual(store.get("owner", first["recording_id"])["status"], "archived")
            with self.assertRaises(SubmissionConflict):
                store.submit("owner", "late", metadata, sample_audio(), "audio/wav")
            with store.transaction() as db:
                store.retire_call(db, first["call_id"], include_archived=True)
            with self.assertRaises(SubmissionConflict):
                store.submit("owner", "later", metadata, sample_audio(), "audio/wav")
            self.assertEqual(store.submit("another", "later", metadata, sample_audio(), "audio/wav")["status"], "queued")

    def test_known_bad_outbound_recording_blocks_healthy_sibling(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            first = store.submit("owner", "healthy", {
                "flow": "remote", "archive_instance": "directory-v1",
                "call": {"callee": "10002"}, "options": {},
            }, sample_audio(), "audio/wav")
            with store.transaction() as db:
                db.execute("""INSERT INTO outbound_calls (owner,source,external_id,call_id,metadata,complete)
                              VALUES ('owner','source','test',?,'{}',1)""", (first["call_id"],))
                db.execute("""INSERT INTO outbound_recordings (call_id,external_id,reference,metadata,error)
                              VALUES (?,'bad','reference','{}','invalid_audio')""", (first["call_id"],))
            self.assertIsNone(store.claim(now=100))
            with store.transaction() as db:
                db.execute("DELETE FROM outbound_recordings WHERE call_id=?", (first["call_id"],))
            self.assertEqual(store.claim(now=100)["id"], first["recording_id"])

    def test_unknown_write_is_automatically_verified_without_idempotency(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_verifiable_unknown(Path(directory)))

    async def exercise_verifiable_unknown(self, root):
        store = ArchiveStore(root / "archive.sqlite")
        provider = Remote()
        provider.find_targets = AsyncMock(return_value=Lookup(MatchStatus.MATCHED, ("target",)))
        provider.put_attachment = AsyncMock(side_effect=[
            WriteResult(WriteStatus.CONFIRMED, "audio"), WriteResult(WriteStatus.UNKNOWN),
        ])
        provider.verify_attachment = AsyncMock(return_value=VerifyResult(VerifyStatus.CONFIRMED, "text"))
        item = store.submit("owner", "verify-write", {
            "flow": "remote", "archive_instance": provider.id, "call": None, "options": {},
        }, sample_audio(), "audio/wav")
        store.advance(store.claim(now=100), "delivering", transcript='{"text":"result"}')
        worker = Workflow(store, None, LocalArchive(root / "attachments"), providers={provider.id: provider})
        for tick in (100, 101, 161):
            self.assertTrue(await worker.step(now=tick))
        details = store.get("owner", item["recording_id"])
        self.assertEqual(details["status"], "archived")
        self.assertEqual(details["receipts"]["target"]["transcript"]["receipt"], "text")
        self.assertEqual(provider.put_attachment.await_count, 2)

    def test_call_view_and_notifications_follow_archive_recovery_across_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_call_recovery(Path(directory)))

    async def exercise_call_recovery(self, root):
        database = root / "archive.sqlite"
        store = ArchiveStore(database)
        provider = Remote()
        provider.find_targets = AsyncMock(return_value=Lookup(MatchStatus.NEEDS_INPUT))
        provider.put_attachment = AsyncMock(side_effect=[
            WriteResult(WriteStatus.CONFIRMED, "audio"),
            WriteResult(WriteStatus.UNKNOWN),
            WriteResult(WriteStatus.CONFIRMED, "second-audio"),
            WriteResult(WriteStatus.CONFIRMED, "second-text"),
        ])
        provider.verify_attachment = AsyncMock(return_value=VerifyResult(VerifyStatus.INCONCLUSIVE))
        metadata = {"flow": "remote", "archive_instance": provider.id,
                    "call": {"callee": "10002", "started_at": "2026-11-24T12:00:00Z"}, "options": {}}
        first = store.submit("owner", "first", metadata, sample_audio(), "audio/wav")
        call_id = first["call_id"]
        with store.transaction() as db:
            db.execute("""INSERT INTO outbound_calls (owner,source,external_id,call_id,metadata,complete)
                          VALUES ('owner','demo','call',?, ?,0)""", (call_id, json.dumps(metadata["call"])))
        console = ConsoleCalls(store, {})
        worker = Workflow(store, None, LocalArchive(root / "attachments"), providers={provider.id: provider})
        store.advance(store.claim(now=100), "delivering", transcript='{"text":"first"}')
        self.assertTrue(await worker.step(now=100))
        self.assertEqual(console.pending_detail(call_id)["status"], "waiting_applicant")
        with store.transaction() as db:
            self.assertEqual([i["category"] for i in collect_issues(db, "owner")[1]], ["follow_up"])

        provider.find_targets.return_value = Lookup(MatchStatus.MATCHED, ("target",))
        self.assertTrue(console.retry(call_id, "operator"))
        for tick in (101, 102, 162):
            self.assertTrue(await worker.step(now=tick))
        detail = console.pending_detail(call_id)
        self.assertEqual(detail["status"], "attention")
        self.assertEqual(detail["recordings"][0]["targets"], [
            {"apply_id": "target", "audio": "已归档", "text": "待核对"},
        ])
        with store.transaction() as db:
            self.assertEqual([i["reason"] for i in collect_issues(db, "owner")[1]], ["archive_attachment_unknown"])
        self.assertEqual(provider.put_attachment.await_count, 2)

        store = ArchiveStore(database)
        console = ConsoleCalls(store, {})
        worker = Workflow(store, None, LocalArchive(root / "attachments"), providers={provider.id: provider})
        second = store.submit("owner", "second", metadata, sample_audio(), "audio/wav", linked_call=call_id)
        store.advance(store.claim(now=163), "delivering", transcript='{"text":"second"}')
        with store.transaction() as db:
            db.execute("UPDATE outbound_calls SET complete=1 WHERE call_id=?", (call_id,))
        provider.verify_attachment.return_value = VerifyResult(VerifyStatus.CONFIRMED, "first-text")
        self.assertTrue(console.retry(call_id, "operator"))
        for tick in (163, 164, 165):
            self.assertTrue(await worker.step(now=tick))
        self.assertEqual(provider.put_attachment.await_count, 4)
        self.assertEqual(provider.verify_attachment.await_count, 2)
        self.assertEqual(store.get("owner", second["recording_id"])["status"], "archived")
        self.assertIsNone(console.pending_detail(call_id))
        self.assertEqual(len(RecentArchives(store).files(call_id)), 2)
        with store.transaction() as db:
            self.assertEqual(collect_issues(db, "owner"), (0, []))
            closed_at = db.execute("""SELECT max(e.occurred_at)
                FROM recording_events e JOIN recordings r ON r.id=e.recording_id
                WHERE r.call_id=? AND e.status='archived'""", (call_id,)).fetchone()[0]
        store.purge_archive_cache(now=closed_at + 86400)
        self.assertEqual(RecentArchives(store).files(call_id), [])

    def test_attachment_stops_after_four_confirmed_failures_without_replaying_other_files(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_attachment_attempt_limit(Path(directory)))

    async def exercise_attachment_attempt_limit(self, root):
        store = ArchiveStore(root / "archive.sqlite")
        provider = Remote()
        provider.find_targets = AsyncMock(return_value=Lookup(MatchStatus.MATCHED, ("target",)))
        provider.put_attachment = AsyncMock(side_effect=[
            WriteResult(WriteStatus.RETRYABLE), WriteResult(WriteStatus.CONFIRMED, "text"),
            *[WriteResult(WriteStatus.RETRYABLE)] * 3, WriteResult(WriteStatus.CONFIRMED, "audio"),
        ])
        recording = store.submit("owner", "attachment-limit", {
            "flow": "remote", "archive_instance": provider.id, "call": {"callee": "10002"}, "options": {},
        }, sample_audio(), "audio/wav")
        recording_id = recording["recording_id"]
        store.advance(store.claim(now=100), "delivering", transcript='{"text":"result"}')
        worker = Workflow(store, None, LocalArchive(root / "attachments"), providers={provider.id: provider})
        for now in (100, 400, 700, 1000):
            await worker.run_due(now=now)
        self.assertEqual(store.get("owner", recording_id)["status"], "needs_review")
        await worker.run_due(now=1300)
        self.assertEqual(provider.put_attachment.await_count, 5)
        store.request_retry("owner", recording_id, actor="operator")
        await worker.run_due(now=1300)
        self.assertEqual(store.get("owner", recording_id)["status"], "archived")
        self.assertEqual([call.args[1].kind for call in provider.put_attachment.await_args_list],
                         ["audio", "transcript", "audio", "audio", "audio", "audio"])

    def test_explicit_upload_rate_limit_retries_same_attachment(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_rate_limit(Path(directory)))

    async def exercise_rate_limit(self, root):
        store = ArchiveStore(root / "archive.sqlite")
        provider = Remote()
        provider.find_targets = AsyncMock(return_value=Lookup(MatchStatus.MATCHED, ("target",)))
        provider.put_attachment = AsyncMock(side_effect=[
            WriteResult(WriteStatus.RETRYABLE), WriteResult(WriteStatus.CONFIRMED, "text"),
            WriteResult(WriteStatus.RETRYABLE), WriteResult(WriteStatus.CONFIRMED, "audio"),
        ])
        item = store.submit("owner", "retry-write", {
            "flow": "remote", "archive_instance": provider.id, "call": {"callee": "10002"}, "options": {},
        }, sample_audio(), "audio/wav")
        store.advance(store.claim(now=100), "delivering", transcript='{"text":"result"}')
        worker = Workflow(store, None, LocalArchive(root / "attachments"), providers={provider.id: provider})
        self.assertTrue(await worker.step(now=100))
        self.assertTrue(await worker.step(now=101))
        details = store.get("owner", item["recording_id"])
        self.assertEqual(details["receipts"]["target"]["transcript"]["status"], "confirmed")
        self.assertFalse(await worker.step(now=159))
        self.assertTrue(await worker.step(now=160))
        self.assertFalse(await worker.step(now=161))
        self.assertTrue(await worker.step(now=220))
        self.assertEqual(store.get("owner", item["recording_id"])["status"], "archived")
        calls = provider.put_attachment.await_args_list
        self.assertEqual([call.args[1].kind for call in calls], ["audio", "transcript", "audio", "audio"])
        self.assertEqual(calls[0].args[2], calls[2].args[2])
        self.assertEqual(calls[0].args[2], calls[3].args[2])

    def test_configured_remote_flow_resumes_after_missing_archive_is_fixed(self):
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(self.exercise_flow(Path(directory)))

    async def exercise_flow(self, root):
        db = root / "archive.sqlite"
        credentials = {"operator-token": ("operator", False)}
        backend = create_backend(db, credentials, external_archive_id="directory-v1")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=backend), base_url="http://archive",
                                     headers={"Authorization": "Bearer operator-token"}) as client:
            response = await client.post("/v1/archive/recordings", files={
                "audio": ("call.wav", sample_audio(), "audio/wav"),
                "metadata": (None, json.dumps({"request_id": "remote-1", "flow": "remote",
                                               "call": {"callee": "10002"}})),
            })
            self.assertEqual(response.status_code, 202)
            recording_id = response.json()["recording_id"]
            url = f"/v1/archive/recordings/{recording_id}"
            store = ArchiveStore(db)
            upstream_db = root / "transcription.sqlite"
            upstream = create_transcription(upstream_db, {"service-token": ("archive", True)})
            provider = Remote()
            provider.crash = False
            provider.find_targets = AsyncMock(side_effect=[
                Lookup(MatchStatus.NEEDS_INPUT), Lookup(MatchStatus.MATCHED, ("target",)),
            ])
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=upstream),
                                         base_url="http://transcribe",
                                         headers={"Authorization": "Bearer service-token"}) as transcription:
                workflow = Workflow(store, transcription, LocalArchive(root / "files"),
                                    providers={"directory-v1": provider})
                self.assertTrue(await workflow.step(now=100))
                jobs = JobStore(upstream_db)
                self.assertFalse(process_one(jobs, DemoEngine()))
                missing = (await client.get(url)).json()
                self.assertEqual(missing["status"], "needs_input")
                self.assertEqual(missing["archive_instance"], "directory-v1")
                self.assertTrue(missing["can_retry"])
                self.assertEqual((await client.post(url + "/retry")).status_code, 202)
                self.assertTrue(await workflow.step(now=101))
                self.assertTrue(process_one(jobs, DemoEngine()))
                for tick in range(102, 106):
                    self.assertTrue(await workflow.step(now=tick))
                self.assertEqual((await client.get(url)).json()["status"], "archived")
                self.assertEqual(len(provider.calls), 2)
