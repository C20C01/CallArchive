
import asyncio
import json
import tempfile
import unittest
from pathlib import Path

import httpx

from callarchive.backend.api import create_app
from test_service import sample_audio


class IntakeTest(unittest.TestCase):
    def test_call_and_recording_intake(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "archive.sqlite"
            asyncio.run(self.exercise_intake(database))

    async def exercise_intake(self, database):
        credentials = {"operator-token": ("operator", False), "other-token": ("other", False)}
        owner = {"Authorization": "Bearer operator-token"}
        other = {"Authorization": "Bearer other-token"}
        app = create_app(database, credentials)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            root = "/v1/archive/recordings"
            call = {
                "external_call_id": "call-1", "started_at": "2026-11-24T09:43:00+08:00",
                "caller": "10001", "callee": "10002", "call_duration_ms": 30000,
            }

            async def submit(request_id, audio, headers=owner):
                return await client.post(root, headers=headers, files={
                    "audio": ("clip.wav", audio, "audio/wav"),
                    "metadata": (None, json.dumps({"request_id": request_id, "flow": "local", "call": call})),
                })

            first = await submit("part-1", sample_audio())
            self.assertEqual(first.status_code, 202, first.text)
            item = first.json()
            self.assertEqual(item["status"], "queued")
            self.assertEqual((await submit("part-1", sample_audio())).json(), item)
            self.assertEqual((await submit("part-1", sample_audio(1))).status_code, 409)
            self.assertEqual((await submit("part-3", b"invalid")).status_code, 422)
            second = (await submit("part-2", sample_audio(2))).json()
            another_owner = (await submit("part-1", sample_audio(), other)).json()
            self.assertNotEqual(second["recording_id"], item["recording_id"])
            self.assertEqual(second["call_id"], item["call_id"])
            self.assertNotEqual(another_owner["call_id"], item["call_id"])
            path = f'{root}/{item["recording_id"]}'
            self.assertEqual((await client.get(path, headers=other)).status_code, 404)
            details = (await client.get(path, headers=owner)).json()
            self.assertEqual(details["call"]["callee"], "10002")
            self.assertEqual(details["recording_duration_ms"], 100)
            listing = (await client.get(root, headers=owner)).json()["items"]
            self.assertEqual(listing[0]["recording_duration_ms"], 100)
            self.assertEqual((await client.get(path + "/audio", headers=owner)).content, sample_audio())
