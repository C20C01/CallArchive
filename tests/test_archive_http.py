import asyncio
import json
import unittest

import httpx

from callarchive.archive.http import HTTPArchive
from callarchive.archive.provider import Attachment, MatchStatus, VerifyStatus, WriteStatus


class ArchiveHTTPTest(unittest.TestCase):
    def test_gateway_protocol_transports_results_conservatively(self):
        asyncio.run(self.exercise())

    async def exercise(self):
        paths = []

        def respond(request):
            paths.append(request.url.path)
            if request.url.path.endswith(":find"):
                self.assertEqual(json.loads(request.content)["fields"]["callee"], "10002")
                return httpx.Response(200, json={"status": "matched", "targets": ["target"]})
            if request.url.path.endswith(":verify"):
                return httpx.Response(200, json={"status": "confirmed", "receipt": "remote-2"})
            self.assertEqual(request.headers["Idempotency-Key"], "stable")
            self.assertIn(b"content-recording", request.content)
            return httpx.Response(502, text="upstream unavailable")

        async with httpx.AsyncClient(base_url="http://gateway", transport=httpx.MockTransport(respond)) as client:
            adapter = HTTPArchive("default", client, supports_verification=True)
            targets = await adapter.find_targets({"callee": "10002"})
            self.assertEqual(targets.status, MatchStatus.MATCHED)
            self.assertEqual(targets.targets, ("target",))
            attachment = Attachment("audio", "call.wav", "audio/wav", b"content-recording")
            self.assertEqual((await adapter.put_attachment("target", attachment, "stable")).status, WriteStatus.UNKNOWN)
            verified = await adapter.verify_attachment("target", attachment, "stable")
            self.assertEqual(verified.status, VerifyStatus.CONFIRMED)
            self.assertEqual(verified.receipt, "remote-2")
            self.assertEqual(len(paths), 3)
