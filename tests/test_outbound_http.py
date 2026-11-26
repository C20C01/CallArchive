import json
import unittest
from datetime import datetime, timezone

import httpx

from callarchive.outbound import RecordingRef
from callarchive.outbound.http import HTTPSource


class OutboundHTTPTest(unittest.TestCase):
    def test_uses_fixed_endpoints_and_reads_bounded_recording(self):
        calls = []

        def respond(request):
            calls.append((request.url.path, json.loads(request.content)))
            if request.url.path.endswith("calls:find"):
                return httpx.Response(200, json={"calls": [{
                    "external_call_id": "test", "started_at": "2026-11-24T00:00:00Z",
                }], "next_cursor": "cursor"})
            if request.url.path.endswith("recordings:find"):
                return httpx.Response(200, json={"recordings": [{"id": "part-1", "reference": "opaque"}],
                                                 "complete": True})
            return httpx.Response(200, content=b"test")

        with httpx.Client(base_url="http://provider", transport=httpx.MockTransport(respond)) as client:
            source = HTTPSource(client, max_bytes=4)
            start = datetime(2026, 11, 24, tzinfo=timezone.utc)
            end = datetime(2026, 11, 25, tzinfo=timezone.utc)
            page = source.list_calls(start, end, None, 1)
            self.assertEqual(page.cursor, "cursor")
            recordings, complete = source.get_recordings(page.calls[0].external_call_id)
            self.assertTrue(complete)
            with source.open_recording(recordings[0]) as stream:
                self.assertEqual(stream.read(), b"test")
            with self.assertRaises(ValueError):
                with HTTPSource(client, max_bytes=3).open_recording(RecordingRef(id="part-1", reference="opaque")):
                    self.fail("Oversize recording was exposed")
        self.assertEqual(calls[2], ("/v1/outbound-provider/recordings:open",
                                    {"id": "part-1", "reference": "opaque"}))
