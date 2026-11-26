from datetime import datetime, timedelta, timezone
import json
import unittest

import httpx

from callarchive.outbound.qianke import QiankeSource, call_identity


class QiankeSourceTest(unittest.TestCase):
    def test_recheck_matches_same_call_and_refreshes_recording(self):
        start = datetime(2026, 11, 24, tzinfo=timezone.utc)
        call = {"talkId": 0, "callTime": int(start.timestamp() * 1000), "talkLen": "4250",
                "type": 1, "status": 1, "caller": "10001", "called": "10002"}
        paths = []

        def respond(request):
            paths.append(request.url.path)
            path = request.url.path
            if path.endswith("access_token"):
                return httpx.Response(200, json={"access_token": "test-token"})
            if path.endswith("getCallRecordList"):
                body = json.loads(request.content)
                assert request.url.params["access_token"] == "test-token"
                if paths.count(path) == 1:
                    return httpx.Response(200, json={"result": 0, "data": {
                        "records": [call, {**call, "status": 2}], "last": {"lastId": 4, "lastKey": 3}}})
                if paths.count(path) == 2:
                    assert body["last"] == {"lastId": 4, "lastKey": 3}
                    return httpx.Response(200, json={"result": 0, "data": {"records": [], "last": None}})
                part = {"id": "segment", "name": "sample.wav"}
                match = {**call, "talkId": 7, "audioInfos": [part]}
                return httpx.Response(200, json={"result": 0, "data": {
                    "records": [{**match, "called": "10003"}, match], "last": None}})
            if path.endswith("getFileUrl"):
                assert json.loads(request.content) == {"infos": [{"id": "segment", "name": "sample.wav"}]}
                return httpx.Response(200, json={"result": 0, "data": {"infos": [
                    {"id": "segment", "url": "https://storage.test/sample"}]}})
            assert str(request.url) == "https://storage.test/sample"
            assert not request.headers.get("Authorization")
            return httpx.Response(200, content=b"recording")

        with httpx.Client(base_url="https://api.test", transport=httpx.MockTransport(respond)) as client:
            source = QiankeSource(client, "app", "company", "secret", ("storage.test",))
            page = source.list_calls(start, start + timedelta(minutes=1), None, 10)
            first, = page.calls
            self.assertFalse(first.recordings_complete)
            self.assertEqual(first.call_duration_ms, 4250)
            self.assertEqual(source.list_calls(start, start + timedelta(minutes=1), page.cursor, 10).calls, [])
            recordings, complete = source.get_recordings(first.external_call_id)
            self.assertTrue(complete)
            with source.open_recording(recordings[0]) as stream:
                self.assertEqual(stream.read(), b"recording")
            self.assertEqual(paths.count("/cgi-bin/oauth/access_token"), 1)

    def test_grouped_lookup_paginates_and_resolves_migrated_call_aliases(self):
        start = datetime(2026, 11, 24, tzinfo=timezone.utc)
        stamp = int(start.timestamp() * 1000)
        calls = [
            {"talkId": 7, "callTime": stamp, "type": 1, "status": 1, "caller": "10001", "called": "10002",
             "audioInfos": [{"id": "first", "name": "first.wav"}]},
            {"talkId": 8, "callTime": stamp + 1000, "type": 1, "status": 1, "caller": "10003", "called": "10004",
             "audioInfos": [{"id": "second", "name": "second.wav"}]},
        ]
        requested = [call_identity(stamp, "10001", "10002"), call_identity(stamp + 1000, None, "10004"), "8"]
        query_count = 0

        def respond(request):
            nonlocal query_count
            if request.url.path.endswith("access_token"):
                return httpx.Response(200, json={"access_token": "test-token"})
            self.assertTrue(request.url.path.endswith("getCallRecordList"))
            body = json.loads(request.content)
            self.assertEqual(body["filter"]["timeRange"], {"stime": stamp, "etime": stamp + 60_000 - 1})
            cursor = {"lastId": 4, "lastKey": 3}
            self.assertEqual(body.get("last"), cursor if query_count else None)
            data = {"records": [calls[query_count]], "last": None if query_count else cursor}
            query_count += 1
            return httpx.Response(200, json={"result": 0, "data": data})

        with httpx.Client(base_url="https://api.test", transport=httpx.MockTransport(respond)) as client:
            source = QiankeSource(client, "app", "company", "secret", ("storage.test",))
            results = source.get_recordings_in_window(requested, start, start + timedelta(minutes=1))
        self.assertEqual(query_count, 2)
        self.assertEqual({key: (files[0].id, complete) for key, (files, complete) in results.items()},
                         dict(zip(requested, [("first", True), ("second", True), ("second", True)])))

    def test_manual_redownload_refreshes_a_cached_url(self):
        visited = []

        def respond(request):
            visited.append(request.url.path)
            if request.url.path.endswith("access_token"):
                return httpx.Response(200, json={"access_token": "test-token"})
            if request.url.path.endswith("getFileUrl"):
                return httpx.Response(200, json={"result": 0, "data": {"infos": [
                    {"id": "segment", "url": "https://storage.test/fresh"}]}})
            self.assertEqual(request.url.path, "/fresh")
            return httpx.Response(200, content=b"fresh audio")

        from callarchive.outbound import RecordingRef

        with httpx.Client(base_url="https://api.test", transport=httpx.MockTransport(respond)) as client:
            source = QiankeSource(client, "app", "company", "secret", ("storage.test",))
            recording = RecordingRef(id="segment", reference="sample.wav")
            source.download_urls[("segment", "sample.wav")] = "https://storage.test/stale"
            with source.reopen_recording(recording) as stream:
                self.assertEqual(stream.read(), b"fresh audio")
        self.assertIn("/open/api/file/getFileUrl", visited)

    def test_download_never_follows_an_unapproved_host(self):
        def respond(request):
            path = request.url.path
            if path.endswith("access_token"):
                return httpx.Response(200, json={"access_token": "test-token"})
            if path.endswith("getFileUrl"):
                return httpx.Response(200, json={"result": 0, "data": {"infos": [
                    {"id": "segment", "url": "https://unapproved.test/sample"}]}})
            self.fail("A request left the allowed endpoints")

        from callarchive.outbound import RecordingRef

        with httpx.Client(base_url="https://api.test", transport=httpx.MockTransport(respond)) as client:
            source = QiankeSource(client, "app", "company", "secret", ("storage.test",))
            ref = RecordingRef(id="segment", reference="sample.wav")
            with self.assertRaises(OSError), source.open_recording(ref):
                self.fail("An unauthorized host was allowed")
