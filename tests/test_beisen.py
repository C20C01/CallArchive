import base64
import json
import unittest

import httpx

from callarchive.archive.beisen import BeisenArchive
from callarchive.archive.provider import Attachment, Lookup, MatchStatus, VerifyStatus, WriteStatus


class BeisenArchiveTest(unittest.IsolatedAsyncioTestCase):
    async def test_match_all_applications_and_do_not_replay_uncertain_upload(self):
        writes = []

        def respond(request):
            path = request.url.path
            body = json.loads(request.content)
            if path == "/token":
                return httpx.Response(200, json={"access_token": "test-token"})
            assert request.headers["Authorization"] == "Bearer test-token"
            if path.endswith("GetApplicantIds"):
                assert body == {"mobile": "10002"}
                data = [1]
            elif path.endswith("GetApplyListByApplicantId"):
                assert body["applicantIds"] == [1]
                data = [{"applicantId": 1, "applyId": 21},
                        {"applicantId": 1, "applyId": "22"},
                        {"applicantId": 1, "applyId": 21}]
            elif path.endswith("AddAdditionalMaterialByApplyId"):
                writes.append(body)
                if len(writes) == 2:
                    raise httpx.ReadTimeout("Response interrupted")
                data = True
            else:
                self.fail("Unexpected provider endpoint")
            return httpx.Response(200, json={"code": 200, "data": data})

        async with httpx.AsyncClient(base_url="https://archive.test", transport=httpx.MockTransport(respond)) as client:
            archive = BeisenArchive("test", client, "key", "secret", requests_per_second=10)
            self.assertEqual(await archive.find_targets({"callee": "10002"}),
                             Lookup(MatchStatus.MATCHED, ("21", "22")))
            audio = Attachment("audio", "recording.wav", "audio/wav", b"data")
            first = await archive.put_attachment("21", audio, "audio1")
            second = await archive.put_attachment("22", audio, "audio2")
            self.assertEqual(first.status, WriteStatus.CONFIRMED)
            self.assertEqual(second.status, WriteStatus.UNKNOWN)
            self.assertFalse(archive.supports_idempotency)
            self.assertFalse(archive.supports_verification)
            self.assertEqual((await archive.verify_attachment("22", audio, "audio2")).status,
                             VerifyStatus.INCONCLUSIVE)
            self.assertEqual(len(writes), 2)
            self.assertEqual(base64.b64decode(writes[0]["fileContent"]), b"data")
            self.assertEqual(writes[0]["fileName"], "recording.wav")

    async def test_missing_callee_is_not_sent_to_provider(self):
        def respond(request):
            self.fail("Missing callee must be handled locally")

        async with httpx.AsyncClient(base_url="https://archive.test", transport=httpx.MockTransport(respond)) as client:
            archive = BeisenArchive("test", client, "key", "secret", requests_per_second=10)
            self.assertEqual(await archive.find_targets({}), Lookup(MatchStatus.NEEDS_INPUT))
