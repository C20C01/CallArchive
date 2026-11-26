
import tempfile
import unittest
from pathlib import Path

from callarchive.backend.sessions import SessionStore
from callarchive.backend.store import ArchiveStore


class SessionTest(unittest.TestCase):
    def test_service_tokens_and_expired_or_revoked_sessions_are_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory) / "archive.sqlite")
            credentials = {
                "person-token": ("operator", False), "service-token": ("worker", True),
            }
            sessions = SessionStore(store, credentials, lifetime=30)
            self.assertIsNone(sessions.login("service-token", now=100))
            self.assertIsNone(sessions.login("wrong-token", now=100))
            token, session = sessions.login("person-token", now=100)
            self.assertEqual(session.owner, "operator")
            self.assertTrue(session.accepts_csrf(session.csrf))
            self.assertFalse(session.accepts_csrf("wrong"))
            self.assertEqual(sessions.get(token, now=129), session)
            self.assertIsNone(sessions.get(token, now=130))
            replacement = SessionStore(store, {"new-token": ("operator", False)}, lifetime=30)
            self.assertIsNone(replacement.get(token, now=129))
            sessions.logout(token)
            self.assertIsNone(sessions.get(token, now=129))
