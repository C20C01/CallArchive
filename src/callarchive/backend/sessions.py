import hashlib
import hmac
import secrets
import time
from contextlib import closing
from dataclasses import dataclass

from .password_accounts import PasswordAccount
from .store import ArchiveStore


CONSOLE_SESSION = "callarchive_console_session"


def digest(value: str):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Session:
    owner: str
    csrf: str
    expires: float
    role: str | None = None

    def accepts_csrf(self, supplied: str) -> bool:
        return hmac.compare_digest(self.csrf.encode(), supplied.encode())


class SessionStore:
    def __init__(self, store: ArchiveStore, credentials: dict[str, tuple[str, bool]], *,
                 password_accounts: tuple[PasswordAccount, ...] | None = None, lifetime=8 * 3600):
        self.store = store
        self.lifetime = lifetime
        self.password_accounts = password_accounts
        if password_accounts is None:
            self.users = {digest(token): (owner, None) for token, (owner, service) in credentials.items() if not service}
        else:
            self.users = {account.session_key: (account.owner, account.role) for account in password_accounts}
        with closing(store.connect()) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS web_sessions (
                    token_hash TEXT PRIMARY KEY,
                    credential_hash TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    csrf TEXT NOT NULL,
                    expires REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS web_session_expiry ON web_sessions (expires);
            """)

    def login(self, credential: str, *, now=None) -> tuple[str, Session] | None:
        if self.password_accounts is None:
            supplied = digest(credential)
            key = next((expected for expected in self.users if hmac.compare_digest(expected, supplied)), None)
            if key is None:
                return None
        else:
            if len(credential) > 256:
                return None
            password = credential.encode("utf-8")
            matches = [account for account in self.password_accounts if account.matches(password)]
            if len(matches) != 1:
                return None
            key = matches[0].session_key
        owner, role = self.users[key]
        now = time.time() if now is None else now
        token = secrets.token_urlsafe(32)
        session = Session(owner, secrets.token_urlsafe(32), now + self.lifetime, role)
        with self.store.transaction() as db:
            db.execute("DELETE FROM web_sessions WHERE expires<=?", (now,))
            db.execute("INSERT INTO web_sessions VALUES (?, ?, ?, ?, ?)",
                       (digest(token), key, owner, session.csrf, session.expires))
        return token, session

    def get(self, token: str, *, now=None) -> Session | None:
        now = time.time() if now is None else now
        with closing(self.store.connect()) as db:
            row = db.execute("SELECT * FROM web_sessions WHERE token_hash=? AND expires>?",
                             (digest(token), now)).fetchone()
        if row is None:
            return None
        identity = self.users.get(row["credential_hash"])
        if identity is None or identity[0] != row["owner"]:
            return None
        return Session(row["owner"], row["csrf"], row["expires"], identity[1])

    def logout(self, token: str):
        with self.store.transaction() as db:
            db.execute("DELETE FROM web_sessions WHERE token_hash=?", (digest(token),))
