"""Browser accounts backed by the existing scrypt password verifiers."""

import hashlib
import hmac
from configparser import ConfigParser, Error
from dataclasses import dataclass, field
from pathlib import Path


ROLES = frozenset({"admin", "ops", "external"})


@dataclass(frozen=True)
class PasswordAccount:
    owner: str
    role: str
    salt: bytes = field(repr=False)
    verifier: bytes = field(repr=False)

    @property
    def session_key(self):
        identity = f"password:{self.owner}:{self.role}:{self.salt.hex()}:{self.verifier.hex()}"
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    def matches(self, password: bytes) -> bool:
        candidate = hashlib.scrypt(password, salt=self.salt, n=2**14, r=8, p=1)
        return hmac.compare_digest(self.verifier, candidate)


def load_password_accounts(path: Path) -> tuple[PasswordAccount, ...]:
    parser = ConfigParser(interpolation=None)
    try:
        with path.open(encoding="utf-8-sig") as stream:
            parser.read_file(stream)
        accounts = []
        for owner, entry in parser.items("api_users"):
            role, salt_hex, verifier_hex = entry.split(":")
            salt, verifier = bytes.fromhex(salt_hex), bytes.fromhex(verifier_hex)
            if role not in ROLES or len(salt) != 16 or len(verifier) != 64 or not owner:
                raise ValueError
            accounts.append(PasswordAccount(owner, role, salt, verifier))
        if not accounts:
            raise ValueError
    except (OSError, Error, ValueError):
        raise ValueError("Unable to load browser accounts from the configured [api_users] section.") from None
    return tuple(accounts)
