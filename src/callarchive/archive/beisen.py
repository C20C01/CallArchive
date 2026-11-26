"""Adapter for the Beisen recruiting archive."""

import asyncio
import base64
import os
import time
from pathlib import PurePosixPath

import httpx
from pydantic import Field, ValidationError

from ..configuration import Section
from .provider import Lookup, LookupUnavailable, MatchStatus, VerifyResult, VerifyStatus, WriteResult, WriteStatus

BASE_URL = "https://openapi.italent.cn"
RECRUIT = "/RecruitV6/api/v1/"


class BeisenOptions(Section):
    key_env: str = Field(default="BEISEN_CONNECTOR_KEY", pattern=r"^[A-Z_][A-Z0-9_]*$")
    secret_env: str = Field(default="BEISEN_CONNECTOR_SECRET", pattern=r"^[A-Z_][A-Z0-9_]*$")
    requests_per_second: int = Field(default=10, ge=1, le=10)


def create_archive(resources, name, options):
    try:
        settings = BeisenOptions.model_validate(options)
    except ValidationError:
        raise ValueError("Invalid Beisen adapter options.") from None
    key = os.getenv(settings.key_env)
    secret = os.getenv(settings.secret_env)
    if not key or not secret:
        raise ValueError("Set the configured Beisen credentials.")
    client = httpx.AsyncClient(base_url=BASE_URL, timeout=20, trust_env=False)
    resources.push_async_callback(client.aclose)
    return BeisenArchive(name, client, key, secret, requests_per_second=settings.requests_per_second)


class BeisenResponseError(ValueError):
    def __init__(self, *, retryable):
        self.retryable = retryable


class BeisenArchive:
    lookup_by_callee = True
    supports_idempotency = False
    supports_verification = False

    def __init__(self, instance_id, client, key, secret, *, requests_per_second):
        self.id = instance_id
        self.client = client
        self.credentials = {"grant_type": "client_credentials", "app_key": key, "app_secret": secret}
        self.token = ""
        self.refresh_at = 0
        self.interval = 1 / requests_per_second
        self.next_request = 0
        self.request_lock = asyncio.Lock()

    async def post(self, endpoint, body, *, token=None):
        async with self.request_lock:
            delay = self.next_request - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self.next_request = time.monotonic() + self.interval
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        response = await self.client.post(endpoint, json=body, headers=headers, follow_redirects=False)
        if token and response.status_code == 401:
            self.refresh_at = 0
        return response

    async def access_token(self):
        if time.monotonic() >= self.refresh_at:
            response = await self.post("/token", self.credentials)
            payload = response.raise_for_status().json()
            token = payload.get("access_token") if isinstance(payload, dict) else None
            if not isinstance(token, str) or not token:
                raise BeisenResponseError(retryable=False)
            expiry = payload.get("expires_in", 1200)
            expiry = expiry if type(expiry) is int and expiry > 0 else 1200
            self.token = token
            self.refresh_at = time.monotonic() + max(1, expiry - min(60, expiry // 2))
        return self.token

    async def lookup(self, endpoint, body):
        try:
            token = await self.access_token()
            response = await self.post(RECRUIT + endpoint, body, token=token)
            if response.status_code == 401:
                token = await self.access_token()
                response = await self.post(RECRUIT + endpoint, body, token=token)
            payload = response.raise_for_status().json()
            if not isinstance(payload, dict) or str(payload.get("code")) not in {"0", "200"}:
                code = payload.get("code") if isinstance(payload, dict) else None
                raise BeisenResponseError(retryable=str(code) == "429" or type(code) is int and code >= 500)
            if not isinstance(payload.get("data"), list):
                raise BeisenResponseError(retryable=False)
            return payload["data"]
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            raise LookupUnavailable(retryable=status in {408, 429} or status >= 500) from None
        except httpx.RequestError:
            raise LookupUnavailable(retryable=True) from None
        except BeisenResponseError as error:
            raise LookupUnavailable(retryable=error.retryable) from None
        except ValueError:
            raise LookupUnavailable(retryable=False) from None

    @staticmethod
    def valid_id(value):
        return (isinstance(value, str) and bool(value.strip()) and len(value) <= 256
                or type(value) is int and value > 0)

    async def find_targets(self, fields):
        callee = fields.get("callee")
        if not isinstance(callee, str) or not callee.strip():
            return Lookup(MatchStatus.NEEDS_INPUT)
        applicant_ids = await self.lookup("Applicant/GetApplicantIds", {"mobile": callee.strip()})
        if not applicant_ids:
            return Lookup(MatchStatus.NEEDS_INPUT)
        if any(not self.valid_id(value) for value in applicant_ids):
            raise LookupUnavailable(retryable=False)
        applicants = {str(value) for value in applicant_ids}
        records = await self.lookup("Apply/GetApplyListByApplicantId", {
            "applicantIds": applicant_ids, "fieldNames": ["Status"],
        })
        targets = []
        for item in records:
            if (not isinstance(item, dict) or str(item.get("applicantId")) not in applicants
                    or not self.valid_id(item.get("applyId"))):
                return Lookup(MatchStatus.CONFLICT)
            targets.append(str(item["applyId"]))
        if not targets:
            return Lookup(MatchStatus.NEEDS_INPUT)
        return Lookup(MatchStatus.MATCHED, tuple(dict.fromkeys(targets)))

    async def put_attachment(self, target, attachment, request_id):
        extension = PurePosixPath(attachment.name).suffix.lower().lstrip(".")
        if extension not in {"txt", "mp3", "wav"} or not 0 < len(attachment.content) <= 100_000_000:
            return WriteResult(WriteStatus.REJECTED)
        try:
            token = await self.access_token()
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            retryable = status in {408, 429} or status >= 500
            return WriteResult(WriteStatus.RETRYABLE if retryable else WriteStatus.REJECTED)
        except httpx.RequestError:
            return WriteResult(WriteStatus.RETRYABLE)
        except ValueError:
            return WriteResult(WriteStatus.REJECTED)
        body = {
            "applyId": target,
            "fileExtension": extension,
            "fileName": attachment.name,
            "fileContent": base64.b64encode(attachment.content).decode("ascii"),
        }
        try:
            response = await self.post(RECRUIT + "Applicant/AddAdditionalMaterialByApplyId", body, token=token)
            if response.status_code == 429:
                return WriteResult(WriteStatus.RETRYABLE)
            if response.status_code in {400, 401, 403, 404, 413, 415, 422}:
                return WriteResult(WriteStatus.REJECTED)
            if response.is_success:
                payload = response.json()
                if isinstance(payload, dict) and str(payload.get("code")) == "429":
                    return WriteResult(WriteStatus.RETRYABLE)
                if isinstance(payload, dict) and str(payload.get("code")) in {"0", "200"}:
                    if payload.get("data") is True:
                        return WriteResult(WriteStatus.CONFIRMED)
                    if payload.get("data") is False:
                        return WriteResult(WriteStatus.REJECTED)
        except (httpx.HTTPError, ValueError):
            pass
        return WriteResult(WriteStatus.UNKNOWN)

    async def verify_attachment(self, target, attachment, request_id):
        return VerifyResult(VerifyStatus.INCONCLUSIVE)
