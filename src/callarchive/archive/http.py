import hashlib
import json

import httpx
from pydantic import BaseModel, Field, ValidationError

from .provider import (
    Attachment, Lookup, LookupUnavailable, MatchStatus, VerifyResult, VerifyStatus, WriteResult, WriteStatus,
)

ROOT = "/v1/archive-provider"


class TargetResponse(BaseModel):
    status: MatchStatus
    targets: list[str] = Field(default_factory=list, max_length=100)


class WriteResponse(BaseModel):
    status: WriteStatus
    receipt: str | None = Field(default=None, max_length=1000)


class VerifyResponse(BaseModel):
    status: VerifyStatus
    receipt: str | None = Field(default=None, max_length=1000)


class HTTPArchive:
    def __init__(self, instance_id: str, client: httpx.AsyncClient, *,
                 supports_idempotency=False, supports_verification=False):
        self.id = instance_id
        self.client = client
        self.supports_idempotency = supports_idempotency
        self.supports_verification = supports_verification

    async def find_targets(self, fields):
        try:
            response = await self.client.post(ROOT + "/targets:find", json={"fields": fields})
        except httpx.RequestError:
            raise LookupUnavailable(retryable=True) from None
        if not response.is_success:
            status = response.status_code
            raise LookupUnavailable(retryable=status in {408, 429} or status >= 500)
        try:
            result = TargetResponse.model_validate_json(response.content)
        except ValidationError:
            raise LookupUnavailable(retryable=False) from None
        return Lookup(result.status, tuple(result.targets))

    @staticmethod
    def attachment_metadata(target, attachment, request_id):
        return {"target": target, "kind": attachment.kind, "request_id": request_id,
                "sha256": hashlib.sha256(attachment.content).hexdigest()}

    async def put_attachment(self, target: str, attachment: Attachment, request_id: str) -> WriteResult:
        metadata = self.attachment_metadata(target, attachment, request_id)
        try:
            response = await self.client.post(ROOT + "/attachments", headers={"Idempotency-Key": request_id},
                                              files={"metadata": (None, json.dumps(metadata)),
                                                     "file": (attachment.name, attachment.content, attachment.media_type)})
            if response.is_success:
                result = WriteResponse.model_validate_json(response.content)
                return WriteResult(result.status, result.receipt)
            if response.status_code == 429:
                return WriteResult(WriteStatus.RETRYABLE)
            if 400 <= response.status_code < 500:
                result = WriteResponse.model_validate_json(response.content)
                if result.status in {WriteStatus.REJECTED, WriteStatus.RETRYABLE}:
                    return WriteResult(result.status, result.receipt)
        except (httpx.RequestError, ValidationError):
            pass
        return WriteResult(WriteStatus.UNKNOWN)

    async def verify_attachment(self, target: str, attachment: Attachment, request_id: str) -> VerifyResult:
        if self.supports_verification:
            try:
                response = await self.client.post(ROOT + "/attachments:verify",
                                                  json=self.attachment_metadata(target, attachment, request_id))
                if response.is_success:
                    result = VerifyResponse.model_validate_json(response.content)
                    return VerifyResult(result.status, result.receipt)
            except (httpx.RequestError, ValidationError):
                pass
        return VerifyResult(VerifyStatus.INCONCLUSIVE)
