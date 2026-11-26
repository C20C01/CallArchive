import hmac
import io
import json
import os
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException, UploadFile
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field
from soundfile import SoundFile, SoundFileError


RequestId = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")]


class Options(BaseModel):
    model_config = ConfigDict(extra="forbid")

    language: str | None = Field(default=None, min_length=1, max_length=35)
    hotwords: list[Annotated[str, Field(min_length=1, max_length=100)]] = Field(
        default_factory=list, max_length=100,
    )
    speaker_count: int | None = Field(default=None, ge=1, le=100, strict=True)


@dataclass(frozen=True)
class RecordingInfo:
    media_type: str
    duration_ms: int


def inspect_recording(audio):
    supported = {"WAV": "audio/wav", "WAVEX": "audio/wav", "MP3": "audio/mpeg",
                 "FLAC": "audio/flac"}
    try:
        with SoundFile(io.BytesIO(audio)) as source:
            media_type = supported.get(source.format)
            if not media_type or not len(source):
                raise ValueError("Unsupported or empty audio")
            for _ in source.blocks(blocksize=65536):
                pass
            return RecordingInfo(media_type, round(len(source) * 1000 / source.samplerate))
    except (ValueError, SoundFileError):
        raise HTTPException(422, detail={"code": "invalid_audio"}) from None


def load_credentials():
    try:
        records = json.loads(os.environ["CALLARCHIVE_IDENTITIES"])
        credentials = {}
        for owner, record in records.items():
            token = record["token"]
            requires_ack = record.get("service", False)
            if not owner or not isinstance(token, str) or not token or token in credentials:
                raise ValueError
            if not isinstance(requires_ack, bool):
                raise ValueError
            credentials[token] = (owner, requires_ack)
        if not credentials:
            raise ValueError
        return credentials
    except (AttributeError, KeyError, TypeError, ValueError):
        raise SystemExit("Configure CALLARCHIVE_IDENTITIES with unique tokens.") from None


def make_identity(credentials):
    bearer = HTTPBearer(auto_error=False)

    def identity(auth: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]):
        if auth:
            supplied = auth.credentials.encode("utf-8")
            for token, identity_value in credentials.items():
                if hmac.compare_digest(supplied, token.encode("utf-8")):
                    return identity_value
        raise HTTPException(401, detail={"code": "authentication_required"},
                            headers={"WWW-Authenticate": "Bearer"})

    return identity


def inspect_audio(audio):
    return inspect_recording(audio).media_type


def read_recording(audio: UploadFile, max_bytes: int):
    content = audio.file.read(max_bytes + 1)
    if len(content) > max_bytes:
        raise HTTPException(413, detail={"code": "audio_too_large"})
    return content, inspect_recording(content)


def read_audio(audio: UploadFile, max_bytes: int):
    content, info = read_recording(audio, max_bytes)
    return content, info.media_type
