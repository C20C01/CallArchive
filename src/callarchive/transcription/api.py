import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ..http import Options, RequestId, make_identity, read_audio
from ..observability import event
from .jobs import DEFAULT_MAX_UNCLAIMED, DEFAULT_RETENTION, ArchiveGroupRetired, JobStore, RequestConflict


ArchiveGroupId = Annotated[str, Field(pattern=r"^[a-f0-9]{32}$")]


class Metadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: RequestId
    archive_group: ArchiveGroupId | None = None
    options: Options = Field(default_factory=Options)


class ArchiveGroupRetirement(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_ids: Annotated[list[ArchiveGroupId], Field(min_length=1, max_length=100)]


def create_app(database: Path, credentials: dict[str, tuple[str, bool]], *,
               engine="demo", max_bytes=25 * 1024**2, retention=DEFAULT_RETENTION,
               max_unclaimed=DEFAULT_MAX_UNCLAIMED,
               web_gateway_owner=None):
    store = JobStore(database)
    app = FastAPI(title="CallArchive Transcription", version="1")
    identity = make_identity(credentials)
    gateway_identity = (web_gateway_owner, False) if web_gateway_owner else None
    if gateway_identity is not None and gateway_identity not in credentials.values():
        raise ValueError("Configure web_gateway_owner as a dedicated non-service transcription identity.")

    def display_status(row):
        state = row["status"]
        if ((state == "succeeded" and not row["has_result"])
                or (state == "failed" and not row["has_audio"] and not row["requires_ack"])):
            state = "expired"
        retention_start = row["finished"]
        if state == "failed" and row["archive_group"] is not None:
            retention_start = None
        elif row["requires_ack"] and state != "failed":
            retention_start = row["acknowledged"]
        expires_at = (datetime.fromtimestamp(retention_start + retention, timezone.utc).isoformat()
                      if retention_start is not None else None)
        item = {"job_id": row["id"], "status": state, "created": row["created"], "expires_at": expires_at}
        if state == "running" and row["progress"]:
            item["progress"] = json.loads(row["progress"])
        return item

    def get_job(owner, job_id):
        store.purge(retention, max_unclaimed)
        row = store.get(owner, job_id)
        if row is None:
            raise HTTPException(404, detail={"code": "job_not_found"})
        return row

    @app.post("/v1/transcription/jobs", status_code=202)
    def submit(
        caller: Annotated[tuple[str, bool], Depends(identity)],
        audio: Annotated[UploadFile, File()],
        metadata: Annotated[str, Form()],
    ):
        try:
            body = Metadata.model_validate_json(metadata)
        except ValidationError:
            raise HTTPException(422, detail={"code": "invalid_metadata"}) from None
        content, media_type = read_audio(audio, max_bytes)
        owner, requires_ack = caller
        if body.archive_group is not None and not requires_ack:
            raise HTTPException(403, detail={"code": "service_identity_required"})
        filename = re.split(r"[/\\]", audio.filename or "")[-1][:200] or None
        try:
            job_id = store.submit(owner, requires_ack, body.request_id,
                                  body.options.model_dump(), content, media_type, engine, filename=filename,
                                  archive_group=body.archive_group, retention=retention,
                                  max_unclaimed=max_unclaimed)
        except ArchiveGroupRetired:
            raise HTTPException(410, detail={"code": "archive_group_retired"}) from None
        except RequestConflict:
            raise HTTPException(409, detail={"code": "request_conflict"}) from None
        event("transcription_accepted", job_id=job_id)
        return {"job_id": job_id}

    @app.get("/v1/transcription/jobs")
    def statuses(caller: Annotated[tuple[str, bool], Depends(identity)],
                 job_id: Annotated[list[str], Query(max_length=35)] = []):
        store.purge(retention, max_unclaimed)
        items = [display_status(row) for row in store.statuses(caller[0], job_id)]
        return {"jobs": items, "max_upload_bytes": max_bytes, "retention_seconds": retention,
                "queue": store.queue_status(engine), "external_jobs_enabled": caller == gateway_identity}

    @app.post("/internal/transcription/archive-groups/retire", status_code=204, include_in_schema=False)
    def retire_archive_groups(body: ArchiveGroupRetirement,
                              caller: Annotated[tuple[str, bool], Depends(identity)]):
        if not caller[1]:
            raise HTTPException(403, detail={"code": "service_identity_required"})
        store.retire_archive_groups(caller[0], body.group_ids)
        return Response(status_code=204)

    @app.get("/v1/transcription/external-jobs", include_in_schema=False)
    def external_jobs(caller: Annotated[tuple[str, bool], Depends(identity)],
                      owner: Annotated[list[str] | None, Query()] = None,
                      include_system: bool = False,
                      system_only: bool = False,
                      job_id: Annotated[str | None, Query(pattern=r"^[a-f0-9]{32}$")] = None):
        if caller != gateway_identity:
            raise HTTPException(403, detail={"code": "gateway_required"})
        store.purge(retention, max_unclaimed)
        rows = store.external_jobs(web_gateway_owner, owner, job_id=job_id, include_system=include_system,
                                   system_only=system_only)
        if job_id:
            if not rows:
                raise HTTPException(404, detail={"code": "job_not_found"})
            row = rows[0]
            result = dict(display_status(row), system=bool(row["requires_ack"]))
            if result["status"] == "succeeded":
                result["text"] = json.loads(row["result"])["text"]
            return result
        return {"jobs": [dict(display_status(row), owner=row["owner"], filename=row["filename"],
                              system=bool(row["requires_ack"])) for row in rows]}

    @app.get("/v1/transcription/jobs/{job_id}")
    def job(job_id: str, caller: Annotated[tuple[str, bool], Depends(identity)]):
        row = get_job(caller[0], job_id)
        item = {"job_id": row["id"], "engine": row["engine"], "status": row["status"], "error": row["error"]}
        if row["status"] == "running" and row["progress"]:
            item["progress"] = json.loads(row["progress"])
        return item

    @app.get("/v1/transcription/jobs/{job_id}/audio")
    def audio(job_id: str, caller: Annotated[tuple[str, bool], Depends(identity)]):
        store.purge(retention, max_unclaimed)
        row = store.get_audio(caller[0], job_id)
        if row is None:
            raise HTTPException(404, detail={"code": "job_not_found"})
        if row["audio"] is None:
            raise HTTPException(410, detail={"code": "audio_expired"})
        return Response(row["audio"], media_type=row["media_type"])

    @app.get("/v1/transcription/jobs/{job_id}/result")
    def result(job_id: str, caller: Annotated[tuple[str, bool], Depends(identity)]):
        row = get_job(caller[0], job_id)
        if row["status"] != "succeeded":
            raise HTTPException(409, detail={"code": "result_unavailable", "status": row["status"]})
        if row["result"] is None:
            raise HTTPException(410, detail={"code": "result_expired"})
        return json.loads(row["result"])

    @app.post("/v1/transcription/jobs/{job_id}/ack", status_code=204)
    def acknowledge(job_id: str, caller: Annotated[tuple[str, bool], Depends(identity)]):
        row = get_job(caller[0], job_id)
        if not caller[1]:
            raise HTTPException(403, detail={"code": "service_identity_required"})
        if row["status"] == "succeeded" and row["result"] is None:
            if row["acknowledged"] is not None:
                return Response(status_code=204)
            raise HTTPException(410, detail={"code": "result_expired"})
        if not store.acknowledge(caller[0], job_id):
            raise HTTPException(409, detail={"code": "result_unavailable"})
        return Response(status_code=204)

    return app
