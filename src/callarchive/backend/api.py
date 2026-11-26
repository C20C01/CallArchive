from pathlib import Path
from secrets import compare_digest
from typing import Annotated, Literal

from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, Request, Query, UploadFile
from fastapi.responses import Response
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from ..http import Options, RequestId, make_identity, read_recording
from .store import ArchiveStore, RemovalUnavailable, RetryUnavailable, SubmissionConflict


class Call(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_call_id: str | None = Field(default=None, min_length=1, max_length=256)
    started_at: AwareDatetime | None = None
    caller: str | None = Field(default=None, min_length=1, max_length=64)
    callee: str | None = Field(default=None, min_length=1, max_length=64)
    connected: bool | None = Field(default=None, strict=True)
    call_duration_ms: int | None = Field(default=None, ge=0, strict=True)


class Submission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: RequestId
    flow: Literal["local", "remote"]
    call: Call | None = None
    options: Options = Field(default_factory=Options)


class Confirmation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    confirm: Literal[True]


def create_app(database: Path, credentials: dict[str, tuple[str, bool]], *, max_bytes=25 * 1024**2, admins=frozenset(), secure_cookies=False,
               workbench_transcription=None, runtime_log=None, external_archive_id=None, default_flow="local",
               password_accounts=None, enable_workbench=False, operator_transcription=None,
               transcription_api_owners=None, transcription_admin_results=False,
               transcription_admin_system_results=False,
               followup_names=None, attachments=None,
               completion_token=None, resume_transcription=None, resume_recording=None, resume_call=None):
    store = ArchiveStore(database)
    identity = make_identity(credentials)
    app = FastAPI(title="CallArchive Archive", version="1")

    if completion_token and resume_transcription is not None:
        @app.post("/internal/transcriptions/{job_id}/complete", status_code=204, include_in_schema=False)
        async def transcription_completed(job_id: str, request: Request, background: BackgroundTasks):
            credential = request.headers.get("authorization", "")
            if not compare_digest(credential.encode(), f"Bearer {completion_token}".encode()):
                raise HTTPException(401, detail={"code": "authentication_required"})
            background.add_task(resume_transcription, job_id)

    def owner_scope(actor, all_owners):
        if all_owners:
            if actor[1] or actor[0] not in admins:
                raise HTTPException(403, detail={"code": "administrator_required"})
            return None
        return actor[0]

    @app.get("/v1/archive/recordings")
    def list_recordings(actor: Annotated[tuple[str, bool], Depends(identity)],
                        all_owners: bool = False, cursor: int | None = Query(None, ge=1),
                        limit: int = Query(50, ge=1, le=100)):
        return store.list_recordings(owner_scope(actor, all_owners), cursor=cursor, limit=limit)

    @app.post("/v1/archive/recordings", status_code=202)
    def submit(
        background: BackgroundTasks,
        actor: Annotated[tuple[str, bool], Depends(identity)],
        audio: Annotated[UploadFile, File()],
        metadata: Annotated[str, Form()],
    ):
        try:
            body = Submission.model_validate_json(metadata)
        except ValidationError:
            raise HTTPException(422, detail={"code": "invalid_metadata"}) from None
        if body.flow == "remote" and external_archive_id is None:
            raise HTTPException(409, detail={"code": "archive_provider_not_configured"})
        content, info = read_recording(audio, max_bytes)
        submission = body.model_dump(mode="json", exclude={"request_id"})
        if body.flow == "remote":
            submission["archive_instance"] = external_archive_id
        try:
            result = store.submit(actor[0], body.request_id, submission, content, info.media_type,
                                  duration_ms=info.duration_ms)
        except SubmissionConflict:
            raise HTTPException(409, detail={"code": "request_conflict"}) from None
        if resume_recording is not None:
            background.add_task(resume_recording, result["recording_id"])
        return result

    @app.get("/v1/archive/recordings/{recording_id}")
    def get(recording_id: str, actor: Annotated[tuple[str, bool], Depends(identity)],
            all_owners: bool = False):
        row = store.get(owner_scope(actor, all_owners), recording_id)
        if row is None:
            raise HTTPException(404, detail={"code": "recording_not_found"})
        return row

    @app.get("/v1/archive/recordings/{recording_id}/audio")
    def get_audio(recording_id: str, actor: Annotated[tuple[str, bool], Depends(identity)],
                  all_owners: bool = False):
        row = store.get_audio(owner_scope(actor, all_owners), recording_id)
        if row is None:
            raise HTTPException(404, detail={"code": "recording_not_found"})
        return Response(row["audio"], media_type=row["media_type"])

    @app.get("/v1/archive/recordings/{recording_id}/events")
    def events(recording_id: str, actor: Annotated[tuple[str, bool], Depends(identity)],
               all_owners: bool = False, after: int = Query(0, ge=0),
               limit: int = Query(50, ge=1, le=100)):
        result = store.events(owner_scope(actor, all_owners), recording_id, after=after, limit=limit)
        if result is None:
            raise HTTPException(404, detail={"code": "recording_not_found"})
        return result

    @app.post("/v1/archive/recordings/{recording_id}/retry", status_code=202)
    def retry(recording_id: str, actor: Annotated[tuple[str, bool], Depends(identity)],
              background: BackgroundTasks, all_owners: bool = False):
        scope = owner_scope(actor, all_owners)
        try:
            result = store.request_retry(scope, recording_id, actor=actor[0])
        except RetryUnavailable:
            raise HTTPException(409, detail={"code": "retry_unavailable"}) from None
        if result is None:
            raise HTTPException(404, detail={"code": "recording_not_found"})
        if resume_recording is not None:
            background.add_task(resume_recording, recording_id)
        return result

    @app.post("/v1/archive/recordings/{recording_id}/{operation}")
    def change_removal(recording_id: str, operation: Literal["remove", "restore"], confirmation: Confirmation,
                       actor: Annotated[tuple[str, bool], Depends(identity)], all_owners: bool = False):
        if confirmation.confirm is not True:
            raise HTTPException(422, detail={"code": "confirmation_required"})
        scope = owner_scope(actor, all_owners)
        try:
            result = store.set_removed(scope, recording_id, actor=actor[0], removed=operation == "remove")
        except RemovalUnavailable:
            raise HTTPException(409, detail={"code": "removal_unavailable"}) from None
        if result is None:
            raise HTTPException(404, detail={"code": "recording_not_found"})
        return result

    @app.get("/v1/archive/operations/logs")
    def runtime_events(actor: Annotated[tuple[str, bool], Depends(identity)],
                       related_id: str | None = Query(None, max_length=128),
                       component: str | None = Query(None, max_length=128),
                       before: int | None = Query(None, ge=1), limit: int = Query(50, ge=1, le=100)):
        owner_scope(actor, True)
        if runtime_log is None:
            raise HTTPException(404, detail={"code": "runtime_log_disabled"})
        return runtime_log.list(related_id=related_id, component=component, before=before, limit=limit)

    if not enable_workbench:
        from .console import create_console

        app.include_router(create_console(store, credentials, admins, password_accounts=password_accounts,
                                          secure_cookies=secure_cookies, has_logs=runtime_log is not None,
                                          transcription=operator_transcription, followup_names=followup_names,
                                          attachments=attachments, resume_call=resume_call))

    if runtime_log is not None:
        from .runtime_log_view import create_runtime_log_view

        app.include_router(create_runtime_log_view(store, runtime_log, credentials, admins,
                                                   password_accounts=password_accounts, secure_cookies=secure_cookies))

    if operator_transcription is not None:
        from .recording_transcription import create_recording_transcription

        app.include_router(create_recording_transcription(store, operator_transcription, credentials,
                                                          password_accounts=password_accounts,
                                                          secure_cookies=secure_cookies, max_bytes=max_bytes,
                                                          admins=admins, api_owners=transcription_api_owners,
                                                          admin_results=transcription_admin_results,
                                                          admin_system_results=transcription_admin_system_results))

    if enable_workbench:
        from .web import HOME, create_workbench, redirect

        @app.get("/", include_in_schema=False)
        def home():
            return redirect(HOME)

        app.include_router(create_workbench(store, credentials, admins, max_bytes=max_bytes,
                                            secure_cookies=secure_cookies, submission_model=Submission,
                                            workbench_transcription=workbench_transcription, runtime_log=runtime_log,
                                            external_archive_id=external_archive_id, default_flow=default_flow,
                                            password_accounts=password_accounts))
    return app
