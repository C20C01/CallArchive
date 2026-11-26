import json
import re
from typing import Annotated
from uuid import uuid4

import httpx
from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from pydantic import TypeAdapter, ValidationError

from ..http import Options, RequestId, read_audio
from .browser_jobs import BrowserJobs
from .store import SubmissionConflict
from .workflow import JOBS


def create_transcription_pages(store, upstream, *, protected_page, login_url, session_for, check_form,
                               redirect, private_headers, max_bytes):
    router = APIRouter(prefix="/transcription")
    jobs = BrowserJobs(store)
    input_id = TypeAdapter(RequestId)
    media_types = {"audio/wav", "audio/mpeg", "audio/flac"}

    def job_scope(session):
        return None if session.role == "admin" else session.owner

    def render_home(session, *, cursor=None, request_id=None, error=None, status_code=200):
        return protected_page("transcription", session, records=jobs.list(job_scope(session), cursor=cursor),
                              cursor=cursor, request_id=request_id or uuid4().hex, error=error,
                              status_code=status_code)

    @router.get("")
    def index(request: Request, cursor: int | None = Query(None, ge=1)):
        session = session_for(request)
        return render_home(session, cursor=cursor) if session else redirect(login_url(request))

    @router.post("")
    async def submit(request: Request, audio: Annotated[UploadFile, File()], csrf: Annotated[str, Form()],
                     request_id: Annotated[str, Form()], hotwords: Annotated[str, Form()] = "",
                     speaker_count: Annotated[str, Form()] = ""):
        session = await run_in_threadpool(check_form, request, csrf)
        try:
            input_id.validate_python(request_id)
            options = Options.model_validate({
                "hotwords": list(dict.fromkeys(word.strip() for word in hotwords.splitlines() if word.strip())),
                "speaker_count": int(speaker_count) if speaker_count else None,
            }).model_dump()
            content, media = await run_in_threadpool(read_audio, audio, max_bytes)
            filename = re.split(r"[/\\]", audio.filename or "录音")[-1][:200]
            item = await run_in_threadpool(jobs.reserve, session.owner, request_id, content, options, filename)
        except (ValueError, ValidationError, HTTPException, SubmissionConflict):
            return await run_in_threadpool(render_home, session, error="请检查录音文件和转写选项后重试。",
                                           status_code=422)
        if item["upstream_id"] is None:
            try:
                result = await upstream.post(JOBS, files={
                    "audio": ("recording", content, media),
                    "metadata": (None, json.dumps({"request_id": "web:" + item["id"], "options": options})),
                })
                upstream_id = result.raise_for_status().json()["job_id"]
                if not isinstance(upstream_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]+", upstream_id):
                    raise ValueError("Invalid task ID")
                await run_in_threadpool(jobs.attach, item["id"], upstream_id)
            except (httpx.HTTPError, ValueError, KeyError, TypeError):
                return await run_in_threadpool(
                    render_home, session, request_id=request_id,
                    error="暂时无法确认提交，请选择同一文件并用相同选项重试。", status_code=503,
                )
        return redirect("/workbench/transcription/" + item["id"])

    async def record(session, job_id):
        item = await run_in_threadpool(jobs.get, job_scope(session), job_id)
        if item is None:
            raise HTTPException(404)
        return item

    async def fetch(upstream_id, suffix=""):
        try:
            return await upstream.get(f"{JOBS}/{upstream_id}{suffix}")
        except httpx.RequestError:
            raise HTTPException(503, detail="转写服务暂不可用。") from None

    @router.get("/{job_id}")
    async def details(request: Request, job_id: str):
        session = await run_in_threadpool(session_for, request)
        if not session:
            return redirect(login_url(request))
        item = await record(session, job_id)
        status, transcript = "submitting", None
        if item["upstream_id"]:
            try:
                response = await fetch(item["upstream_id"])
                if response.status_code in (404, 410):
                    status = "expired"
                else:
                    status = response.raise_for_status().json()["status"]
                if status == "succeeded":
                    response = await fetch(item["upstream_id"], "/result")
                    if response.status_code in (404, 410):
                        status = "expired"
                    else:
                        transcript = response.raise_for_status().json()["text"]
            except (httpx.HTTPError, HTTPException, ValueError, KeyError, TypeError):
                status = "unavailable"
        return protected_page("transcription_job", session, item=item, status=status, transcript=transcript)

    async def get_artifact(request, job_id, suffix):
        session = await run_in_threadpool(session_for, request)
        if session is None:
            raise HTTPException(401)
        item = await record(session, job_id)
        if item["upstream_id"] is None:
            raise HTTPException(409)
        response = await fetch(item["upstream_id"], suffix)
        if response.status_code in (404, 409, 410):
            raise HTTPException(response.status_code)
        if response.status_code != 200:
            raise HTTPException(502)
        return response

    @router.get("/{job_id}/audio")
    async def audio(request: Request, job_id: str):
        response = await get_artifact(request, job_id, "/audio")
        media = response.headers.get("content-type", "").split(";", 1)[0]
        if media not in media_types or len(response.content) > max_bytes:
            raise HTTPException(502)
        return Response(response.content, media_type=media, headers=private_headers)

    @router.get("/{job_id}/text")
    async def download(request: Request, job_id: str):
        response = await get_artifact(request, job_id, "/result")
        try:
            transcript = response.json()["text"]
            if not isinstance(transcript, str):
                raise ValueError("Invalid transcript")
        except (KeyError, ValueError, TypeError):
            raise HTTPException(502) from None
        return Response(transcript, media_type="text/plain",
                        headers={**private_headers, "Content-Disposition": 'attachment; filename="transcript.txt"'})

    return router
