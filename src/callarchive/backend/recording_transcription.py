import hmac
import json
import re
import secrets
from pathlib import Path
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Environment, PackageLoader, select_autoescape

from ..http import Options, RequestId, read_audio
from .browser_jobs import BrowserJobs
from .console import page, render_arrival, signed_in_redirect
from .recording_names import archive_recording_names
from .sessions import CONSOLE_SESSION, Session, SessionStore
from .store import SubmissionConflict
from .workflow import JOBS


PATH = "/transcriptions"
API = PATH + "/api"
EXTERNAL_JOBS = "/v1/transcription/external-jobs"
COOKIE = "callarchive_transcription_session"
PRELOGIN = "callarchive_transcription_signin"
PRIVATE = {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer"}
TEMPLATES = Environment(loader=PackageLoader("callarchive.backend"), autoescape=select_autoescape(("html",)))
STATES = {"queued": "queued", "running": "running", "succeeded": "done", "failed": "failed", "expired": "expired"}


def create_recording_transcription(store, upstream, credentials, *, password_accounts, secure_cookies, max_bytes,
                                   admins, api_owners, admin_results, admin_system_results=False):
    api_owners = api_owners or {}
    router = APIRouter(include_in_schema=False)
    sessions = SessionStore(store, credentials, password_accounts=password_accounts)
    jobs = BrowserJobs(store)
    console_css = (Path(__file__).parent / "static" / "console.css").read_text(encoding="utf-8")

    def session_for(request: Request):
        token = request.cookies.get(CONSOLE_SESSION) or request.cookies.get(COOKIE)
        return sessions.get(token) if token else None

    def require_session(request: Request):
        session = session_for(request)
        if session is None:
            raise HTTPException(401, detail="登录已过期，请刷新页面后重新登录。")
        return session

    def render(*, status_code=200, **context):
        nonce = secrets.token_urlsafe(24)
        policy = (f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; "
                  "connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
        html = TEMPLATES.get_template("recording_transcription.html").render(
            nonce=nonce, console_css=console_css, **context)
        return HTMLResponse(html, headers={**PRIVATE, "Content-Security-Policy": policy}, status_code=status_code)

    def signin(*, error=None, status_code=200):
        csrf = secrets.token_urlsafe(32)
        response = page("console_login", title="录音转写", action=PATH, csrf=csrf, error=error, status_code=status_code)
        response.set_cookie(PRELOGIN, csrf, path=PATH, max_age=600, httponly=True,
                            samesite="strict", secure=secure_cookies)
        return response

    @router.get(PATH)
    def index(request: Request):
        session = session_for(request)
        if session is None:
            return signin()
        return render_arrival(request, render, secure_cookies=secure_cookies, csrf=session.csrf)

    @router.post(PATH)
    def login(request: Request, credential: Annotated[str, Form(max_length=1024)],
              csrf: Annotated[str, Form(max_length=128)]):
        expected = request.cookies.get(PRELOGIN, "")
        if not expected or not hmac.compare_digest(expected.encode(), csrf.encode()):
            return signin(error="登录已过期，请重试。", status_code=403)
        result = sessions.login(credential)
        if result is None:
            return signin(error="访问凭据无效。", status_code=401)
        token, _ = result
        response = signed_in_redirect(PATH, token, lifetime=sessions.lifetime, secure_cookies=secure_cookies)
        response.delete_cookie(PRELOGIN, path=PATH, secure=secure_cookies)
        return response

    async def service(method, route, *, allowed=(200,), **kwargs):
        try:
            reply = await upstream.request(method, route, **kwargs)
        except httpx.RequestError:
            raise HTTPException(503, detail="转写接口暂不可用。") from None
        if reply.status_code not in allowed:
            raise HTTPException(503, detail="转写接口暂不可用。")
        try:
            return reply.status_code, reply.json()
        except ValueError:
            raise HTTPException(502, detail="转写接口返回了无效响应。") from None

    async def statuses(upstream_ids):
        _, payload = await service("GET", JOBS, params=[("job_id", job_id) for job_id in upstream_ids])
        return payload

    def is_admin(session):
        return session.role == "admin" if session.role is not None else session.owner in admins

    def can_view_system(session):
        return admin_system_results and (is_admin(session) or session.role == "ops")

    @router.get(API + "/status")
    async def status(session: Annotated[Session, Depends(require_session)]):
        while True:
            page = await run_in_threadpool(jobs.list, session.owner, confirmed_only=True)
            rows = page["items"]
            payload = await statuses([row["upstream_id"] for row in rows])
            states = {item["job_id"]: item for item in payload["jobs"]}
            missing = [(row["id"], row["upstream_id"]) for row in rows
                       if row["upstream_id"] not in states]
            if not missing:
                break
            await run_in_threadpool(jobs.remove_missing, missing)
            if page["next_cursor"] is None:
                rows = [row for row in rows if row["upstream_id"] in states]
                break
        tasks = []
        for row in rows:
            item = states.get(row["upstream_id"], {})
            state = STATES.get(item.get("status"), "unavailable")
            tasks.append({"task_id": row["id"], "filename": row["filename"], "created": item.get("created"),
                          "state": state, "source": "web", "progress": item.get("progress"),
                          "can_view_result": state in {"done", "failed"}})
        bound_owners = api_owners.get(session.owner, ())
        admin = is_admin(session)
        system_visible = can_view_system(session)
        if payload.get("external_jobs_enabled") and (admin or bound_owners or system_visible):
            params = [] if admin else [("owner", owner) for owner in bound_owners]
            if admin or system_visible:
                params.append(("include_system", "true"))
            if not admin and not bound_owners:
                params.append(("system_only", "true"))
            _, external = await service("GET", EXTERNAL_JOBS, params=params)
            unnamed_system = [item["job_id"] for item in external["jobs"]
                              if item.get("system") and item["filename"] in (None, "", "recording")]
            system_names = await run_in_threadpool(archive_recording_names, store, unnamed_system, by_job=True)
            for item in external["jobs"]:
                state = STATES.get(item["status"], "unavailable")
                system = item.get("system", False)
                can_view = (system_visible if system
                            else item["owner"] in bound_owners or (admin and admin_results))
                prefix = "system:" if system else "external:"
                filename = item["filename"]
                if system and filename in (None, "", "recording"):
                    filename = system_names.get(item["job_id"], "录音")
                tasks.append({"task_id": prefix + item["job_id"],
                              "filename": filename or item["job_id"], "created": item["created"],
                              "state": state, "source": "system" if system else "API",
                              "progress": item.get("progress"),
                              "api_owner": item["owner"] if admin and not system else None,
                              "can_view_result": can_view and state in {"done", "failed"}})
            tasks.sort(key=lambda task: (task["created"] is None, -(task["created"] or 0)))
        return JSONResponse({"tasks": tasks, "max_upload_bytes": min(max_bytes, payload["max_upload_bytes"]),
                             "result_retention_seconds": payload["retention_seconds"],
                             "queue": payload.get("queue")}, headers=PRIVATE)

    @router.post(API, status_code=202)
    async def submit(session: Annotated[Session, Depends(require_session)],
                     audio: Annotated[UploadFile, File()], csrf: Annotated[str, Form(max_length=128)],
                     request_id: Annotated[RequestId, Form()]):
        if not session.accepts_csrf(csrf):
            raise HTTPException(403, detail="提交凭据已过期，请刷新页面。")
        try:
            content, media = await run_in_threadpool(read_audio, audio, max_bytes)
        except HTTPException as error:
            message = "录音文件超过大小限制。" if error.status_code == 413 else "无法读取录音，请选择有效的 MP3 或 WAV。"
            raise HTTPException(error.status_code, detail=message) from None
        filename = re.split(r"[/\\\\]", audio.filename or "录音")[-1][:200]
        options = Options().model_dump()
        try:
            item = await run_in_threadpool(jobs.reserve, session.owner, request_id, content, options, filename)
        except SubmissionConflict:
            raise HTTPException(409, detail="这次提交已用于其他录音，请重新选择文件。") from None
        if item["upstream_id"] is None:
            code, result = await service("POST", JOBS, allowed=(202, 413, 415, 422), files={
                "audio": ("recording", content, media),
                "metadata": (None, json.dumps({"request_id": "web:" + item["id"], "options": options})),
            })
            if code != 202:
                raise HTTPException(code, detail="转写服务拒绝了此录音，请核对格式和大小。")
            upstream_id = result.get("job_id")
            if not isinstance(upstream_id, str) or not re.fullmatch(r"[a-f0-9]{32}", upstream_id):
                raise HTTPException(502, detail="无法确认提交，请使用同一文件重试。")
            await run_in_threadpool(jobs.attach, item["id"], upstream_id)
        return JSONResponse({"task_id": item["id"]}, status_code=202, headers=PRIVATE)

    async def external_result(session, task_id, job_id, *, system=False):
        owners = api_owners.get(session.owner, ())
        can_read_all = can_view_system(session) if system else is_admin(session) and admin_results
        allowed = can_read_all if system else owners or can_read_all
        if not re.fullmatch(r"[a-f0-9]{32}", job_id) or not allowed:
            raise HTTPException(404, detail="找不到该转写任务。")
        params = [("job_id", job_id)]
        if system:
            params.append(("system_only", "true"))
        elif not can_read_all:
            params.extend(("owner", owner) for owner in owners)
        code, item = await service("GET", EXTERNAL_JOBS, params=params, allowed=(200, 403, 404))
        if code != 200 or bool(item.get("system")) != system:
            raise HTTPException(404, detail="找不到该转写任务。")
        state = STATES.get(item["status"], "unavailable")
        result = {"task_id": task_id, "state": state, "expires_at": item["expires_at"]}
        if state == "done":
            result["text"] = item["text"]
        if state == "failed":
            result["error"] = "转写失败，请检查录音后重新提交。"
        return JSONResponse(result, headers=PRIVATE)

    @router.get(API + "/{job_id}")
    async def result(session: Annotated[Session, Depends(require_session)], job_id: str):
        if job_id.startswith("system:"):
            return await external_result(session, job_id, job_id.removeprefix("system:"), system=True)
        if job_id.startswith("external:"):
            return await external_result(session, job_id, job_id.removeprefix("external:"))
        record = await run_in_threadpool(jobs.get, session.owner, job_id)
        if record is None or not record["upstream_id"]:
            raise HTTPException(404, detail="找不到该转写任务。")
        upstream_id = record["upstream_id"]
        payload = await statuses([upstream_id])
        if not payload["jobs"]:
            await run_in_threadpool(jobs.remove_missing, [(job_id, upstream_id)])
            raise HTTPException(404, detail="找不到该转写任务。")
        item = payload["jobs"][0]
        state = STATES.get(item["status"], "unavailable")
        result = {"task_id": job_id, "state": state, "expires_at": item["expires_at"]}
        if state == "done":
            code, transcript = await service("GET", f"{JOBS}/{upstream_id}/result", allowed=(200, 404, 410))
            if code == 200 and isinstance(transcript.get("text"), str):
                result["text"] = transcript["text"]
            else:
                result["state"] = "expired" if code != 200 else "unavailable"
        if state == "failed":
            result["error"] = "转写失败，请检查录音后重新上传。"
        return JSONResponse(result, headers=PRIVATE)

    return router
