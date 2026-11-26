import hmac
import secrets
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import parse_qsl, urlencode, urlsplit
from uuid import uuid4

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from jinja2 import Environment, PackageLoader, select_autoescape

from ..http import read_recording
from .sessions import SessionStore
from .web_transcription import create_transcription_pages
from .store import ArchiveStore, RemovalUnavailable, RetryUnavailable, SubmissionConflict

BASE = "/workbench"
HOME = BASE + "/home"
COOKIE = "callarchive_session"
PRELOGIN = "callarchive_signin"
STATUS = {
    "queued": "待转写", "transcribing": "转写中", "acknowledging": "保存结果中",
    "delivering": "归档中", "archived": "已归档", "needs_review": "需管理员核对",
    "needs_input": "待跟进", "removed": "已移除",
}
ERROR = {
    "archive_target_missing": "待补录应聘档案",
    "archive_target_conflict": "匹配到的档案需要核对",
    "archive_routing_changed": "话单信息已更新，归档目标和附件需核对",
    "archive_attachment_unknown": "附件结果待核对",
    "archive_attachment_rejected": "档案系统拒绝接收附件",
    "archive_instance_unavailable": "归档接入实例不可用",
    "archive_lookup_unavailable": "档案查询暂不可用",
    "attachment_write_failed": "档案附件写入失败",
    "attachment_conflict": "已有附件与当前内容不一致",
    "transcription_failed": "转写失败",
    "transcription_needs_review": "转写结果需核对",
    "transcription_unavailable": "转写服务暂不可用",
    "invalid_workflow_response": "处理结果格式异常",
}
COMPONENTS = {
    "archive-api": "归档后台", "archive-worker": "归档进程", "transcription-api": "转写服务",
    "transcription-worker": "转写进程", "outbound-import": "外呼导入", "notification": "通知",
}
PRIVATE = {"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "X-Content-Type-Options": "nosniff"}
PAGE_HEADERS = {
    **PRIVATE,
    "Content-Security-Policy": "default-src 'none'; style-src 'self'; media-src 'self'; "
                               "script-src 'self'; connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
}
templates = Environment(loader=PackageLoader("callarchive.backend"), autoescape=select_autoescape(("html",)))
templates.filters["event_time"] = lambda timestamp: datetime.fromtimestamp(timestamp, timezone.utc).strftime(
    "%Y-%m-%d %H:%M:%S UTC"
)
def duration_label(milliseconds):
    if milliseconds is None:
        return "—"
    minutes, seconds = divmod(milliseconds // 1000, 60)
    return f"{minutes:02d}:{seconds:02d}"


def call_time(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M")
    except (AttributeError, ValueError):
        return "—"


templates.filters["duration"] = duration_label
templates.filters["call_time"] = call_time
templates.globals.update(status_labels=STATUS, error_labels=ERROR)


def safe_destination(location: str) -> str:
    if any(char in location for char in "\\\r\n\t"):
        return BASE
    try:
        url = urlsplit(location)
    except ValueError:
        return BASE
    if url.netloc or url.scheme or not (url.path == BASE or url.path.startswith(BASE + "/")):
        return BASE
    return url.path + (f"?{url.query}" if url.query else "")


def list_query(back):
    destination = urlsplit(safe_destination(back))
    query = dict(parse_qsl(destination.query)) if destination.path == BASE else {}
    for key in ("after", "fragment", "recording", "retried", "bulk"):
        query.pop(key, None)
    return query


def recording_url(back, recording_id, all_owners, *, retried=False):
    query = list_query(back)
    query.update(recording=recording_id, all_owners=1 if all_owners else None, retried=1 if retried else None)
    return BASE + "?" + urlencode({key: value for key, value in query.items() if value})


def page(template, *, status_code=200, **context):
    return HTMLResponse(templates.get_template(template + ".html").render(**context),
                        status_code=status_code, headers=PAGE_HEADERS)


def redirect(destination):
    return RedirectResponse(destination, status_code=303, headers=PRIVATE)


def create_workbench(store: ArchiveStore, credentials, admins, *, max_bytes, secure_cookies, submission_model,
                     workbench_transcription=None, runtime_log=None, external_archive_id=None, default_flow="local",
                     password_accounts=None):
    router = APIRouter(prefix=BASE, include_in_schema=False)
    sessions = SessionStore(store, credentials, password_accounts=password_accounts)

    def login_url(request):
        destination = request.url.path + (f"?{request.url.query}" if request.url.query else "")
        return BASE + "/login?" + urlencode({"next": safe_destination(destination)})

    def session_for(request):
        token = request.cookies.get(COOKIE)
        return sessions.get(token) if token else None

    def check_form(request, csrf):
        session = session_for(request)
        if session is None:
            raise HTTPException(401)
        if not session.accepts_csrf(csrf):
            raise HTTPException(403)
        return session

    def is_admin(session):
        return session.role == "admin" if session.role is not None else session.owner in admins

    def scope(session, all_owners):
        if session.role == "external" or (all_owners and not is_admin(session)):
            raise HTTPException(403)
        return None if all_owners or session.role in {"ops", "admin"} else session.owner

    def protected_page(template, session, **context):
        return page(template, session=session, is_admin=is_admin(session),
                    transcription_enabled=workbench_transcription is not None,
                    runtime_log_enabled=runtime_log is not None,
                    external_archive_enabled=external_archive_id is not None, default_flow=default_flow, **context)

    def signin(destination, *, error=None, status_code=200):
        csrf = secrets.token_urlsafe(32)
        response = page("login", next=safe_destination(destination), csrf=csrf, error=error,
                        password_login=password_accounts is not None, status_code=status_code)
        response.set_cookie(PRELOGIN, csrf, path=BASE, max_age=600, httponly=True,
                            samesite="lax", secure=secure_cookies)
        return response

    @router.get("/style.css")
    def stylesheet():
        return FileResponse(Path(__file__).with_name("static") / "workbench.css", media_type="text/css")

    @router.get("/{script}.js")
    def workbench_script(script: Literal["details", "bulk-remove", "call-filters"]):
        return FileResponse(Path(__file__).with_name("static") / f"{script}.js",
                            media_type="text/javascript", headers=PRIVATE)

    @router.get("/login")
    def login(request: Request, next: str = HOME):
        return redirect(safe_destination(next)) if session_for(request) else signin(next)

    @router.post("/login")
    def submit_login(request: Request, credential: Annotated[str, Form(max_length=1024)],
                     csrf: Annotated[str, Form(max_length=128)], next: Annotated[str, Form()] = HOME):
        expected = request.cookies.get(PRELOGIN, "")
        if not expected or not hmac.compare_digest(expected.encode(), csrf.encode()):
            return signin(next, error="登录表单已过期，请重试。", status_code=403)
        result = sessions.login(credential)
        if result is None:
            return signin(next, error="凭据无效，请核对后重试。", status_code=401)
        token, _ = result
        response = redirect(safe_destination(next))
        response.set_cookie(COOKIE, token, path=BASE, max_age=sessions.lifetime, httponly=True,
                            samesite="lax", secure=secure_cookies)
        response.delete_cookie(PRELOGIN, path=BASE, secure=secure_cookies)
        return response

    @router.post("/logout")
    def logout(request: Request, csrf: Annotated[str, Form()]):
        check_form(request, csrf)
        sessions.logout(request.cookies[COOKIE])
        response = redirect(BASE + "/login")
        response.delete_cookie(COOKIE, path=BASE, secure=secure_cookies)
        return response

    @router.get("/home")
    def home(request: Request):
        session = session_for(request)
        if session is None:
            return redirect(login_url(request))
        return protected_page("home", session, can_archive=session.role != "external",
                              role_label={"admin": "管理员", "ops": "运营", "external": "外部"}.get(session.role))

    @router.get("")
    def index(request: Request, all_owners: bool = False, cursor: Annotated[int | None, Query(ge=1)] = None,
              view: Literal["all", "pending", "archived"] = "all", day: date | Literal[""] = "",
              mobile: Annotated[str, Query(max_length=64)] = "", assignee: Annotated[str, Query(max_length=128)] = "",
              status: Annotated[str, Query(max_length=32)] = "", recording: Annotated[str, Query(max_length=64)] = "",
              after: Annotated[int, Query(ge=0)] = 0, retried: bool = False, fragment: bool = False,
              bulk: Literal["removed", "unavailable", ""] = ""):
        session = session_for(request)
        if session is None:
            if fragment:
                raise HTTPException(401)
            return redirect(login_url(request))
        owner = scope(session, all_owners)
        item = store.get(owner, recording) if recording else None
        if recording and item is None:
            raise HTTPException(404)
        events = store.events(owner, recording, after=after, limit=40) if item else None
        filters = {"day": day.isoformat() if day else "", "mobile": mobile.strip(), "assignee": assignee.strip(), "status": status}
        query = {**filters, "view": view, "all_owners": 1 if all_owners else None}

        def list_url(**overrides):
            params = urlencode({key: value for key, value in (query | overrides).items() if value})
            return BASE + (f"?{params}" if params else "")

        detail = {"item": item, "events": events, "retried": retried, "all_owners": all_owners,
                  "cursor": cursor, "list_url": list_url, "close_url": list_url(cursor=cursor)}
        if fragment:
            if item is None:
                raise HTTPException(404)
            return protected_page("recording", session, **detail)
        result = store.list_recordings(
            owner, cursor=cursor, limit=35, view=view, include_actions=True,
            **{key: value or None for key, value in filters.items()},
        )
        return protected_page("recordings", session, result=result, view=view, filters=filters, bulk=bulk, **detail)

    @router.post("/bulk-remove")
    def bulk_remove(request: Request, csrf: Annotated[str, Form()], confirm: Annotated[str, Form()],
                    recording_ids: Annotated[list[str], Form(min_length=1, max_length=35)],
                    back: Annotated[str, Form(max_length=2048)] = BASE, all_owners: bool = False):
        session = check_form(request, csrf)
        owner = scope(session, all_owners)
        if confirm != "yes" or any(not recording_id or len(recording_id) > 64 for recording_id in recording_ids):
            raise HTTPException(422)
        try:
            store.remove_many(owner, dict.fromkeys(recording_ids), actor=session.owner)
            result = "removed"
        except RemovalUnavailable:
            result = "unavailable"
        query = list_query(back)
        query.update(bulk=result)
        if all_owners:
            query["all_owners"] = 1
        else:
            query.pop("all_owners", None)
        return redirect(BASE + "?" + urlencode(query))

    @router.get("/upload")
    def upload(request: Request):
        session = session_for(request)
        if session is None:
            return redirect(login_url(request))
        scope(session, False)
        return protected_page("upload", session, request_id=uuid4().hex)

    @router.post("/upload")
    def submit_upload(request: Request, audio: Annotated[UploadFile, File()], csrf: Annotated[str, Form()],
                      request_id: Annotated[str, Form()], callee: Annotated[str, Form()] = "",
                      caller: Annotated[str, Form()] = "", external_id: Annotated[str, Form()] = "",
                      flow: Annotated[str, Form()] = ""):
        session = check_form(request, csrf)
        scope(session, False)
        try:
            call = {key: value.strip() for key, value in
                    (("caller", caller), ("callee", callee), ("external_call_id", external_id)) if value.strip()}
            body = submission_model.model_validate({"request_id": request_id, "flow": flow or default_flow,
                                                    "call": call or None})
            metadata = body.model_dump(mode="json", exclude={"request_id"})
            if body.flow == "remote":
                if external_archive_id is None:
                    raise ValueError("External archive is not configured")
                metadata["archive_instance"] = external_archive_id
            content, info = read_recording(audio, max_bytes)
            result = store.submit(session.owner, body.request_id, metadata, content, info.media_type,
                                  duration_ms=info.duration_ms)
        except (HTTPException, ValueError, SubmissionConflict) as error:
            if isinstance(error, HTTPException) and error.status_code == 413:
                message = "录音文件超过服务端大小限制。"
            elif isinstance(error, SubmissionConflict):
                message = "这次提交已使用，请重新上传。"
            else:
                message = "请检查录音格式及话单字段。"
            return protected_page("upload", session, request_id=uuid4().hex, error=message, status_code=422)
        return redirect(f'{BASE}/recordings/{result["recording_id"]}')

    @router.get("/recordings/{recording_id}")
    def details(request: Request, recording_id: str, all_owners: bool = False, after: int = Query(0, ge=0),
                retried: bool = False):
        return index(request, all_owners=all_owners, recording=recording_id, after=after, retried=retried)

    @router.get("/recordings/{recording_id}/audio")
    def audio(request: Request, recording_id: str, all_owners: bool = False):
        session = session_for(request)
        if session is None:
            raise HTTPException(401)
        row = store.get_audio(scope(session, all_owners), recording_id)
        if row is None:
            raise HTTPException(404)
        return Response(row["audio"], media_type=row["media_type"], headers=PRIVATE)

    @router.post("/recordings/{recording_id}/retry")
    def retry(request: Request, recording_id: str, csrf: Annotated[str, Form()], all_owners: bool = False,
              back: Annotated[str, Form(max_length=2048)] = BASE):
        session = check_form(request, csrf)
        try:
            result = store.request_retry(scope(session, all_owners), recording_id, actor=session.owner)
        except RetryUnavailable:
            raise HTTPException(409, detail="这条录音当前无法重试。") from None
        if result is None:
            raise HTTPException(404)
        return redirect(recording_url(back, recording_id, all_owners, retried=True))

    @router.get("/recordings/{recording_id}/remove")
    def remove_page(request: Request, recording_id: str, all_owners: bool = False,
                    back: Annotated[str, Query(max_length=2048)] = BASE):
        session = session_for(request)
        if session is None:
            return redirect(login_url(request))
        item = store.get(scope(session, all_owners), recording_id)
        if item is None:
            raise HTTPException(404)
        if not item["can_remove"]:
            raise HTTPException(409, detail="这条话单当前无法移除。")
        return protected_page("remove", session, item=item, all_owners=all_owners, back=safe_destination(back),
                              detail_url=recording_url(back, recording_id, all_owners))

    @router.post("/recordings/{recording_id}/{operation}")
    def change_removal(request: Request, recording_id: str, operation: str,
                       csrf: Annotated[str, Form()], confirm: Annotated[str, Form()], all_owners: bool = False,
                       back: Annotated[str, Form(max_length=2048)] = BASE):
        session = check_form(request, csrf)
        if operation not in {"remove", "restore"} or confirm != "yes":
            raise HTTPException(422)
        try:
            result = store.set_removed(scope(session, all_owners), recording_id,
                                       actor=session.owner, removed=operation == "remove")
        except RemovalUnavailable:
            raise HTTPException(409, detail="这条话单当前无法移除或恢复。") from None
        if result is None:
            raise HTTPException(404)
        return redirect(recording_url(back, recording_id, all_owners))

    @router.get("/operations")
    def operations(request: Request, related_id: str | None = Query(None, max_length=128),
                   component: str | None = Query(None, max_length=128), before: int | None = Query(None, ge=1)):
        session = session_for(request)
        if session is None:
            return redirect(login_url(request))
        if not is_admin(session):
            raise HTTPException(403)
        if runtime_log is None:
            raise HTTPException(404)
        result = runtime_log.list(related_id=related_id or None, component=component or None, before=before)
        next_url = None
        if result["next_cursor"]:
            query = {"before": result["next_cursor"], "related_id": related_id or "", "component": component or ""}
            next_url = BASE + "/operations?" + urlencode(query)
        return protected_page("operations", session, result=result, next_url=next_url, related_id=related_id,
                              component=component, components=COMPONENTS)

    if workbench_transcription is not None:
        router.include_router(create_transcription_pages(
            store, workbench_transcription, protected_page=protected_page, login_url=login_url,
            session_for=session_for, check_form=check_form, redirect=redirect,
            private_headers=PRIVATE, max_bytes=max_bytes,
        ))
    return router
