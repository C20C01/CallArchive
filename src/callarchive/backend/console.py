import hmac
import re
import secrets
import shutil
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import quote, urlencode, urlsplit

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from pydantic import BaseModel, Field
from jinja2 import Environment, PackageLoader, select_autoescape

from .sessions import CONSOLE_SESSION, Session, SessionStore
from .store import RemovalUnavailable
from .workflow import JOBS


LOGIN = "/login"
PRELOGIN = "callarchive_console_signin"
ARRIVAL = "callarchive_console_arrival"
TITLES = {
    "/": "录音转写与归档", "/calls/pending": "待归档话单", "/calls/recent": "近期归档",
    "/transcriptions": "录音转写", "/admin/task-logs": "运行日志",
}
PRIVATE = {"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "X-Content-Type-Options": "nosniff"}
TEMPLATES = Environment(loader=PackageLoader("callarchive.backend"), autoescape=select_autoescape(("html",)))
CONSOLE_CSS = (Path(__file__).parent / "static" / "console.css").read_text(encoding="utf-8")
ROLES = {"admin": "管理员", "ops": "运营", "external": "外部"}


def page(template, *, status_code=200, **context):
    nonce = secrets.token_urlsafe(24)
    html = TEMPLATES.get_template(template + ".html").render(nonce=nonce, console_css=CONSOLE_CSS, **context)
    policy = (f"default-src 'none'; style-src 'nonce-{nonce}'; script-src 'nonce-{nonce}'; "
              "connect-src 'self'; media-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
    return HTMLResponse(html, status_code=status_code, headers={**PRIVATE, "Content-Security-Policy": policy})


def safe_destination(value):
    if any(ord(char) < 32 or char == chr(92) for char in value):
        return "/"
    try:
        target = urlsplit(value)
    except ValueError:
        return "/"
    if target.scheme or target.netloc or target.path not in TITLES or value.startswith("//"):
        return "/"
    return target.path + ("?" + target.query if target.query else "")


def request_destination(request):
    return request.url.path + ("?" + request.url.query if request.url.query else "")


def login_redirect(request):
    return RedirectResponse(LOGIN + "?" + urlencode({"destination": request_destination(request)}),
                            status_code=303, headers=PRIVATE)


def signed_in_redirect(destination, token, *, lifetime, secure_cookies):
    response = RedirectResponse(destination, status_code=303, headers=PRIVATE)
    response.set_cookie(ARRIVAL, urlsplit(destination).path, path="/", httponly=True, secure=secure_cookies,
                        samesite="strict", max_age=30)
    response.set_cookie(CONSOLE_SESSION, token, path="/", httponly=True, secure=secure_cookies,
                        samesite="strict", max_age=lifetime)
    return response


def render_arrival(request, render, *args, secure_cookies, **context):
    enter_from_login = request.cookies.get(ARRIVAL) == request.url.path
    response = render(*args, enter_from_login=enter_from_login, **context)
    if enter_from_login:
        response.delete_cookie(ARRIVAL, path="/", httponly=True, secure=secure_cookies, samesite="strict")
    return response


def role_for(session, admins):
    return session.role or ("admin" if session.owner in admins else "external")


class RemoveCalls(BaseModel):
    talk_ids: list[str] = Field(min_length=1, max_length=30)


def attachment_response(request, content, media_type, filename, *, inline):
    headers = {**PRIVATE, "Accept-Ranges": "bytes",
               "Content-Disposition": ("inline" if inline else "attachment") + "; filename*=UTF-8''" + quote(filename)}
    size = len(content)
    match = re.fullmatch(r"bytes=(\d{0,18})-(\d{0,18})", request.headers.get("range", ""))
    if match and any(match.groups()) and not request.headers.get("if-range"):
        first, last = match.groups()
        start = int(first) if first else max(size - int(last), 0)
        end = min(size - 1, int(last)) if first and last else size - 1
        if start > end:
            return Response(status_code=416, headers={**headers, "Content-Range": f"bytes */{size}"})
        return Response(content[start:end + 1], status_code=206, media_type=media_type,
                        headers={**headers, "Content-Range": f"bytes {start}-{end}/{size}"})
    return Response(content, media_type=media_type, headers=headers)


def create_console(store, credentials, admins, *, password_accounts, secure_cookies,
                   has_logs, transcription, followup_names=None, attachments=None, resume_call=None):
    router = APIRouter(include_in_schema=False)
    from .console_calls import ConsoleCalls
    from .recording_names import timestamp
    from .console_recent import RecentArchives

    sessions = SessionStore(store, credentials, password_accounts=password_accounts)
    calls = ConsoleCalls(store, followup_names)
    recent = RecentArchives(store)

    def session_for(request):
        token = request.cookies.get(CONSOLE_SESSION)
        return sessions.get(token) if token else None

    def require_ops(request: Request):
        session = session_for(request)
        if session is None:
            raise HTTPException(401, "登录已过期，请刷新页面。")
        if role_for(session, admins) not in {"admin", "ops"}:
            raise HTTPException(403, "当前账号没有访问权限。")
        return session

    def authorized_page(request, template, **context):
        session = session_for(request)
        if session is None:
            return login_redirect(request)
        if role_for(session, admins) not in {"admin", "ops"}:
            return page("console_denied", status_code=403)
        return render_arrival(request, page, template, secure_cookies=secure_cookies,
                              csrf=session.csrf, **context)

    def csrf_check(request, session):
        if not session.accepts_csrf(request.headers.get("X-CallArchive-CSRF", "")):
            raise HTTPException(403, "操作凭据已过期，请刷新页面。")

    def signin(destination="/", *, error=None, status_code=200):
        csrf = secrets.token_urlsafe(32)
        destination = safe_destination(destination)
        response = page("console_login", title=TITLES[urlsplit(destination).path], action=LOGIN,
                        csrf=csrf, destination=destination, error=error, status_code=status_code)
        response.set_cookie(PRELOGIN, csrf, path=LOGIN, httponly=True, secure=secure_cookies,
                            samesite="strict", max_age=600)
        return response

    @router.get(LOGIN)
    def login_page(destination: str = "/"):
        return signin(destination)

    @router.post(LOGIN)
    def login(request: Request, credential: Annotated[str, Form(max_length=1024)],
              csrf: Annotated[str, Form(max_length=128)], destination: Annotated[str, Form(max_length=2048)] = "/"):
        expected = request.cookies.get(PRELOGIN, "")
        if not expected or not hmac.compare_digest(expected.encode(), csrf.encode()):
            return signin(destination, error="登录请求已过期，请重试。", status_code=403)
        result = sessions.login(credential)
        if result is None:
            return signin(destination, error="访问凭据无效。", status_code=401)
        token, _ = result
        response = signed_in_redirect(safe_destination(destination), token,
                                      lifetime=sessions.lifetime, secure_cookies=secure_cookies)
        response.delete_cookie(PRELOGIN, path=LOGIN, httponly=True, secure=secure_cookies, samesite="strict")
        return response

    def overview_data():
        with closing(store.connect()) as db:
            poll = db.execute("SELECT max(ends) FROM outbound_windows WHERE done=1").fetchone()[0]
            archived = db.execute("SELECT max(occurred_at) FROM recording_events WHERE status='archived'").fetchone()[0]
        try:
            archive_disk = Path(attachments) if attachments else store.database.parent
            while not archive_disk.exists() and archive_disk != archive_disk.parent:
                archive_disk = archive_disk.parent
            usage = shutil.disk_usage(archive_disk)
        except OSError:
            usage = None
        counts = calls.counts()

        def display(value):
            return (timestamp(value) or "—")[:19].replace("T", " ")

        return {"poll": display(poll), "poll_note": "已查至此时间" if poll else "",
                "archive": display(archived), "free": f"{usage.free / 1024**3:.1f} GiB" if usage else "—",
                "capacity": f"共 {usage.total / 1024**3:.1f} GiB" if usage else "",
                "recording": f"{counts.get('waiting_recording', 0)} 通",
                "applicant": f"{counts.get('waiting_applicant', 0)} 通",
                "attention": f"{counts.get('attention', 0) + counts.get('recording_invalid', 0)} 通"}

    async def transcription_health():
        if transcription is not None:
            try:
                response = await transcription.get(JOBS, timeout=2)
                if response.is_success:
                    queue = response.json().get("queue")
                    if queue is not None:
                        return ("在线", "online") if queue["online"] else ("未收到心跳", "unknown")
            except (httpx.RequestError, ValueError, KeyError):
                pass
        return "—", "unknown"

    @router.get("/")
    async def home(request: Request):
        session = await run_in_threadpool(session_for, request)
        if session is None:
            return signin()
        role = role_for(session, admins)
        links = []
        if role == "admin" and has_logs:
            links.append(("/admin/task-logs", "运行日志", "查看关键运行记录，快速定位异常。"))
        if role in {"admin", "ops"}:
            links.extend([
                ("/calls/pending", "待归档话单", "查看未归档话单和处理进度。"),
                ("/calls/recent", "近期归档", "查看最近 24 小时归档的录音和转写文本。"),
            ])
        if transcription is not None:
            links.append(("/transcriptions", "录音转写", "上传录音、查看进度和下载转写文本。"))
        overview = None
        if role in {"admin", "ops"}:
            overview = await run_in_threadpool(overview_data)
            overview["worker"], overview["worker_state"] = await transcription_health()
        return render_arrival(request, page, "console_home", secure_cookies=secure_cookies,
                              role_label=ROLES[role], links=links, overview=overview)

    @router.get("/calls", include_in_schema=False)
    def old_calls_link(request: Request):
        location = "/calls/pending"
        if request.url.query:
            location += "?" + request.url.query
        return RedirectResponse(location, status_code=303, headers=PRIVATE)

    @router.get("/calls/pending")
    def pending_page(request: Request, status: str = ""):
        from .console_calls import PENDING_LABELS

        return authorized_page(request, "console_pending", initial_status=status if status in PENDING_LABELS else "")

    @router.get("/api/admin/calls")
    def pending_list(session: Annotated[Session, Depends(require_ops)],
                     mobile: Annotated[str | None, Query(pattern=r"^\+?[0-9]{1,20}$")] = None,
                     day: Annotated[date | None, Query(alias="date")] = None,
                     owner: Annotated[str | None, Query(max_length=100)] = None,
                     salesperson: Annotated[str | None, Query(max_length=100)] = None,
                     status: Annotated[str | None, Query(max_length=40)] = None,
                     offset: Annotated[int, Query(ge=0, le=2**63-1)] = 0,
                     limit: Annotated[int, Query(ge=1, le=30)] = 30):
        return calls.pending(day=day.isoformat() if day else None, mobile=mobile, owner=owner,
                             salesperson=salesperson, status=status, offset=offset, limit=limit)

    @router.get("/api/admin/calls/{call_key}")
    def pending_detail(call_key: str, session: Annotated[Session, Depends(require_ops)]):
        detail = calls.pending_detail(call_key)
        if detail is None:
            raise HTTPException(404, "待归档话单不存在。")
        return detail

    @router.get("/api/admin/calls/{call_key}/recording")
    def pending_audio(call_key: str, file_id: str, request: Request,
                      session: Annotated[Session, Depends(require_ops)], inline: bool = False):
        detail = calls.pending_detail(call_key)
        segment = next((item for item in detail["recordings"]
                        if item["file_id"] == file_id and item.get("download_name")), None) if detail else None
        audio = store.get_audio(None, file_id) if segment else None
        if audio is None or not audio["audio"]:
            raise HTTPException(404, "录音不可用。")
        return attachment_response(request, audio["audio"], audio["media_type"], segment["download_name"],
                                   inline=inline)

    @router.post("/api/calls/{call_key}/retry")
    def pending_retry(call_key: str, request: Request, background: BackgroundTasks,
                      session: Annotated[Session, Depends(require_ops)]):
        csrf_check(request, session)
        status = calls.retry(call_key, session.owner)
        if status == "processing":
            raise HTTPException(409, "话单正在处理，请稍后重试。")
        if status is None:
            raise HTTPException(409, "这通话单当前无法重试。")
        if status == "scheduled" and resume_call is not None:
            background.add_task(resume_call, call_key)
        message = "已在队列。" if status == "queued" else "已安排重试。"
        return {"status": status, "message": message}

    @router.post("/api/calls/remove")
    def pending_remove(body: RemoveCalls, request: Request, session: Annotated[Session, Depends(require_ops)]):
        csrf_check(request, session)
        removed, failed = [], []
        for call_key in dict.fromkeys(body.talk_ids):
            try:
                changed = calls.remove(call_key, session.owner)
            except RemovalUnavailable:
                failed.append({"talk_id": call_key, "reason": "话单正在处理，请稍后重试。"})
            else:
                if changed:
                    removed.append(call_key)
                else:
                    failed.append({"talk_id": call_key, "reason": "话单已不在待归档列表。"})
        return {"removed": removed, "failed": failed}

    @router.get("/calls/recent")
    def recent_page(request: Request):
        return authorized_page(request, "console_recent")

    @router.get("/api/archives/recent")
    def recent_list(session: Annotated[Session, Depends(require_ops)],
                    mobile: Annotated[str | None, Query(pattern=r"^\+?[0-9]{1,20}$")] = None,
                    day: Annotated[date | None, Query(alias="date")] = None,
                    offset: Annotated[int, Query(ge=0, le=2**63-1)] = 0):
        return recent.list(day=day.isoformat() if day else None, mobile=mobile, offset=offset)

    @router.get("/api/archives/recent/{call_key}")
    def recent_detail(call_key: str, session: Annotated[Session, Depends(require_ops)]):
        files = recent.files(call_key)
        if not files:
            raise HTTPException(404, "归档话单不存在或附件已到期。")
        return {"files": files}

    @router.get("/api/archives/recent/{call_key}/attachment")
    def recent_attachment(call_key: str, file_id: str, kind: Literal["audio", "text"], request: Request,
                          session: Annotated[Session, Depends(require_ops)], inline: bool = False):
        attachment = recent.attachment(call_key, file_id, kind)
        if attachment is None:
            raise HTTPException(404, "归档附件不存在或已到期。")
        return attachment_response(request, *attachment, inline=inline)

    return router
