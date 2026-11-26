import hmac
import secrets
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal
from urllib.parse import urlencode, urlsplit

from fastapi import APIRouter, Form, Query, Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment, PackageLoader, select_autoescape

from .console import page, render_arrival, request_destination, safe_destination, signed_in_redirect

from .sessions import CONSOLE_SESSION, SessionStore


PATH = "/admin/task-logs"
SESSION = "callarchive_log_session"
PRELOGIN = "callarchive_log_signin"
PRIVATE = {"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "X-Content-Type-Options": "nosniff"}
TEMPLATES = Environment(loader=PackageLoader("callarchive.backend"), autoescape=select_autoescape(("html",)))
COMPONENTS = {
    "archive-api": "归档后台", "archive-worker": "归档进程", "outbound-import": "外呼导入",
    "transcription-api": "转写服务", "transcription-worker": "转写进程", "notification": "通知",
}
EVENTS = {
    "archive_stage": "归档进度", "transcription_accepted": "已受理转写", "transcription_started": "开始转写",
    "transcription_finished": "转写结果", "outbound_import_finished": "外呼导入结果",
    "outbound_import_stopped": "外呼导入中断", "archive_provider_not_configured": "档案接入不可用",
    "notification_delivery": "通知投递结果", "notification_exception": "通知投递异常",
    "report_delivery": "日报投递", "report_retry_delivery": "日报补发", "alert_delivery": "单条提醒投递",
    "process_started": "进程启动", "runtime_log_unavailable": "无法写入运行日志库",
    "scheduled_task_failed": "定时任务未完成", "scheduled_daily_missed": "日报错过发送时段", "outbound_scan_deferred": "外呼待续扫",
    "outbound_poll_stalled": "外呼扫描进度停滞", "outbound_poll_recovered": "外呼扫描已恢复",
    "outbound_disk_reserve": "存储空间不足，暂停下载录音",
}
STATUSES = {
    "queued": "待转写", "transcribing": "转写中", "acknowledging": "保存转写结果", "delivering": "归档中",
    "archived": "归档成功", "needs_review": "需管理员核对", "needs_input": "待跟进",
    "failed": "失败", "succeeded": "成功", "confirmed": "已确认", "unknown": "结果待核对", "rejected": "未接受",
    "removed": "已移除", "scan_finished": "扫描完成", "scan_deferred": "待续扫", "scan_current": "无新增扫描窗口",
    "scan_empty": "扫描完成 · 本轮无话单",
}
REASONS = {
    "outbound_schedule_arguments": "外呼启动参数缺失或冲突，请检查计划任务命令。",
    "outbound_source_configuration": "外呼来源或归属账号未配置。",
    "outbound_page_limit": "每轮最大页数必须大于零。",
    "feishu_9499": "飞书拒绝消息格式，请检查卡片内容。",
    "feishu_http_400": "飞书拒绝请求格式。", "feishu_http_401": "飞书拒绝认证。",
    "feishu_http_403": "飞书拒绝访问。", "feishu_http_404": "飞书机器人地址不存在。",
    "feishu_http_413": "飞书消息超过大小限制。",
    "feishu_19021": "飞书签名校验失败，请核对机器人签名配置。",
    "feishu_19022": "飞书拒绝请求，请核对机器人的 IP 白名单。",
    "feishu_19024": "消息未通过飞书机器人的关键词校验。",
    "feishu_11232": "飞书投递限流。", "feishu_http_429": "飞书投递限流。",
    "feishu_timeout": "等待飞书响应超时，投递结果待核对。",
    "feishu_payload_too_large": "飞书消息超过大小限制。",
    "feishu_connect_error": "无法连接飞书。", "feishu_request_error": "飞书请求异常。",
    "feishu_http_error": "飞书服务返回错误。", "feishu_invalid_response": "飞书返回无法解析。",
    "feishu_unrecognized_receipt": "无法识别飞书回执。",
    "archive_target_missing": "未找到应聘档案。补录后系统会自动复查。",
    "archive_target_conflict": "匹配到多个档案，请核对归档目标。",
    "archive_routing_changed": "话单信息已更新，请核对归档目标和已提交的附件。",
    "archive_attachment_unknown": "附件写入结果不明，请先核对接收情况。",
    "attachment_write_failed": "档案附件写入失败。",
    "archive_attachment_rejected": "档案系统拒绝接收附件。",
    "attachment_conflict": "已有附件与当前内容不一致。",
    "archive_lookup_unavailable": "档案查询暂不可用。",
    "archive_instance_unavailable": "档案接入实例不可用。",
    "missing_provider_credential": "缺少档案接入凭据。",
    "transcription_failed": "转写失败。", "transcription_unavailable": "转写服务暂不可用。",
    "transcription_needs_review": "转写结果需核对。", "TimeoutError": "等待响应超时。", "OSError": "文件或网络操作失败。",
}
LEVELS = {"DEBUG": "调试", "INFO": "信息", "WARNING": "警告", "ERROR": "错误", "CRITICAL": "严重"}
COUNTS = {
    "imported": ("已导入", "success"), "pending": ("下载失败次数", "warning"),
    "confirmed": ("已确认", "success"), "rejected": ("未接受", "error"), "unknown": ("待核对", "warning"),
}
REFERENCES = {"recording_id": "录音 ID", "job_id": "转写任务 ID", "message_id": "通知 ID"}
LOCAL_TIME = timezone(timedelta(hours=8))
SCHEDULE_LABELS = {"outbound": "外呼拉取", "daily": "日报", "daily_retry": "日报补发", "alerts": "单条提醒"}
SCHEDULE_STATES = {"not_run": "尚未运行", "running": "运行中", "succeeded": "成功", "failed": "未完成",
                   "recovery": "等待接管", "missed": "错过时段"}


def display_time(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(LOCAL_TIME).strftime("%m-%d %H:%M:%S")
    except (AttributeError, ValueError):
        return value or "—"


def describe(row):
    status = row.get("status")
    reason = row.get("reason")
    level = row.get("level", "")
    counts = [(label, row[key], tone) for key, (label, tone) in COUNTS.items() if row.get(key)]
    context = []
    if row.get("calls_returned"):
        context.append(("上游返回", f'{row["calls_returned"]} 条话单'))
    if row.get("scan_through"):
        context.append(("话单已查至", display_time(row["scan_through"])))
    if row.get("report_day"):
        day = row["report_day"]
        if row.get("page") and row.get("total_pages"):
            day = f'{day} · 第 {row["page"]}/{row["total_pages"]} 页'
        context.append(("日报日期", day))
    if row.get("task") in SCHEDULE_LABELS:
        context.append(("定时任务", SCHEDULE_LABELS[row["task"]]))
    identifiers = [(label, row[key]) for key, label in REFERENCES.items() if row.get(key)]
    if level in {"ERROR", "CRITICAL"} or status in {"failed", "rejected"}:
        tone = "error"
    elif level == "WARNING" or status in {"needs_input", "needs_review", "unknown"} or reason or (
            row["event"] == "outbound_import_finished" and row.get("pending")):
        tone = "warning"
    elif status in {"archived", "succeeded", "confirmed"} or (
            row["event"] == "outbound_import_finished" and row.get("imported")):
        tone = "success"
    else:
        tone = "neutral"
    result = "成功" if status in {"archived", "succeeded", "confirmed"} else STATUSES.get(status, status)
    return {
        "raw": row, "title": EVENTS.get(row["event"], row["event"]), "result": result or LEVELS.get(level, level),
        "component": COMPONENTS.get(row["component"], row["component"]), "time": display_time(row.get("time")),
        "reason": REASONS.get(reason, reason), "counts": counts, "context": context,
        "identifiers": identifiers, "tone": tone,
    }


def render(*, status_code=200, **context):
    nonce = secrets.token_urlsafe(24)
    headers = {
        **PRIVATE,
        "Content-Security-Policy": f"default-src 'none'; style-src 'nonce-{nonce}'; script-src 'nonce-{nonce}'; connect-src 'self'; form-action 'self'; "
                                   "base-uri 'none'; frame-ancestors 'none'",
    }
    html = TEMPLATES.get_template("runtime_logs.html").render(nonce=nonce, path=PATH, **context)
    return HTMLResponse(html, status_code=status_code, headers=headers)





def create_runtime_log_view(store, runtime_log, credentials, admins, *, password_accounts=None,
                            secure_cookies=False):
    router = APIRouter(include_in_schema=False)
    sessions = SessionStore(store, credentials, password_accounts=password_accounts)

    def is_admin(session):
        return session.role == "admin" if session.role is not None else session.owner in admins

    def sign_in(destination, *, error=None, status=200):
        csrf = secrets.token_urlsafe(32)
        response = page("console_login", title="运行日志", action=PATH, csrf=csrf, destination=destination,
                        error=error, status_code=status)
        response.set_cookie(PRELOGIN, csrf, httponly=True, secure=secure_cookies,
                            samesite="strict", path=PATH, max_age=600)
        return response

    @router.get(PATH)
    def view(request: Request, mode: Literal["important", "attention", "all"] = "important",
             component: Annotated[str, Query(max_length=128)] = "",
             related_id: Annotated[str, Query(max_length=128)] = "",
             before: Annotated[int | None, Query(ge=1)] = None):
        token = request.cookies.get(CONSOLE_SESSION) or request.cookies.get(SESSION)
        session = sessions.get(token) if token else None
        if session is None:
            return sign_in(request_destination(request))
        if not is_admin(session):
            return render(error="当前凭据没有查看日志的权限。", status_code=403)

        selected_id = related_id.strip()
        result = runtime_log.list(related_id=selected_id or None, component=component or None, before=before,
                                  limit=30, important_only=mode == "important", attention_only=mode == "attention")
        filters = {"related_id": selected_id, "component": component}

        def url(**params):
            query = urlencode({key: value for key, value in (filters | {"mode": mode} | params).items() if value})
            return PATH + ("?" + query if query else "")

        return render_arrival(request, render, secure_cookies=secure_cookies,
                              rows=[describe(row) for row in result["items"]], mode=mode, filters=filters,
                              components=COMPONENTS, url=url, has_previous=before is not None,
                              next_cursor=result["next_cursor"])

    @router.post(PATH)
    def login(request: Request, credential: Annotated[str, Form(max_length=1024)],
              csrf: Annotated[str, Form(max_length=128)], destination: Annotated[str, Form(max_length=2048)] = PATH):
        target = safe_destination(destination)
        if urlsplit(target).path != PATH:
            target = PATH
        expected = request.cookies.get(PRELOGIN, "")
        if not expected or not hmac.compare_digest(expected.encode(), csrf.encode()):
            return sign_in(target, error="登录请求已过期，请重试。", status=403)
        result = sessions.login(credential)
        if result is None:
            return sign_in(target, error="访问凭据无效。", status=401)
        token, session = result
        if not is_admin(session):
            sessions.logout(token)
            return sign_in(target, error="当前凭据没有查看日志的权限。", status=403)
        response = signed_in_redirect(target, token, lifetime=sessions.lifetime, secure_cookies=secure_cookies)
        response.delete_cookie(PRELOGIN, httponly=True, secure=secure_cookies, samesite="strict", path=PATH)
        return response

    return router
