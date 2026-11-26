import base64
import hmac
import json
import re
import time
from datetime import datetime
from urllib.parse import quote, urlsplit

import httpx

from . import Delivery, Message, Receipt

KNOWN_REJECTION = {9499, 11232, 19021, 19022, 19024}
SHORT_DURATION_COLOR = "rgba(204,32,1,1.000000)"  # Frontend #cc2001.
SHORT_DURATION_COLOR_KEY = "cus-0"
REASON_LABELS = {
    "attachment_write_failed": "附件写入失败",
    "archive_target_missing": "未找到应聘档案",
    "archive_target_conflict": "应聘档案待核对",
    "archive_routing_changed": "话单信息已更新，归档目标待核对",
    "archive_attachment_unknown": "附件待核对",
    "archive_attachment_rejected": "归档失败",
    "attachment_conflict": "已有附件与本次内容不一致",
    "transcription_failed": "转写失败",
    "transcription_unavailable": "转写服务异常",
    "transcription_needs_review": "转写结果待核对",
    "source_recording_invalid": "录音异常",
    "source_recording_unavailable": "录音获取失败",
    "invalid_workflow_response": "处理结果异常",
    "needs_input": "档案待补录",
    "needs_review": "处理结果待核对",
}


def plain(content):
    return {"tag": "plain_text", "content": content}


def line(content, *, size="normal_v2"):
    return {"tag": "div", "text": {**plain(content), "text_size": size}}


def escape_markdown(text):
    return re.sub(r"([\\`*_{}\[\]()<>#!|~])", r"\\\1", " ".join(text.splitlines()))


def report_note(text):
    return {"tag": "markdown", "text_size": "notation",
            "content": f"<font color='grey'>{escape_markdown(text)}</font>"}


def report_totals(followup, admin, archived):
    columns = []
    for title, count in (("待跟进", followup), ("管理员事项", admin), ("已归档", archived)):
        columns.append({"tag": "column", "width": "weighted", "weight": 1, "elements": [
            {"tag": "markdown", "content": f"**{count}**", "text_align": "center", "text_size": "heading-2"},
            {"tag": "markdown", "content": title, "text_align": "center", "text_size": "notation"},
        ]})
    return {"tag": "column_set", "flex_mode": "none", "columns": columns}


def issue_reasons(issue):
    return tuple(dict.fromkeys(REASON_LABELS.get(reason, "请查看详情")
                              for reason in issue.get("reasons") or [issue["reason"]]))


def followup_action(reasons):
    actions = []
    if "应聘档案待核对" in reasons:
        actions.append("请核对招聘系统里的应聘档案。")
    if any(reason != "应聘档案待核对" for reason in reasons):
        actions.append("请补录应聘档案，或在后台移除无需归档的话单。")
    return " ".join(actions)


def report_panel(title, items, link, *, admin=False):
    groups = {}
    for issue in items:
        reasons = issue_reasons(issue)
        groups.setdefault(reasons, []).append(issue)
    elements = []
    for reasons, rows in sorted(groups.items()):
        label = escape_markdown("、".join(reasons))
        heading = {"tag": "markdown", "content": f"**{label} · {len(rows)} 通**"}
        if elements:
            heading["margin"] = "8px 0 0 0"
        elements.extend((heading, {"tag": "markdown", "content": "<br>".join(
            summary_line(issue) for issue in rows
        )}))
        if not admin:
            elements.append(report_note(followup_action(reasons)))
    if admin:
        elements.append(report_note("请联系管理员处理。"))
    if link:
        button = {**details_button(link), "type": "primary", "size": "small"}
        elements.append({"tag": "column_set", "margin": "8px 0 0 0", "columns": [
            {"tag": "column", "width": "weighted", "weight": 1, "elements": [button]},
        ]})
    return {
        "tag": "collapsible_panel", "expanded": False,
        "padding": "4px 0 4px 6px", "vertical_spacing": "small",
        "header": {
            "title": {"tag": "markdown", "content": f"**{escape_markdown(title)}**"},
            "icon": {"tag": "standard_icon", "token": "down-small-ccm_outlined", "size": "16px 16px"},
            "icon_position": "follow_text",
        },
        "elements": elements,
    }


def summary_line(issue, *, link=None):
    number = escape_markdown(issue.get("callee") or "号码缺失")
    if link:
        target = quote(link, safe=":/?&=%+;,@-._~#")
        number = f"[{number}]({target})"
    fields = [number]
    if issue.get("started_at"):
        try:
            call_time = datetime.fromisoformat(issue["started_at"].replace("Z", "+00:00"))
            fields.append(call_time.strftime("%m/%d %H:%M"))
        except ValueError:
            pass
    duration_ms = issue.get("recording_duration_ms")
    if duration_ms is not None:
        minutes, seconds = divmod(duration_ms // 1000, 60)
        duration = f"{minutes:02d}:{seconds:02d}"
        if duration_ms < 120000:
            duration = f"<font color='{SHORT_DURATION_COLOR_KEY}'>{duration}</font>"
        fields.append(f"⏱\ufe0e {duration}")
    return " · ".join(fields)


def details_button(url):
    return {"tag": "button", "type": "default", "width": "fill",
            "text": plain("详情"), "behaviors": [{"type": "open_url", "default_url": url}]}


def notification_card(title, elements, *, template, spacing):
    color = {"light_mode": SHORT_DURATION_COLOR, "dark_mode": SHORT_DURATION_COLOR}
    return {
        "schema": "2.0",
        "config": {"style": {"color": {SHORT_DURATION_COLOR_KEY: color}}},
        "header": {"template": template, "title": plain(title)},
        "body": {"vertical_spacing": spacing, "elements": elements},
    }


def alert_card(message):
    issue = message.content["issue"]
    review = issue["category"] == "admin"
    reasons = issue_reasons(issue)
    action = "请联系管理员处理。" if review else followup_action(reasons)
    link = message.content.get("details_url")
    target = urlsplit(link) if link else None
    if not (target and target.scheme in {"https", "http"} and target.hostname):
        link = None
    summary = {"tag": "markdown", "content": summary_line(issue, link=link)}
    elements = [summary, line("、".join(reasons) + " · " + action, size="notation")]
    return notification_card(message.title, elements, template="orange" if review else "blue", spacing="8px")


def build_card(message: Message):
    content = message.content
    if content.get("kind") == "alert":
        return alert_card(message)
    elements = []
    page = content.get("page", 1)
    total_pages = content.get("total_pages", 1)
    title = message.title if total_pages == 1 else f"{message.title}（{page}/{total_pages}）"
    archived = content.get("archived_calls")
    waiting = content.get("waiting_recordings")
    followups = {}
    admins = []
    for issue in content.get("issues", []):
        if issue["category"] == "follow_up":
            followups.setdefault(issue.get("assignee"), []).append(issue)
        elif issue["category"] == "admin":
            admins.append(issue)
    if page == 1 and archived is not None:
        elements.append(report_totals(
            content.get("followup_count", sum(map(len, followups.values()))),
            content.get("admin_count", len(admins)),
            archived,
        ))
        if followups or admins:
            elements.append({"tag": "hr"})
    workbench = content.get("workbench_url")
    continued_followups = content.get("continued_followups", ())
    for assignee, items in sorted(followups.items(), key=lambda entry: entry[0] or ""):
        heading = f"{assignee} 待跟进" if assignee else "待跟进"
        continued = assignee in continued_followups
        suffix = "（续）" if continued else ""
        has_more = assignee in content.get("followups_with_more", continued_followups)
        link = None if has_more else (items[0].get("owner_url") or workbench)
        elements.append(report_panel(f"{heading} {len(items)} 通{suffix}", items, link))
    if admins:
        continued = content.get("admin_continued")
        suffix = "（续）" if continued else ""
        has_more = content.get("admin_has_more", continued)
        link = None if has_more else (content.get("admin_url") or workbench)
        elements.append(report_panel(f"需管理员处理 {len(admins)} 通{suffix}", admins, link, admin=True))
    if waiting:
        elements.append(report_note(f"等待录音上传 {waiting} 通，系统会自动补查，请耐心等待。"))
    if not waiting and not followups and not admins:
        elements.append(report_note("暂无待跟进或需要管理员介入的事项。"))
    return notification_card(title, elements, template="turquoise", spacing="medium")


class FeishuChannel:
    supports_idempotency = False
    retryable_rejections = frozenset({"http_429", "11232"})

    def __init__(self, client: httpx.Client, webhook: str, *, secret=None, clock=time.time):
        url = urlsplit(webhook)
        if (url.scheme != "https" or url.netloc not in {"open.feishu.cn", "open.larksuite.com"}
                or not re.fullmatch(r"/open-apis/bot/v2/hook/[A-Za-z0-9-]+", url.path)
                or url.query or url.fragment):
            raise ValueError("Invalid Feishu robot webhook URL")
        self.client = client
        self.webhook = webhook
        self.secret = secret
        self.clock = clock

    @staticmethod
    def fits(message: Message) -> bool:
        card = build_card(message)

        def components(elements):
            return sum(1 + components(element.get("elements", [])) + components(element.get("columns", []))
                       for element in elements)

        payload = json.dumps({"msg_type": "interactive", "card": card}, ensure_ascii=False, separators=(",", ":"))
        return len(payload.encode("utf-8")) <= 19_800 and components(card["body"]["elements"]) <= 200

    def send(self, message: Message) -> Receipt:
        payload = {"msg_type": "interactive", "card": build_card(message)}
        if self.secret:
            stamp = str(int(self.clock()))
            key = f"{stamp}\n{self.secret}".encode("utf-8")
            payload.update(timestamp=stamp, sign=base64.b64encode(hmac.digest(key, b"", "sha256")).decode("ascii"))
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(body) > 20000:
            return Receipt(Delivery.REJECTED, "payload_too_large", "feishu_payload_too_large")
        try:
            response = self.client.post(self.webhook, content=body, follow_redirects=False,
                                        headers={"Content-Type": "application/json; charset=utf-8"})
        except httpx.TimeoutException:
            return Receipt(Delivery.UNKNOWN, reason="feishu_timeout")
        except httpx.ConnectError:
            return Receipt(Delivery.UNKNOWN, reason="feishu_connect_error")
        except httpx.RequestError:
            return Receipt(Delivery.UNKNOWN, reason="feishu_request_error")
        if response.status_code in {400, 401, 403, 404, 413, 429}:
            return Receipt(Delivery.REJECTED, f"http_{response.status_code}", f"feishu_http_{response.status_code}")
        if not response.is_success:
            return Receipt(Delivery.UNKNOWN, reason="feishu_http_error")
        try:
            code = response.json()["code"]
        except (ValueError, KeyError, TypeError):
            return Receipt(Delivery.UNKNOWN, reason="feishu_invalid_response")
        if type(code) is int and code == 0:
            return Receipt(Delivery.CONFIRMED)
        if type(code) is int and code in KNOWN_REJECTION:
            return Receipt(Delivery.REJECTED, str(code), f"feishu_{code}")
        return Receipt(Delivery.UNKNOWN, reason="feishu_unrecognized_receipt")
